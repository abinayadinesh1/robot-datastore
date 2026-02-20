use std::sync::Arc;
use std::time::Duration;

use frame_bucket_common::config::RecordingConfig;
use frame_bucket_common::frame::TimestampedFrame;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::db::SegmentDb;
use crate::filter::phash::{compute_ahash, hamming};
use crate::storage::RustfsStorage;

use super::encoder::{EncoderError, SegmentEncoder};
use super::keys::{active_segment_key, idle_jpeg_key};

enum RecordingState {
    /// The scene is static. We track the initial frame and the last timestamp
    /// at which the scene was still considered unchanged.
    Idle {
        initial_jpeg: Vec<u8>,
        initial_hash: Vec<bool>,
        idle_start_ms: i64,
        /// Last frame timestamp that was still similar to the initial frame.
        last_similar_ms: i64,
    },
    /// The scene is changing. We encode frames into 1-minute MP4 segments.
    Active {
        encoder: SegmentEncoder,
        /// Monotonic deadline for rolling the current segment.
        segment_deadline: Instant,
        segment_start_ms: i64,
        last_frame_hash: Vec<bool>,
        /// Consecutive frames that look similar (potential idle transition).
        consecutive_idle_count: u32,
    },
}

pub struct RecordingStateMachine {
    state: Option<RecordingState>, // Option so we can take() during transitions
    config: RecordingConfig,
    phash_threshold: u32,
    hash_size: u32,
    storage: Arc<RustfsStorage>,
    db: Option<Arc<SegmentDb>>,
    prefix: String,
    robot_id: String,
}

impl RecordingStateMachine {
    pub fn new(
        config: RecordingConfig,
        phash_threshold: u32,
        hash_size: u32,
        storage: Arc<RustfsStorage>,
        db: Option<Arc<SegmentDb>>,
        prefix: String,
        robot_id: String,
    ) -> Self {
        Self {
            state: None,
            config,
            phash_threshold,
            hash_size,
            storage,
            db,
            prefix,
            robot_id,
        }
    }

    /// Process one incoming frame from Kafka. This is the main entry point.
    pub async fn process_frame(&mut self, frame: &TimestampedFrame) {
        // Compute hash for this frame upfront (used in both modes).
        let hash = match compute_ahash(&frame.jpeg_data, self.hash_size) {
            Some(h) => h,
            None => {
                warn!(ts = frame.captured_at_ms, "failed to compute aHash for frame, skipping");
                return;
            }
        };

        // First frame ever: enter Idle.
        if self.state.is_none() {
            info!(ts = frame.captured_at_ms, "first frame — entering IDLE mode");
            self.state = Some(RecordingState::Idle {
                initial_jpeg: frame.jpeg_data.clone(),
                initial_hash: hash,
                idle_start_ms: frame.captured_at_ms,
                last_similar_ms: frame.captured_at_ms,
            });
            return;
        }

        // Route to the appropriate handler. We take() the state to allow ownership moves.
        match self.state.take().unwrap() {
            idle @ RecordingState::Idle { .. } => {
                self.state = Some(self.handle_idle(idle, frame, &hash).await);
            }
            active @ RecordingState::Active { .. } => {
                self.state = Some(self.handle_active(active, frame, &hash).await);
            }
        }
    }

    // -------------------------------------------------------------------------
    // IDLE mode handler
    // -------------------------------------------------------------------------

    async fn handle_idle(
        &mut self,
        state: RecordingState,
        frame: &TimestampedFrame,
        hash: &[bool],
    ) -> RecordingState {
        let RecordingState::Idle {
            initial_jpeg,
            initial_hash,
            idle_start_ms,
            last_similar_ms,
        } = state
        else {
            unreachable!()
        };

        let dist = hamming(&initial_hash, hash);

        if dist <= self.phash_threshold {
            // Still idle — just update the last-seen timestamp.
            debug!(
                dist,
                threshold = self.phash_threshold,
                ts = frame.captured_at_ms,
                "IDLE: frame similar to baseline"
            );
            return RecordingState::Idle {
                initial_jpeg,
                initial_hash,
                idle_start_ms,
                last_similar_ms: frame.captured_at_ms,
            };
        }

        // Scene changed — finalize the idle record and transition to Active.
        info!(
            dist,
            threshold = self.phash_threshold,
            idle_start_ms,
            idle_end_ms = last_similar_ms,
            "IDLE→ACTIVE: scene changed, finalizing idle record"
        );

        self.upload_idle_record(
            &initial_jpeg,
            idle_start_ms,
            last_similar_ms,
        )
        .await;

        // Transition to Active with the current (changed) frame.
        match self.start_active_segment(frame, hash.to_vec()).await {
            Some(active_state) => active_state,
            None => {
                // ffmpeg failed to start — fall back to Idle with this frame as new baseline.
                warn!("ffmpeg failed to start, staying in IDLE with new baseline");
                RecordingState::Idle {
                    initial_jpeg: frame.jpeg_data.clone(),
                    initial_hash: hash.to_vec(),
                    idle_start_ms: frame.captured_at_ms,
                    last_similar_ms: frame.captured_at_ms,
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // ACTIVE mode handler
    // -------------------------------------------------------------------------

    async fn handle_active(
        &mut self,
        state: RecordingState,
        frame: &TimestampedFrame,
        hash: &[bool],
    ) -> RecordingState {
        let RecordingState::Active {
            mut encoder,
            segment_deadline,
            segment_start_ms,
            last_frame_hash,
            mut consecutive_idle_count,
        } = state
        else {
            unreachable!()
        };

        // Check if the 1-minute segment timer has expired.
        if Instant::now() >= segment_deadline {
            info!(
                segment_start_ms,
                end_ms = frame.captured_at_ms,
                frames = encoder.frame_count(),
                "ACTIVE: rolling segment (timer expired)"
            );
            self.finish_and_upload_segment(encoder, frame.captured_at_ms).await;

            // Start a new segment with the current frame.
            return match self.start_active_segment(frame, hash.to_vec()).await {
                Some(s) => s,
                None => {
                    warn!("ffmpeg failed to start new segment, falling back to IDLE");
                    RecordingState::Idle {
                        initial_jpeg: frame.jpeg_data.clone(),
                        initial_hash: hash.to_vec(),
                        idle_start_ms: frame.captured_at_ms,
                        last_similar_ms: frame.captured_at_ms,
                    }
                }
            };
        }

        // Push the current frame into the encoder.
        if let Err(e) = encoder.push_frame(&frame.jpeg_data).await {
            error!(error = %e, "ACTIVE: failed to push frame to encoder, finalizing broken segment");
            // Encoder is broken — finalize what we have and go Idle.
            self.finish_and_upload_segment(encoder, frame.captured_at_ms).await;
            return RecordingState::Idle {
                initial_jpeg: frame.jpeg_data.clone(),
                initial_hash: hash.to_vec(),
                idle_start_ms: frame.captured_at_ms,
                last_similar_ms: frame.captured_at_ms,
            };
        }

        // Check for Active→Idle transition (consecutive similar frames).
        let dist = hamming(&last_frame_hash, hash);
        if dist <= self.phash_threshold {
            consecutive_idle_count += 1;
            debug!(
                dist,
                consecutive_idle_count,
                threshold = self.config.active_to_idle_consecutive_frames,
                "ACTIVE: consecutive similar frame"
            );

            if consecutive_idle_count >= self.config.active_to_idle_consecutive_frames {
                info!(
                    consecutive_idle_count,
                    segment_start_ms,
                    "ACTIVE→IDLE: scene stabilized, finalizing active segment"
                );
                self.finish_and_upload_segment(encoder, frame.captured_at_ms).await;
                return RecordingState::Idle {
                    initial_jpeg: frame.jpeg_data.clone(),
                    initial_hash: hash.to_vec(),
                    idle_start_ms: frame.captured_at_ms,
                    last_similar_ms: frame.captured_at_ms,
                };
            }

            // Not enough consecutive similar frames yet — stay Active.
            RecordingState::Active {
                encoder,
                segment_deadline,
                segment_start_ms,
                last_frame_hash: hash.to_vec(),
                consecutive_idle_count,
            }
        } else {
            // Scene is still changing — reset consecutive counter.
            RecordingState::Active {
                encoder,
                segment_deadline,
                segment_start_ms,
                last_frame_hash: hash.to_vec(),
                consecutive_idle_count: 0,
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// Spawn a new SegmentEncoder and push the first frame. Returns the Active state,
    /// or None if ffmpeg failed to start.
    async fn start_active_segment(
        &self,
        frame: &TimestampedFrame,
        hash: Vec<bool>,
    ) -> Option<RecordingState> {
        let mut encoder = match SegmentEncoder::start(
            frame.captured_at_ms,
            &self.config.codec,
            self.config.crf,
            &self.config.preset,
            self.config.fps,
        )
        .await
        {
            Ok(e) => e,
            Err(EncoderError::Spawn(e)) => {
                error!(error = %e, "failed to spawn ffmpeg");
                return None;
            }
            Err(e) => {
                error!(error = %e, "unexpected encoder start error");
                return None;
            }
        };

        if let Err(e) = encoder.push_frame(&frame.jpeg_data).await {
            error!(error = %e, "failed to push first frame to new encoder");
            return None;
        }

        let segment_start_ms = frame.captured_at_ms;
        let segment_deadline =
            Instant::now() + Duration::from_secs(self.config.segment_duration_secs);

        info!(
            segment_start_ms,
            codec = self.config.codec,
            "ACTIVE: new segment started"
        );

        Some(RecordingState::Active {
            encoder,
            segment_deadline,
            segment_start_ms,
            last_frame_hash: hash,
            consecutive_idle_count: 0,
        })
    }

    /// Finalize the encoder and upload the resulting MP4 to RustFS. Inserts metadata into SQLite on success.
    async fn finish_and_upload_segment(&self, encoder: SegmentEncoder, end_ms: i64) {
        let start_ms = encoder.start_ms;
        match encoder.finish().await {
            Ok(seg) => {
                let key = active_segment_key(&self.prefix, &self.robot_id, start_ms, end_ms);
                let size_bytes = seg.mp4_bytes.len() as u64;
                match self
                    .storage
                    .put_segment(&key, seg.mp4_bytes, start_ms)
                    .await
                {
                    Ok(()) => {
                        info!(
                            key,
                            frames = seg.frame_count,
                            start_ms,
                            end_ms,
                            "uploaded active segment to RustFS"
                        );
                        if let Some(db) = &self.db {
                            if let Err(e) = db.insert_active(start_ms, end_ms, &key, size_bytes, seg.frame_count) {
                                error!(error = %e, key, "failed to insert active segment into SQLite");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, key, "failed to upload segment to RustFS");
                    }
                }
            }
            Err(e) => {
                error!(error = %e, start_ms, end_ms, "encoder finish failed, segment lost");
            }
        }
    }

    /// Upload the idle period's representative JPEG to RustFS.
    async fn upload_idle_record(
        &self,
        initial_jpeg: &[u8],
        idle_start_ms: i64,
        idle_end_ms: i64,
    ) {
        let jpeg_key = idle_jpeg_key(&self.prefix, &self.robot_id, idle_start_ms, idle_end_ms);

        let jpeg_size = initial_jpeg.len() as u64;
        match self
            .storage
            .put_idle_frame(&jpeg_key, initial_jpeg.to_vec(), idle_start_ms)
            .await
        {
            Ok(()) => {
                info!(
                    key = jpeg_key,
                    idle_start_ms,
                    idle_end_ms,
                    "uploaded idle frame to RustFS"
                );
                if let Some(db) = &self.db {
                    if let Err(e) = db.insert_idle(idle_start_ms, idle_end_ms, &jpeg_key, jpeg_size) {
                        error!(error = %e, key = jpeg_key, "failed to insert idle record into SQLite");
                    }
                }
            }
            Err(e) => {
                error!(error = %e, key = jpeg_key, "failed to upload idle frame");
            }
        }
    }
}
