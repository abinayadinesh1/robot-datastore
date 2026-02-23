use std::sync::Arc;
use std::time::Duration;

use frame_bucket_common::config::RecordingConfig;
use frame_bucket_common::frame::{FramePayload, TimestampedFrame};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::db::SegmentDb;
use crate::filter::framesize::FrameSizeFilter;
use crate::filter::phash::{compute_ahash, hamming};
use crate::storage::RustfsStorage;

use super::encoder::SegmentEncoder;
use super::keys::{active_segment_key, idle_jpeg_key};

#[allow(dead_code)]
enum RecordingState {
    /// The scene is static. We track the initial frame and the last timestamp
    /// at which the scene was still considered unchanged.
    Idle {
        /// Raw bytes of the initial frame (JPEG or H.264 keyframe) for the idle record.
        initial_payload: Vec<u8>,
        /// Whether the initial frame is H.264 (false = JPEG).
        is_h264: bool,
        /// aHash of the initial frame (only for JPEG path).
        initial_hash: Option<Vec<bool>>,
        idle_start_ms: i64,
        /// Last frame timestamp that was still considered "idle".
        last_similar_ms: i64,
    },
    /// The scene is changing. We encode frames into MP4 segments.
    Active {
        encoder: SegmentEncoder,
        /// Whether we're in H.264 passthrough mode (false = JPEG re-encode).
        is_h264: bool,
        /// Monotonic deadline for rolling the current segment.
        segment_deadline: Instant,
        segment_start_ms: i64,
        /// aHash of the last frame (only for JPEG path).
        last_frame_hash: Option<Vec<bool>>,
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
    /// Frame-size heuristic filter for H.264 streams.
    frame_size_filter: FrameSizeFilter,
}

impl RecordingStateMachine {
    pub fn new(
        config: RecordingConfig,
        phash_threshold: u32,
        hash_size: u32,
        spike_ratio: f64,
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
            frame_size_filter: FrameSizeFilter::new(spike_ratio),
        }
    }

    /// Process one incoming frame from Kafka. This is the main entry point.
    pub async fn process_frame(&mut self, frame: &TimestampedFrame) {
        match &frame.payload {
            FramePayload::Jpeg(jpeg_data) => {
                self.process_jpeg_frame(frame, jpeg_data).await;
            }
            FramePayload::H264 { data, nal_type } => {
                self.process_h264_frame(frame, data, *nal_type).await;
            }
        }
    }

    // =========================================================================
    // JPEG path (existing aHash logic)
    // =========================================================================

    async fn process_jpeg_frame(&mut self, frame: &TimestampedFrame, jpeg_data: &[u8]) {
        let hash = match compute_ahash(jpeg_data, self.hash_size) {
            Some(h) => h,
            None => {
                warn!(ts = frame.captured_at_ms, "failed to compute aHash for frame, skipping");
                return;
            }
        };

        // First frame ever: enter Idle.
        if self.state.is_none() {
            info!(ts = frame.captured_at_ms, "first frame (JPEG) — entering IDLE mode");
            self.state = Some(RecordingState::Idle {
                initial_payload: jpeg_data.to_vec(),
                is_h264: false,
                initial_hash: Some(hash),
                idle_start_ms: frame.captured_at_ms,
                last_similar_ms: frame.captured_at_ms,
            });
            return;
        }

        match self.state.take().unwrap() {
            idle @ RecordingState::Idle { .. } => {
                self.state = Some(self.handle_idle_jpeg(idle, frame, jpeg_data, &hash).await);
            }
            active @ RecordingState::Active { .. } => {
                self.state = Some(self.handle_active_jpeg(active, frame, jpeg_data, &hash).await);
            }
        }
    }

    async fn handle_idle_jpeg(
        &mut self,
        state: RecordingState,
        frame: &TimestampedFrame,
        jpeg_data: &[u8],
        hash: &[bool],
    ) -> RecordingState {
        let RecordingState::Idle {
            initial_payload,
            initial_hash,
            idle_start_ms,
            last_similar_ms,
            ..
        } = state
        else {
            unreachable!()
        };

        let initial_hash_ref = initial_hash.as_ref().expect("JPEG idle must have hash");
        let dist = hamming(initial_hash_ref, hash);

        if dist <= self.phash_threshold {
            debug!(
                dist,
                threshold = self.phash_threshold,
                ts = frame.captured_at_ms,
                "IDLE: frame similar to baseline"
            );
            return RecordingState::Idle {
                initial_payload,
                is_h264: false,
                initial_hash,
                idle_start_ms,
                last_similar_ms: frame.captured_at_ms,
            };
        }

        info!(
            dist,
            threshold = self.phash_threshold,
            idle_start_ms,
            idle_end_ms = last_similar_ms,
            "IDLE→ACTIVE: scene changed, finalizing idle record"
        );
        self.upload_idle_record(&initial_payload, false, idle_start_ms, last_similar_ms)
            .await;

        match self
            .start_active_segment_jpeg(frame, jpeg_data, hash.to_vec())
            .await
        {
            Some(active_state) => active_state,
            None => {
                warn!("ffmpeg failed to start, staying in IDLE with new baseline");
                RecordingState::Idle {
                    initial_payload: jpeg_data.to_vec(),
                    is_h264: false,
                    initial_hash: Some(hash.to_vec()),
                    idle_start_ms: frame.captured_at_ms,
                    last_similar_ms: frame.captured_at_ms,
                }
            }
        }
    }

    async fn handle_active_jpeg(
        &mut self,
        state: RecordingState,
        frame: &TimestampedFrame,
        jpeg_data: &[u8],
        hash: &[bool],
    ) -> RecordingState {
        let RecordingState::Active {
            mut encoder,
            segment_deadline,
            segment_start_ms,
            last_frame_hash,
            mut consecutive_idle_count,
            ..
        } = state
        else {
            unreachable!()
        };

        // Check segment timer
        if Instant::now() >= segment_deadline {
            info!(
                segment_start_ms,
                end_ms = frame.captured_at_ms,
                frames = encoder.frame_count(),
                "ACTIVE: rolling segment (timer expired)"
            );
            self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                .await;

            return match self
                .start_active_segment_jpeg(frame, jpeg_data, hash.to_vec())
                .await
            {
                Some(s) => s,
                None => {
                    warn!("ffmpeg failed to start new segment, falling back to IDLE");
                    RecordingState::Idle {
                        initial_payload: jpeg_data.to_vec(),
                        is_h264: false,
                        initial_hash: Some(hash.to_vec()),
                        idle_start_ms: frame.captured_at_ms,
                        last_similar_ms: frame.captured_at_ms,
                    }
                }
            };
        }

        // Push frame to encoder
        if let Err(e) = encoder.push_frame(jpeg_data).await {
            error!(error = %e, "ACTIVE: failed to push frame to encoder, finalizing broken segment");
            self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                .await;
            return RecordingState::Idle {
                initial_payload: jpeg_data.to_vec(),
                is_h264: false,
                initial_hash: Some(hash.to_vec()),
                idle_start_ms: frame.captured_at_ms,
                last_similar_ms: frame.captured_at_ms,
            };
        }

        // Check Active→Idle transition (consecutive similar frames)
        let prev_hash = last_frame_hash.as_ref().expect("JPEG active must have hash");
        let dist = hamming(prev_hash, hash);
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
                    segment_start_ms, "ACTIVE→IDLE: scene stabilized, finalizing active segment"
                );
                self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                    .await;
                return RecordingState::Idle {
                    initial_payload: jpeg_data.to_vec(),
                    is_h264: false,
                    initial_hash: Some(hash.to_vec()),
                    idle_start_ms: frame.captured_at_ms,
                    last_similar_ms: frame.captured_at_ms,
                };
            }

            RecordingState::Active {
                encoder,
                is_h264: false,
                segment_deadline,
                segment_start_ms,
                last_frame_hash: Some(hash.to_vec()),
                consecutive_idle_count,
            }
        } else {
            RecordingState::Active {
                encoder,
                is_h264: false,
                segment_deadline,
                segment_start_ms,
                last_frame_hash: Some(hash.to_vec()),
                consecutive_idle_count: 0,
            }
        }
    }

    async fn start_active_segment_jpeg(
        &self,
        frame: &TimestampedFrame,
        jpeg_data: &[u8],
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
            Err(e) => {
                error!(error = %e, "failed to spawn ffmpeg");
                return None;
            }
        };

        if let Err(e) = encoder.push_frame(jpeg_data).await {
            error!(error = %e, "failed to push first frame to new encoder");
            return None;
        }

        let segment_deadline =
            Instant::now() + Duration::from_secs(self.config.segment_duration_secs);

        info!(
            segment_start_ms = frame.captured_at_ms,
            codec = self.config.codec,
            "ACTIVE: new JPEG segment started"
        );

        Some(RecordingState::Active {
            encoder,
            is_h264: false,
            segment_deadline,
            segment_start_ms: frame.captured_at_ms,
            last_frame_hash: Some(hash),
            consecutive_idle_count: 0,
        })
    }

    // =========================================================================
    // H.264 path (frame-size heuristic filter)
    // =========================================================================

    async fn process_h264_frame(
        &mut self,
        frame: &TimestampedFrame,
        h264_data: &[u8],
        nal_type: u8,
    ) {
        let frame_size = h264_data.len();
        let is_active = self.frame_size_filter.is_active(frame_size, nal_type);

        // First frame ever: enter Idle.
        if self.state.is_none() {
            info!(
                ts = frame.captured_at_ms,
                nal_type, "first frame (H.264) — entering IDLE mode"
            );
            self.state = Some(RecordingState::Idle {
                initial_payload: h264_data.to_vec(),
                is_h264: true,
                initial_hash: None,
                idle_start_ms: frame.captured_at_ms,
                last_similar_ms: frame.captured_at_ms,
            });
            return;
        }

        match self.state.take().unwrap() {
            RecordingState::Idle {
                initial_payload,
                idle_start_ms,
                last_similar_ms,
                ..
            } => {
                if !is_active {
                    // Still idle
                    debug!(
                        frame_size,
                        nal_type,
                        ts = frame.captured_at_ms,
                        "IDLE (H.264): scene quiet"
                    );
                    self.state = Some(RecordingState::Idle {
                        initial_payload,
                        is_h264: true,
                        initial_hash: None,
                        idle_start_ms,
                        last_similar_ms: frame.captured_at_ms,
                    });
                } else {
                    // Scene changed → ACTIVE
                    info!(
                        frame_size,
                        nal_type, idle_start_ms, "IDLE→ACTIVE (H.264): motion detected"
                    );
                    self.upload_idle_record(
                        &initial_payload,
                        true,
                        idle_start_ms,
                        last_similar_ms,
                    )
                    .await;

                    match self.start_active_segment_h264(frame, h264_data).await {
                        Some(active_state) => {
                            self.state = Some(active_state);
                        }
                        None => {
                            warn!("ffmpeg failed to start, staying in IDLE with new baseline");
                            self.state = Some(RecordingState::Idle {
                                initial_payload: h264_data.to_vec(),
                                is_h264: true,
                                initial_hash: None,
                                idle_start_ms: frame.captured_at_ms,
                                last_similar_ms: frame.captured_at_ms,
                            });
                        }
                    }
                }
            }
            RecordingState::Active {
                mut encoder,
                segment_deadline,
                segment_start_ms,
                mut consecutive_idle_count,
                ..
            } => {
                // Check segment timer
                if Instant::now() >= segment_deadline {
                    info!(
                        segment_start_ms,
                        end_ms = frame.captured_at_ms,
                        frames = encoder.frame_count(),
                        "ACTIVE (H.264): rolling segment (timer)"
                    );
                    self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                        .await;

                    match self.start_active_segment_h264(frame, h264_data).await {
                        Some(s) => self.state = Some(s),
                        None => {
                            self.state = Some(RecordingState::Idle {
                                initial_payload: h264_data.to_vec(),
                                is_h264: true,
                                initial_hash: None,
                                idle_start_ms: frame.captured_at_ms,
                                last_similar_ms: frame.captured_at_ms,
                            });
                        }
                    }
                    return;
                }

                // Push frame to encoder
                if let Err(e) = encoder.push_h264(h264_data).await {
                    error!(error = %e, "ACTIVE (H.264): failed to push frame, finalizing");
                    self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                        .await;
                    self.state = Some(RecordingState::Idle {
                        initial_payload: h264_data.to_vec(),
                        is_h264: true,
                        initial_hash: None,
                        idle_start_ms: frame.captured_at_ms,
                        last_similar_ms: frame.captured_at_ms,
                    });
                    return;
                }

                // Check Active→Idle: count consecutive quiet P-frames
                if self.frame_size_filter.is_quiet(frame_size) {
                    consecutive_idle_count += 1;
                    debug!(
                        frame_size,
                        consecutive_idle_count,
                        threshold = self.config.active_to_idle_consecutive_frames,
                        "ACTIVE (H.264): quiet frame"
                    );
                    if consecutive_idle_count >= self.config.active_to_idle_consecutive_frames {
                        info!(
                            consecutive_idle_count,
                            segment_start_ms, "ACTIVE→IDLE (H.264): scene stabilized"
                        );
                        self.finish_and_upload_segment(encoder, frame.captured_at_ms)
                            .await;
                        self.state = Some(RecordingState::Idle {
                            initial_payload: h264_data.to_vec(),
                            is_h264: true,
                            initial_hash: None,
                            idle_start_ms: frame.captured_at_ms,
                            last_similar_ms: frame.captured_at_ms,
                        });
                        return;
                    }
                } else {
                    consecutive_idle_count = 0;
                }

                self.state = Some(RecordingState::Active {
                    encoder,
                    is_h264: true,
                    segment_deadline,
                    segment_start_ms,
                    last_frame_hash: None,
                    consecutive_idle_count,
                });
            }
        }
    }

    async fn start_active_segment_h264(
        &self,
        frame: &TimestampedFrame,
        h264_data: &[u8],
    ) -> Option<RecordingState> {
        let mut encoder =
            match SegmentEncoder::start_passthrough(frame.captured_at_ms, self.config.fps).await {
                Ok(e) => e,
                Err(e) => {
                    error!(error = %e, "failed to spawn ffmpeg passthrough");
                    return None;
                }
            };

        if let Err(e) = encoder.push_h264(h264_data).await {
            error!(error = %e, "failed to push first H.264 AU to encoder");
            return None;
        }

        let segment_deadline =
            Instant::now() + Duration::from_secs(self.config.segment_duration_secs);

        info!(
            segment_start_ms = frame.captured_at_ms,
            "ACTIVE: new H.264 passthrough segment started"
        );

        Some(RecordingState::Active {
            encoder,
            is_h264: true,
            segment_deadline,
            segment_start_ms: frame.captured_at_ms,
            last_frame_hash: None,
            consecutive_idle_count: 0,
        })
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    /// Finalize the encoder and upload the resulting MP4 to RustFS.
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
                            if let Err(e) = db.insert_active(
                                start_ms,
                                end_ms,
                                &key,
                                size_bytes,
                                seg.frame_count,
                            ) {
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

    /// Upload the idle period's representative frame to RustFS.
    async fn upload_idle_record(
        &self,
        initial_payload: &[u8],
        is_h264: bool,
        idle_start_ms: i64,
        idle_end_ms: i64,
    ) {
        if is_h264 {
            // For H.264, we skip uploading the idle frame — there's no JPEG to display.
            // Just log the idle period and insert into SQLite.
            info!(idle_start_ms, idle_end_ms, "H.264 idle period (no snapshot)");
            if let Some(db) = &self.db {
                let key = format!("idle:{}/{}", idle_start_ms, idle_end_ms);
                if let Err(e) = db.insert_idle(idle_start_ms, idle_end_ms, &key, 0) {
                    error!(error = %e, "failed to insert H.264 idle record into SQLite");
                }
            }
            return;
        }

        let jpeg_key = idle_jpeg_key(&self.prefix, &self.robot_id, idle_start_ms, idle_end_ms);
        let jpeg_size = initial_payload.len() as u64;
        match self
            .storage
            .put_idle_frame(&jpeg_key, initial_payload.to_vec(), idle_start_ms)
            .await
        {
            Ok(()) => {
                info!(
                    key = jpeg_key,
                    idle_start_ms, idle_end_ms, "uploaded idle frame to RustFS"
                );
                if let Some(db) = &self.db {
                    if let Err(e) =
                        db.insert_idle(idle_start_ms, idle_end_ms, &jpeg_key, jpeg_size)
                    {
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
