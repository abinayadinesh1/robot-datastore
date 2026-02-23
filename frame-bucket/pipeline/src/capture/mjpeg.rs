use bytes::BytesMut;
use chrono::Utc;
use frame_bucket_common::frame::TimestampedFrame;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

static SEQ_COUNTER: AtomicU64 = AtomicU64::new(0);

const BOUNDARY: &[u8] = b"--frame\r\n";
const HEADER_END: &[u8] = b"\r\n\r\n";

/// Parse state for the MJPEG multipart stream.
enum ParseState {
    /// Looking for the boundary marker `--frame\r\n`.
    SeekingBoundary,
    /// Found boundary, now looking for end of headers `\r\n\r\n`.
    SeekingHeaderEnd,
    /// Collecting JPEG bytes until the next boundary.
    CollectingJpeg,
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("HTTP connection failed: {0}")]
    HttpConnect(reqwest::Error),
    #[error("HTTP stream error: {0}")]
    HttpStream(reqwest::Error),
    #[error("HTTP status {0}")]
    HttpStatus(u16),
}

/// Consume the MJPEG stream and send frames to the channel.
/// Reconnects with exponential backoff on failure.
pub async fn run_mjpeg_capture(
    stream_url: &str,
    tx: mpsc::Sender<TimestampedFrame>,
    robot_id: &str,
) {
    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!(url = stream_url, robot_id, "connecting to MJPEG stream");
        match consume_stream(stream_url, &tx, robot_id).await {
            Ok(()) => {
                info!(robot_id, "stream ended cleanly, reconnecting");
                backoff = Duration::from_secs(2);
            }
            Err(e) => {
                error!(error = %e, robot_id, "stream error, reconnecting in {:?}", backoff);
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn consume_stream(
    url: &str,
    tx: &mpsc::Sender<TimestampedFrame>,
    robot_id: &str,
) -> Result<(), CaptureError> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(CaptureError::HttpConnect)?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(CaptureError::HttpConnect)?;

    if !response.status().is_success() {
        return Err(CaptureError::HttpStatus(response.status().as_u16()));
    }

    info!(status = %response.status(), "connected to MJPEG stream");

    let mut byte_stream = response.bytes_stream();
    let mut buffer = BytesMut::with_capacity(256 * 1024);
    let mut state = ParseState::SeekingBoundary;
    let mut jpeg_start: usize = 0;

    while let Some(chunk) = byte_stream.next().await {
        let chunk = chunk.map_err(CaptureError::HttpStream)?;
        buffer.extend_from_slice(&chunk);

        loop {
            match state {
                ParseState::SeekingBoundary => {
                    if let Some(pos) = find_subsequence(&buffer, BOUNDARY) {
                        let _ = buffer.split_to(pos + BOUNDARY.len());
                        state = ParseState::SeekingHeaderEnd;
                    } else {
                        if buffer.len() > BOUNDARY.len() {
                            let _ = buffer.split_to(buffer.len() - BOUNDARY.len());
                        }
                        break;
                    }
                }
                ParseState::SeekingHeaderEnd => {
                    if let Some(pos) = find_subsequence(&buffer, HEADER_END) {
                        let _ = buffer.split_to(pos + HEADER_END.len());
                        jpeg_start = 0;
                        state = ParseState::CollectingJpeg;
                    } else {
                        break;
                    }
                }
                ParseState::CollectingJpeg => {
                    if let Some(pos) = find_subsequence(&buffer[jpeg_start..], BOUNDARY) {
                        let jpeg_end = jpeg_start + pos;
                        let end = if jpeg_end >= 2
                            && buffer[jpeg_end - 2] == b'\r'
                            && buffer[jpeg_end - 1] == b'\n'
                        {
                            jpeg_end - 2
                        } else {
                            jpeg_end
                        };

                        let jpeg_data = buffer[..end].to_vec();
                        let _ = buffer.split_to(jpeg_end + BOUNDARY.len());

                        if !jpeg_data.is_empty() {
                            let seq = SEQ_COUNTER.fetch_add(1, Ordering::Relaxed);
                            let now_ms = Utc::now().timestamp_millis();
                            let frame = TimestampedFrame::new(jpeg_data, now_ms, seq);

                            debug!(seq, robot_id, "captured MJPEG frame");

                            if tx.send(frame).await.is_err() {
                                warn!(robot_id, "frame channel closed, stopping capture");
                                return Ok(());
                            }
                        }

                        state = ParseState::SeekingHeaderEnd;
                    } else {
                        jpeg_start = if buffer.len() > BOUNDARY.len() {
                            buffer.len() - BOUNDARY.len()
                        } else {
                            0
                        };
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Polling-based fallback: periodically fetch single frames.
pub async fn run_polling_capture(
    frame_url: &str,
    tx: mpsc::Sender<TimestampedFrame>,
    interval: Duration,
    robot_id: &str,
) {
    let client = reqwest::Client::new();
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;

        match client.get(frame_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.bytes().await {
                    Ok(bytes) => {
                        let jpeg_data = bytes.to_vec();
                        let seq = SEQ_COUNTER.fetch_add(1, Ordering::Relaxed);
                        let now_ms = Utc::now().timestamp_millis();
                        let frame = TimestampedFrame::new(jpeg_data, now_ms, seq);

                        if tx.send(frame).await.is_err() {
                            warn!(robot_id, "frame channel closed, stopping capture");
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to read camera response body");
                    }
                }
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "non-success response from camera");
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch camera frame");
            }
        }
    }
}

/// Find the position of `needle` in `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
