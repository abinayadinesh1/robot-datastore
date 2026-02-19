use bytes::BytesMut;
use chrono::Utc;
use frame_bucket_common::frame::TimestampedFrame;
use futures_util::StreamExt;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::ProducerError;

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

pub fn create_producer(
    brokers: &str,
    compression: &str,
) -> Result<FutureProducer, ProducerError> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.max.bytes", "1048576")
        .set("compression.type", compression)
        .set("linger.ms", "5")
        .set("batch.num.messages", "10")
        .set("queue.buffering.max.messages", "1000")
        .set("request.timeout.ms", "5000")
        .create()
        .map_err(|e| ProducerError::KafkaCreate(e.to_string()))?;
    Ok(producer)
}

/// Consume the MJPEG stream and produce frames to Kafka.
/// Reconnects with exponential backoff on failure.
pub async fn run_mjpeg_producer(
    stream_url: &str,
    topic: &str,
    producer: &FutureProducer,
    robot_id: &str,
) -> Result<(), ProducerError> {
    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!(url = stream_url, robot_id, "connecting to MJPEG stream");
        match consume_stream(stream_url, topic, producer, robot_id).await {
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
    topic: &str,
    producer: &FutureProducer,
    robot_id: &str,
) -> Result<(), ProducerError> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(ProducerError::HttpConnect)?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(ProducerError::HttpConnect)?;

    if !response.status().is_success() {
        return Err(ProducerError::HttpStatus(response.status().as_u16()));
    }

    info!(status = %response.status(), "connected to MJPEG stream");

    let mut byte_stream = response.bytes_stream();
    let mut buffer = BytesMut::with_capacity(256 * 1024);
    let mut state = ParseState::SeekingBoundary;
    let mut jpeg_start: usize = 0;

    while let Some(chunk) = byte_stream.next().await {
        let chunk = chunk.map_err(ProducerError::HttpStream)?;
        buffer.extend_from_slice(&chunk);

        loop {
            match state {
                ParseState::SeekingBoundary => {
                    if let Some(pos) = find_subsequence(&buffer, BOUNDARY) {
                        // Discard everything up to and including the boundary
                        let _ = buffer.split_to(pos + BOUNDARY.len());
                        state = ParseState::SeekingHeaderEnd;
                    } else {
                        // Keep last few bytes in case boundary spans chunks
                        if buffer.len() > BOUNDARY.len() {
                            let _ = buffer.split_to(buffer.len() - BOUNDARY.len());
                        }
                        break;
                    }
                }
                ParseState::SeekingHeaderEnd => {
                    if let Some(pos) = find_subsequence(&buffer, HEADER_END) {
                        // Discard headers
                        let _ = buffer.split_to(pos + HEADER_END.len());
                        jpeg_start = 0;
                        state = ParseState::CollectingJpeg;
                    } else {
                        break;
                    }
                }
                ParseState::CollectingJpeg => {
                    // Look for the next boundary to know where JPEG ends
                    if let Some(pos) = find_subsequence(&buffer[jpeg_start..], BOUNDARY) {
                        let jpeg_end = jpeg_start + pos;
                        // Strip trailing \r\n before boundary
                        let end = if jpeg_end >= 2
                            && buffer[jpeg_end - 2] == b'\r'
                            && buffer[jpeg_end - 1] == b'\n'
                        {
                            jpeg_end - 2
                        } else {
                            jpeg_end
                        };

                        let jpeg_data = buffer[..end].to_vec();

                        // Advance past the boundary
                        let _ = buffer.split_to(jpeg_end + BOUNDARY.len());

                        if !jpeg_data.is_empty() {
                            let seq = SEQ_COUNTER.fetch_add(1, Ordering::Relaxed);
                            let now_ms = Utc::now().timestamp_millis();
                            let frame = TimestampedFrame::new(jpeg_data, now_ms, seq);
                            let payload = frame.serialize();
                            let key = format!("{}:{}", robot_id, now_ms);

                            debug!(seq, bytes = payload.len(), "producing frame to Kafka");

                            let record = FutureRecord::to(topic)
                                .key(&key)
                                .payload(&payload);

                            if let Err((e, _)) =
                                producer.send(record, Duration::from_secs(5)).await
                            {
                                warn!(error = %e, seq, "failed to produce frame to Kafka");
                            }
                        }

                        // Already past boundary, go to header parsing
                        state = ParseState::SeekingHeaderEnd;
                    } else {
                        // No boundary found yet, keep accumulating
                        // Update jpeg_start to avoid re-scanning old data
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
pub async fn run_polling_producer(
    frame_url: &str,
    topic: &str,
    producer: &FutureProducer,
    interval: Duration,
    robot_id: &str,
) -> Result<(), ProducerError> {
    let client = reqwest::Client::new();
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;

        match client.get(frame_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let jpeg_data = resp.bytes().await.map_err(ProducerError::HttpStream)?.to_vec();
                let seq = SEQ_COUNTER.fetch_add(1, Ordering::Relaxed);
                let now_ms = Utc::now().timestamp_millis();
                let frame = TimestampedFrame::new(jpeg_data, now_ms, seq);
                let payload = frame.serialize();
                let key = format!("{}:{}", robot_id, now_ms);

                let record = FutureRecord::to(topic).key(&key).payload(&payload);
                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    warn!(error = %e, seq, "failed to produce frame to Kafka");
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
