use chrono::Utc;
use frame_bucket_common::frame::TimestampedFrame;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::ProducerError;

static H264_SEQ_COUNTER: AtomicU64 = AtomicU64::new(0);

const TS_PACKET_SIZE: usize = 188;
const TS_SYNC_BYTE: u8 = 0x47;

/// Run the H.264 TCP producer with exponential backoff reconnection.
pub async fn run_h264_producer(
    h264_addr: &str,
    topic: &str,
    producer: &FutureProducer,
    robot_id: &str,
) -> Result<(), ProducerError> {
    let mut backoff = Duration::from_secs(2);
    let max_backoff = Duration::from_secs(30);

    loop {
        info!(addr = h264_addr, robot_id, "connecting to H.264 TCP stream");
        match consume_h264_stream(h264_addr, topic, producer, robot_id).await {
            Ok(()) => {
                info!(robot_id, "H.264 stream ended, reconnecting");
                backoff = Duration::from_secs(2);
            }
            Err(e) => {
                error!(error = %e, robot_id, "H.264 stream error, reconnecting in {:?}", backoff);
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn consume_h264_stream(
    addr: &str,
    topic: &str,
    producer: &FutureProducer,
    robot_id: &str,
) -> Result<(), ProducerError> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| ProducerError::TcpConnect(e.to_string()))?;

    info!("connected to H.264 TCP stream at {}", addr);

    let mut read_buf = vec![0u8; 64 * 1024];
    let mut ts_buf = Vec::with_capacity(256 * 1024);
    let mut pes_assembler = PesAssembler::new();

    loop {
        let n = stream
            .read(&mut read_buf)
            .await
            .map_err(|e| ProducerError::TcpStream(e.to_string()))?;
        if n == 0 {
            return Ok(()); // stream closed
        }

        ts_buf.extend_from_slice(&read_buf[..n]);

        // Process complete 188-byte TS packets
        while ts_buf.len() >= TS_PACKET_SIZE {
            // Find sync byte
            let sync_pos = match ts_buf.iter().position(|&b| b == TS_SYNC_BYTE) {
                Some(pos) => pos,
                None => {
                    ts_buf.clear();
                    break;
                }
            };

            // Discard bytes before sync
            if sync_pos > 0 {
                ts_buf.drain(..sync_pos);
            }

            if ts_buf.len() < TS_PACKET_SIZE {
                break;
            }

            // Verify next packet also starts with sync byte (if available)
            if ts_buf.len() >= TS_PACKET_SIZE * 2 && ts_buf[TS_PACKET_SIZE] != TS_SYNC_BYTE {
                // Bad alignment, skip this byte and resync
                ts_buf.drain(..1);
                continue;
            }

            let packet: Vec<u8> = ts_buf.drain(..TS_PACKET_SIZE).collect();

            if let Some(access_unit) = pes_assembler.push_ts_packet(&packet) {
                let nal_type = detect_nal_type(&access_unit);
                let seq = H264_SEQ_COUNTER.fetch_add(1, Ordering::Relaxed);
                let now_ms = Utc::now().timestamp_millis();

                let frame = TimestampedFrame::new_h264(access_unit, nal_type, now_ms, seq);
                let payload = frame.serialize();
                let key = format!("{}:{}", robot_id, now_ms);

                debug!(seq, nal_type, bytes = payload.len(), "producing H.264 frame to Kafka");

                let record = FutureRecord::to(topic).key(&key).payload(&payload);

                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    warn!(error = %e, seq, "failed to produce H.264 frame to Kafka");
                }
            }
        }
    }
}

/// Reassembles PES packets from MPEG-TS packets.
///
/// Each complete PES packet carries one H.264 access unit (one frame).
/// A new PES starts when we see a TS packet with PUSI=1; at that point
/// we emit the previously accumulated access unit.
struct PesAssembler {
    /// Accumulated H.264 ES bytes for the current access unit.
    current_pes: Vec<u8>,
    /// PID of the video elementary stream (auto-detected from first PES).
    video_pid: Option<u16>,
    /// Whether we are currently collecting PES data.
    collecting: bool,
}

impl PesAssembler {
    fn new() -> Self {
        Self {
            current_pes: Vec::with_capacity(128 * 1024),
            video_pid: None,
            collecting: false,
        }
    }

    /// Feed a 188-byte TS packet. Returns a complete H.264 access unit
    /// when a PES boundary is detected (the *next* PES starts, so we
    /// emit the previous one).
    fn push_ts_packet(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        if packet.len() < TS_PACKET_SIZE || packet[0] != TS_SYNC_BYTE {
            return None;
        }

        let pid = (((packet[1] & 0x1F) as u16) << 8) | packet[2] as u16;
        let pusi = (packet[1] & 0x40) != 0;
        let afc = (packet[3] >> 4) & 0x03;
        let has_payload = (afc & 0x01) != 0;

        // Skip PAT (0x0000) and null (0x1FFF) packets
        if pid == 0x0000 || pid == 0x1FFF {
            return None;
        }

        // Auto-detect video PID from first PES start with a video stream_id
        if self.video_pid.is_none() && pusi && has_payload {
            let offset = payload_offset(packet, afc);
            if offset + 4 <= TS_PACKET_SIZE {
                let p = &packet[offset..];
                // PES start code: 00 00 01 [stream_id]
                if p.len() >= 4 && p[0] == 0x00 && p[1] == 0x00 && p[2] == 0x01 {
                    let stream_id = p[3];
                    // Video stream IDs: 0xE0..0xEF
                    if (0xE0..=0xEF).contains(&stream_id) {
                        self.video_pid = Some(pid);
                        info!(pid, stream_id, "auto-detected video PID");
                    }
                }
            }
        }

        // Only process our video PID
        if self.video_pid != Some(pid) || !has_payload {
            return None;
        }

        let offset = payload_offset(packet, afc);
        if offset >= TS_PACKET_SIZE {
            return None;
        }

        let mut completed_au = None;

        if pusi {
            // New PES starting — emit the accumulated access unit
            if self.collecting && !self.current_pes.is_empty() {
                completed_au = Some(std::mem::take(&mut self.current_pes));
            }

            // Parse PES header to find where ES data begins:
            //   start_code_prefix(3) + stream_id(1) + pes_length(2)
            //   + flags(2) + header_data_length(1) + [header_data]
            let payload = &packet[offset..];
            if payload.len() >= 9
                && payload[0] == 0x00
                && payload[1] == 0x00
                && payload[2] == 0x01
            {
                let header_data_len = payload[8] as usize;
                let es_start = 9 + header_data_len;
                if es_start < payload.len() {
                    self.current_pes.extend_from_slice(&payload[es_start..]);
                }
                self.collecting = true;
            }
        } else if self.collecting {
            // Continuation of current PES — append raw payload
            self.current_pes
                .extend_from_slice(&packet[offset..]);
        }

        completed_au
    }
}

/// Compute the byte offset where the TS payload begins, accounting for
/// the optional adaptation field.
fn payload_offset(packet: &[u8], afc: u8) -> usize {
    let has_adaptation = (afc & 0x02) != 0;
    if has_adaptation && packet.len() > 4 {
        let adaptation_length = packet[4] as usize;
        5 + adaptation_length
    } else {
        4 // header only
    }
}

/// Detect the NAL unit type from H.264 Annex B byte-stream data.
///
/// Scans for start codes (0x000001 or 0x00000001) and returns the NAL type
/// of the first VCL NAL (types 1–5). Falls back to the first NAL found.
fn detect_nal_type(data: &[u8]) -> u8 {
    let mut i = 0;
    let mut first_nal_type = 0u8;

    while i + 3 <= data.len() {
        let nal_offset = if data[i] == 0x00 && data[i + 1] == 0x00 {
            if data[i + 2] == 0x01 {
                Some(i + 3)
            } else if i + 3 < data.len() && data[i + 2] == 0x00 && data[i + 3] == 0x01 {
                Some(i + 4)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(offset) = nal_offset {
            if offset < data.len() {
                let nal_type = data[offset] & 0x1F;
                if first_nal_type == 0 {
                    first_nal_type = nal_type;
                }
                // VCL NAL types: 1 = non-IDR slice, 5 = IDR slice
                if (1..=5).contains(&nal_type) {
                    return nal_type;
                }
            }
            i = offset;
        } else {
            i += 1;
        }
    }

    first_nal_type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_idr_nal() {
        // 4-byte start code + IDR NAL (type 5)
        let data = [0x00, 0x00, 0x00, 0x01, 0x65, 0xAA, 0xBB];
        assert_eq!(detect_nal_type(&data), 5);
    }

    #[test]
    fn detect_non_idr_nal() {
        // 3-byte start code + non-IDR slice (type 1)
        let data = [0x00, 0x00, 0x01, 0x41, 0xCC];
        assert_eq!(detect_nal_type(&data), 1);
    }

    #[test]
    fn detect_sps_then_idr() {
        // SPS (type 7) followed by IDR (type 5) — should return 5
        let data = [
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, // SPS NAL
            0x00, 0x00, 0x00, 0x01, 0x65, 0xAA, // IDR NAL
        ];
        assert_eq!(detect_nal_type(&data), 5);
    }

    #[test]
    fn detect_empty_data() {
        assert_eq!(detect_nal_type(&[]), 0);
    }
}
