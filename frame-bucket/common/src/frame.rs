/// The payload carried inside a frame — either a JPEG image or an H.264 access unit.
#[derive(Debug, Clone)]
pub enum FramePayload {
    /// Legacy JPEG frame (from MJPEG/polling producer).
    Jpeg(Vec<u8>),
    /// H.264 access unit in Annex B format (from H.264 TCP producer).
    H264 {
        data: Vec<u8>,
        /// NAL unit type of the primary slice: 5 = IDR (keyframe), 1 = non-IDR (P-frame).
        nal_type: u8,
    },
}

/// A camera frame with timestamp metadata.
///
/// Binary wire formats (for Kafka messages):
///
/// v1 (JPEG, backward-compatible):
///   [0..8]   captured_at_ms  (i64 big-endian, Unix millis)
///   [8..16]  seq             (u64 big-endian, sequence number)
///   [16..]   jpeg_data       (raw JPEG bytes)
///
/// v2 (H.264):
///   [0]      version = 0x02
///   [1]      nal_type
///   [2..10]  captured_at_ms  (i64 big-endian)
///   [10..18] seq             (u64 big-endian)
///   [18..22] h264_len        (u32 big-endian)
///   [22..22+h264_len] h264_data (Annex B access unit)
#[derive(Debug, Clone)]
pub struct TimestampedFrame {
    pub payload: FramePayload,
    pub captured_at_ms: i64,
    pub seq: u64,
}

const V1_HEADER_SIZE: usize = 16; // 8 bytes timestamp + 8 bytes seq
const V2_HEADER_SIZE: usize = 22; // 1 version + 1 nal_type + 8 ts + 8 seq + 4 h264_len
const V2_MARKER: u8 = 0x02;

impl TimestampedFrame {
    /// Create a new JPEG frame (used by MJPEG/polling producer).
    pub fn new(jpeg_data: Vec<u8>, captured_at_ms: i64, seq: u64) -> Self {
        Self {
            payload: FramePayload::Jpeg(jpeg_data),
            captured_at_ms,
            seq,
        }
    }

    /// Create a new H.264 frame (used by H.264 TCP producer).
    pub fn new_h264(h264_data: Vec<u8>, nal_type: u8, captured_at_ms: i64, seq: u64) -> Self {
        Self {
            payload: FramePayload::H264 {
                data: h264_data,
                nal_type,
            },
            captured_at_ms,
            seq,
        }
    }

    // -- Convenience accessors --------------------------------------------------

    /// Returns the JPEG data if this is a JPEG frame.
    pub fn jpeg_data(&self) -> Option<&[u8]> {
        match &self.payload {
            FramePayload::Jpeg(data) => Some(data),
            _ => None,
        }
    }

    /// Returns the H.264 access unit data if this is an H.264 frame.
    pub fn h264_data(&self) -> Option<&[u8]> {
        match &self.payload {
            FramePayload::H264 { data, .. } => Some(data),
            _ => None,
        }
    }

    /// Returns true if this frame is an H.264 IDR keyframe.
    pub fn is_keyframe(&self) -> bool {
        matches!(&self.payload, FramePayload::H264 { nal_type, .. } if *nal_type == 5)
    }

    /// Returns the size of the payload data in bytes.
    pub fn payload_size(&self) -> usize {
        match &self.payload {
            FramePayload::Jpeg(data) => data.len(),
            FramePayload::H264 { data, .. } => data.len(),
        }
    }

    /// Returns the raw payload bytes (JPEG or H.264) regardless of type.
    pub fn payload_bytes(&self) -> &[u8] {
        match &self.payload {
            FramePayload::Jpeg(data) => data,
            FramePayload::H264 { data, .. } => data,
        }
    }

    // -- Serialization ----------------------------------------------------------

    /// Serialize to binary format for Kafka payload.
    pub fn serialize(&self) -> Vec<u8> {
        match &self.payload {
            FramePayload::Jpeg(jpeg_data) => {
                // v1 format: [ts][seq][jpeg_bytes]
                let mut buf = Vec::with_capacity(V1_HEADER_SIZE + jpeg_data.len());
                buf.extend_from_slice(&self.captured_at_ms.to_be_bytes());
                buf.extend_from_slice(&self.seq.to_be_bytes());
                buf.extend_from_slice(jpeg_data);
                buf
            }
            FramePayload::H264 { data, nal_type } => {
                // v2 format: [0x02][nal_type][ts][seq][h264_len][h264_bytes]
                let mut buf = Vec::with_capacity(V2_HEADER_SIZE + data.len());
                buf.push(V2_MARKER);
                buf.push(*nal_type);
                buf.extend_from_slice(&self.captured_at_ms.to_be_bytes());
                buf.extend_from_slice(&self.seq.to_be_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
                buf
            }
        }
    }

    /// Deserialize from binary Kafka payload. Auto-detects v1 (JPEG) vs v2 (H.264).
    pub fn deserialize(data: &[u8]) -> Result<Self, FrameError> {
        if data.is_empty() {
            return Err(FrameError::TooShort {
                got: 0,
                expected: V1_HEADER_SIZE,
            });
        }

        if data[0] == V2_MARKER {
            // v2 format (H.264)
            if data.len() < V2_HEADER_SIZE {
                return Err(FrameError::TooShort {
                    got: data.len(),
                    expected: V2_HEADER_SIZE,
                });
            }
            let nal_type = data[1];
            let captured_at_ms = i64::from_be_bytes(data[2..10].try_into().unwrap());
            let seq = u64::from_be_bytes(data[10..18].try_into().unwrap());
            let h264_len = u32::from_be_bytes(data[18..22].try_into().unwrap()) as usize;
            if data.len() < V2_HEADER_SIZE + h264_len {
                return Err(FrameError::TooShort {
                    got: data.len(),
                    expected: V2_HEADER_SIZE + h264_len,
                });
            }
            let h264_data = data[V2_HEADER_SIZE..V2_HEADER_SIZE + h264_len].to_vec();
            Ok(Self {
                payload: FramePayload::H264 {
                    data: h264_data,
                    nal_type,
                },
                captured_at_ms,
                seq,
            })
        } else {
            // v1 format (JPEG)
            if data.len() < V1_HEADER_SIZE {
                return Err(FrameError::TooShort {
                    got: data.len(),
                    expected: V1_HEADER_SIZE,
                });
            }
            let captured_at_ms = i64::from_be_bytes(data[0..8].try_into().unwrap());
            let seq = u64::from_be_bytes(data[8..16].try_into().unwrap());
            let jpeg_data = data[V1_HEADER_SIZE..].to_vec();
            Ok(Self {
                payload: FramePayload::Jpeg(jpeg_data),
                captured_at_ms,
                seq,
            })
        }
    }

    /// Generate an object key for storage.
    pub fn object_key(&self, prefix: &str) -> String {
        let dt = chrono::DateTime::from_timestamp_millis(self.captured_at_ms)
            .unwrap_or_else(|| chrono::Utc::now());
        let date = dt.format("%Y-%m-%d");
        let ts = dt.format("%Y%m%dT%H%M%S%3fZ");
        let ext = match &self.payload {
            FramePayload::Jpeg(_) => "jpg",
            FramePayload::H264 { .. } => "h264",
        };
        format!("{prefix}{date}/{ts}_{seq:06}.{ext}", seq = self.seq)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame payload too short: got {got} bytes, expected at least {expected}")]
    TooShort { got: usize, expected: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_jpeg_v1() {
        let frame = TimestampedFrame::new(vec![0xFF, 0xD8, 0xFF, 0xE0], 1708300000000, 42);
        let bytes = frame.serialize();
        let decoded = TimestampedFrame::deserialize(&bytes).unwrap();
        assert_eq!(decoded.captured_at_ms, 1708300000000);
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.jpeg_data().unwrap(), &[0xFF, 0xD8, 0xFF, 0xE0]);
        assert!(decoded.h264_data().is_none());
    }

    #[test]
    fn roundtrip_h264_v2() {
        let h264 = vec![0x00, 0x00, 0x00, 0x01, 0x65, 0xAA, 0xBB]; // fake IDR NAL
        let frame = TimestampedFrame::new_h264(h264.clone(), 5, 1708300000000, 99);
        let bytes = frame.serialize();
        assert_eq!(bytes[0], V2_MARKER);
        let decoded = TimestampedFrame::deserialize(&bytes).unwrap();
        assert_eq!(decoded.captured_at_ms, 1708300000000);
        assert_eq!(decoded.seq, 99);
        assert_eq!(decoded.h264_data().unwrap(), &h264);
        assert!(decoded.is_keyframe());
        assert!(decoded.jpeg_data().is_none());
    }

    #[test]
    fn h264_p_frame_not_keyframe() {
        let frame = TimestampedFrame::new_h264(vec![0x00, 0x01], 1, 1000, 1);
        assert!(!frame.is_keyframe());
    }

    #[test]
    fn deserialize_too_short() {
        let result = TimestampedFrame::deserialize(&[0; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn object_key_jpeg() {
        let frame = TimestampedFrame::new(vec![], 1708300000000, 7);
        let key = frame.object_key("frames/");
        assert!(key.starts_with("frames/"));
        assert!(key.ends_with("_000007.jpg"));
    }

    #[test]
    fn object_key_h264() {
        let frame = TimestampedFrame::new_h264(vec![], 5, 1708300000000, 7);
        let key = frame.object_key("frames/");
        assert!(key.ends_with("_000007.h264"));
    }
}
