/// A camera frame with timestamp metadata.
///
/// Binary wire format (for Kafka messages):
///   [0..8]   captured_at_ms  (i64 big-endian, Unix millis)
///   [8..16]  seq             (u64 big-endian, sequence number)
///   [16..]   jpeg_data       (raw JPEG bytes)
#[derive(Debug, Clone)]
pub struct TimestampedFrame {
    pub jpeg_data: Vec<u8>,
    pub captured_at_ms: i64,
    pub seq: u64,
}

const HEADER_SIZE: usize = 16; // 8 bytes timestamp + 8 bytes seq

impl TimestampedFrame {
    pub fn new(jpeg_data: Vec<u8>, captured_at_ms: i64, seq: u64) -> Self {
        Self {
            jpeg_data,
            captured_at_ms,
            seq,
        }
    }

    /// Serialize to binary format for Kafka payload.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.jpeg_data.len());
        buf.extend_from_slice(&self.captured_at_ms.to_be_bytes());
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&self.jpeg_data);
        buf
    }

    /// Deserialize from binary Kafka payload.
    pub fn deserialize(data: &[u8]) -> Result<Self, FrameError> {
        if data.len() < HEADER_SIZE {
            return Err(FrameError::TooShort {
                got: data.len(),
                expected: HEADER_SIZE,
            });
        }
        let captured_at_ms = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let seq = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let jpeg_data = data[HEADER_SIZE..].to_vec();
        Ok(Self {
            jpeg_data,
            captured_at_ms,
            seq,
        })
    }

    /// Generate an object key for storage.
    /// Format: `{prefix}{date}/{timestamp}_{seq:06}.jpg`
    pub fn object_key(&self, prefix: &str) -> String {
        let dt = chrono::DateTime::from_timestamp_millis(self.captured_at_ms)
            .unwrap_or_else(|| chrono::Utc::now());
        let date = dt.format("%Y-%m-%d");
        let ts = dt.format("%Y%m%dT%H%M%S%3fZ");
        format!("{prefix}{date}/{ts}_{seq:06}.jpg", seq = self.seq)
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
    fn roundtrip_serialization() {
        let frame = TimestampedFrame::new(vec![0xFF, 0xD8, 0xFF, 0xE0], 1708300000000, 42);
        let bytes = frame.serialize();
        let decoded = TimestampedFrame::deserialize(&bytes).unwrap();
        assert_eq!(decoded.captured_at_ms, 1708300000000);
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.jpeg_data, vec![0xFF, 0xD8, 0xFF, 0xE0]);
    }

    #[test]
    fn deserialize_too_short() {
        let result = TimestampedFrame::deserialize(&[0; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn object_key_format() {
        let frame = TimestampedFrame::new(vec![], 1708300000000, 7);
        let key = frame.object_key("frames/");
        assert!(key.starts_with("frames/"));
        assert!(key.ends_with("_000007.jpg"));
    }
}
