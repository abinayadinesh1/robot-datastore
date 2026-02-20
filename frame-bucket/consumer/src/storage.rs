use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use chrono::NaiveDateTime;
use frame_bucket_common::config::RustfsConfig;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Tracks stored objects for ring-buffer eviction ordering.
#[derive(Debug)]
pub struct ObjectEntry {
    #[allow(dead_code)]
    pub key: String,
    pub size_bytes: u64,
}

/// RustFS-backed object storage with an in-memory index for ring-buffer eviction.
pub struct RustfsStorage {
    client: aws_sdk_s3::Client,
    bucket: String,
    #[allow(dead_code)]
    prefix: String,
    /// Ordered map: captured_at_ms -> stored object metadata.
    pub index: Arc<Mutex<BTreeMap<i64, ObjectEntry>>>,
}

impl RustfsStorage {
    pub async fn new(config: &RustfsConfig) -> Self {
        let creds = Credentials::new(
            &config.access_key,
            &config.secret_key,
            None,
            None,
            "static",
        );

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(&config.endpoint)
            .credentials_provider(creds)
            .region(Region::new("us-east-1"))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);

        Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            index: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Ensure the bucket exists, creating it if necessary.
    pub async fn ensure_bucket(&self) -> Result<(), StorageError> {
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {
                info!(bucket = self.bucket, "bucket exists");
                Ok(())
            }
            Err(_) => {
                info!(bucket = self.bucket, "creating bucket");
                self.client
                    .create_bucket()
                    .bucket(&self.bucket)
                    .send()
                    .await
                    .map_err(|e| StorageError::CreateBucket(e.to_string()))?;
                info!(bucket = self.bucket, "bucket created");
                Ok(())
            }
        }
    }

    /// Store a frame in RustFS. Kept for backward compatibility.
    #[allow(dead_code)]
    pub async fn put_frame(
        &self,
        object_key: &str,
        jpeg_data: Vec<u8>,
        captured_at_ms: i64,
    ) -> Result<(), StorageError> {
        let size = jpeg_data.len() as u64;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(object_key)
            .content_type("image/jpeg")
            .body(ByteStream::from(jpeg_data))
            .send()
            .await
            .map_err(|e| StorageError::PutObject(e.to_string()))?;

        debug!(key = object_key, size, "stored frame in RustFS");

        self.index.lock().await.insert(
            captured_at_ms,
            ObjectEntry {
                key: object_key.to_string(),
                size_bytes: size,
            },
        );

        Ok(())
    }

    /// Get the oldest N entries from the in-memory index.
    #[allow(dead_code)]
    pub async fn oldest_n(&self, n: usize) -> Vec<(i64, ObjectEntry)> {
        let idx = self.index.lock().await;
        idx.iter()
            .take(n)
            .map(|(&ts, entry)| {
                (
                    ts,
                    ObjectEntry {
                        key: entry.key.clone(),
                        size_bytes: entry.size_bytes,
                    },
                )
            })
            .collect()
    }

    /// Download an object's bytes from RustFS.
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::GetObject(e.to_string()))?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::GetObject(e.to_string()))?;

        Ok(data.into_bytes().to_vec())
    }

    /// Delete an object from RustFS and remove from the index.
    pub async fn delete_object(&self, key: &str, captured_at_ms: i64) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StorageError::DeleteObject(e.to_string()))?;

        self.index.lock().await.remove(&captured_at_ms);
        debug!(key, "deleted from RustFS");
        Ok(())
    }

    /// Store the representative JPEG for an idle period. Indexed for eviction.
    pub async fn put_idle_frame(
        &self,
        object_key: &str,
        jpeg_data: Vec<u8>,
        start_ms: i64,
    ) -> Result<(), StorageError> {
        let size = jpeg_data.len() as u64;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(object_key)
            .content_type("image/jpeg")
            .body(ByteStream::from(jpeg_data))
            .send()
            .await
            .map_err(|e| StorageError::PutObject(e.to_string()))?;

        debug!(key = object_key, size, "stored idle frame in RustFS");

        self.index.lock().await.insert(
            start_ms,
            ObjectEntry {
                key: object_key.to_string(),
                size_bytes: size,
            },
        );

        Ok(())
    }

    /// Store a completed MP4 video segment. Indexed for eviction.
    pub async fn put_segment(
        &self,
        object_key: &str,
        mp4_data: Vec<u8>,
        start_ms: i64,
    ) -> Result<(), StorageError> {
        let size = mp4_data.len() as u64;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(object_key)
            .content_type("video/mp4")
            .body(ByteStream::from(mp4_data))
            .send()
            .await
            .map_err(|e| StorageError::PutObject(e.to_string()))?;

        debug!(key = object_key, size, "stored segment in RustFS");

        self.index.lock().await.insert(
            start_ms,
            ObjectEntry {
                key: object_key.to_string(),
                size_bytes: size,
            },
        );

        Ok(())
    }

    /// Returns (object_count, total_bytes) from the in-memory index.
    pub async fn stats(&self) -> (usize, u64) {
        let idx = self.index.lock().await;
        let count = idx.len();
        let bytes: u64 = idx.values().map(|e| e.size_bytes).sum();
        (count, bytes)
    }

    /// List the N oldest objects directly from the bucket (by lexicographic key order).
    /// Used for eviction when the in-memory index may not have pre-existing objects.
    pub async fn list_oldest_from_bucket(&self, n: usize) -> Vec<(String, u64, i64)> {
        let mut result = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .max_keys(n as i32);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "failed to list objects from bucket for eviction");
                    return result;
                }
            };

            for obj in resp.contents() {
                if result.len() >= n {
                    return result;
                }
                if let Some(key) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    let ts = parse_start_ms_from_key(key).unwrap_or(0);
                    result.push((key.to_string(), size, ts));
                }
            }

            if resp.is_truncated() == Some(true) && result.len() < n {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        result
    }

    #[allow(dead_code)]
    pub fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }

    #[allow(dead_code)]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

/// Parse the start timestamp (in ms) from an object key.
/// Keys look like: `robot/camera/2026-02-18/20260218T093000000Z_20260218T094000000Z.jpg`
fn parse_start_ms_from_key(key: &str) -> Option<i64> {
    let filename = key.rsplit('/').next()?;
    let start_part = filename.split('_').next()?;
    let clean = start_part.trim_end_matches('Z');
    if clean.len() < 18 {
        return None;
    }
    let dt = NaiveDateTime::parse_from_str(clean, "%Y%m%dT%H%M%S%3f").ok()?;
    Some(dt.and_utc().timestamp_millis())
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("failed to create bucket: {0}")]
    CreateBucket(String),
    #[error("failed to put object: {0}")]
    PutObject(String),
    #[error("failed to get object: {0}")]
    GetObject(String),
    #[error("failed to delete object: {0}")]
    DeleteObject(String),
}
