use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_types::region::Region;
use frame_bucket_common::config::{AwsS3Config, EvictionConfig};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::storage::RustfsStorage;

/// Monitors local RustFS storage usage and evicts oldest objects to AWS S3.
pub async fn run_eviction_loop(
    storage: Arc<RustfsStorage>,
    eviction_config: &EvictionConfig,
    aws_config: &AwsS3Config,
    stats_path: PathBuf,
) {
    let aws_s3_client = create_aws_s3_client(aws_config).await;
    ensure_aws_bucket(&aws_s3_client, aws_config).await;
    let interval = Duration::from_secs(eviction_config.check_interval_secs);
    let mut consecutive_failures: u32 = 0;
    let threshold_bytes = (eviction_config.threshold_gb * 1_073_741_824.0) as u64;
    let target_bytes = (eviction_config.target_gb * 1_073_741_824.0) as u64;

    // Read persisted stats from disk to account for pre-existing data from before this restart.
    let mut baseline_bytes: u64 = 0;
    let mut baseline_objects: usize = 0;
    if let Some((objects, total_bytes)) = read_stats_file(&stats_path) {
        if objects > 0 {
            baseline_objects = objects;
            baseline_bytes = total_bytes;
            info!(
                objects = baseline_objects,
                total_gb = format!("{:.3}", baseline_bytes as f64 / 1_073_741_824.0),
                "loaded persisted storage stats from disk"
            );
        }
    }

    // If the stats file was missing, empty, or stale (0 objects), scan the bucket to bootstrap.
    if baseline_objects == 0 {
        info!("stats file missing or empty, scanning bucket to bootstrap");
        let (count, bytes) = storage.bucket_stats().await;
        if count > 0 {
            baseline_objects = count;
            baseline_bytes = bytes;
            info!(
                objects = baseline_objects,
                total_gb = format!("{:.3}", baseline_bytes as f64 / 1_073_741_824.0),
                "bootstrapped storage stats from bucket scan"
            );
            // Persist so next restart doesn't need to scan again.
            write_stats_file(&stats_path, baseline_objects, baseline_bytes);
        }
    }

    loop {
        tokio::time::sleep(interval).await;

        // Current session objects (added since last restart).
        let (session_objects, session_bytes) = storage.stats().await;

        // Total = pre-existing baseline + new objects this session.
        let total_objects = baseline_objects + session_objects;
        let total_bytes = baseline_bytes + session_bytes;
        let total_gb = total_bytes as f64 / 1_073_741_824.0;

        debug!(
            baseline_objects,
            session_objects,
            total_objects,
            total_gb = format!("{:.3}", total_gb),
            threshold_gb = format!("{:.1}", eviction_config.threshold_gb),
            "storage check"
        );

        // Persist combined stats to disk every check.
        write_stats_file(&stats_path, total_objects, total_bytes);

        // Log stats at info level every 10 intervals (~5 min at 30s interval).
        {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count % 10 == 0 {
                info!(
                    objects = total_objects,
                    total_mb = format!("{:.1}", total_bytes as f64 / 1_048_576.0),
                    total_gb = format!("{:.3}", total_gb),
                    threshold_gb = format!("{:.1}", eviction_config.threshold_gb),
                    "storage stats"
                );
            }
        }

        if total_bytes > threshold_bytes {
            info!(
                total_gb = format!("{:.3}", total_gb),
                threshold_gb = format!("{:.1}", eviction_config.threshold_gb),
                objects = total_objects,
                "storage exceeds threshold, starting eviction"
            );

            match evict_batch(
                &storage,
                &aws_s3_client,
                aws_config,
                eviction_config,
                target_bytes,
                &mut baseline_bytes,
                &mut baseline_objects,
            )
            .await
            {
                Ok(count) => {
                    let (session_objects, session_bytes) = storage.stats().await;
                    let new_total_objects = baseline_objects + session_objects;
                    let new_total_bytes = baseline_bytes + session_bytes;
                    info!(
                        evicted = count,
                        remaining_objects = new_total_objects,
                        remaining_gb =
                            format!("{:.3}", new_total_bytes as f64 / 1_073_741_824.0),
                        "eviction batch complete"
                    );
                    write_stats_file(&stats_path, new_total_objects, new_total_bytes);
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    error!(error = %e, consecutive_failures, "eviction batch failed");
                    if consecutive_failures >= 3 {
                        warn!("3 consecutive eviction failures, backing off 5 minutes");
                        tokio::time::sleep(Duration::from_secs(300)).await;
                        consecutive_failures = 0;
                    }
                }
            }
        }
    }
}

/// Read the persistent stats file. Returns (objects, total_bytes) or None.
fn read_stats_file(path: &Path) -> Option<(usize, u64)> {
    let content = std::fs::read_to_string(path).ok()?;
    let objects = parse_json_u64(&content, "objects")? as usize;
    let total_bytes = parse_json_u64(&content, "total_bytes")?;
    Some((objects, total_bytes))
}

/// Extract a u64 value for a given key from a flat JSON object.
fn parse_json_u64(json: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{}\":", key);
    let pos = json.find(&needle)? + needle.len();
    let rest = json[pos..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

/// Write a simple JSON stats file to disk.
fn write_stats_file(path: &Path, objects: usize, total_bytes: u64) {
    let total_mb = total_bytes as f64 / 1_048_576.0;
    let total_gb = total_bytes as f64 / 1_073_741_824.0;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let json = format!(
        r#"{{"objects":{objects},"total_bytes":{total_bytes},"total_mb":{total_mb:.1},"total_gb":{total_gb:.3},"updated_at":"{now}"}}"#
    );
    if let Err(e) = std::fs::write(path, json) {
        warn!(error = %e, path = %path.display(), "failed to write stats file");
    }
}

async fn evict_batch(
    storage: &RustfsStorage,
    aws_client: &aws_sdk_s3::Client,
    aws_config: &AwsS3Config,
    eviction_config: &EvictionConfig,
    target_bytes: u64,
    baseline_bytes: &mut u64,
    baseline_objects: &mut usize,
) -> Result<usize, EvictionError> {
    // Always list from the bucket to find the truly oldest objects,
    // regardless of whether they were added this session or before a restart.
    let entries = storage
        .list_oldest_from_bucket(eviction_config.batch_size)
        .await;

    if entries.is_empty() {
        debug!("no objects to evict");
        return Ok(0);
    }

    let mut evicted = 0;

    for (key, size, ts) in &entries {
        // Check if this object exists in the current session's in-memory index.
        let in_session_index = {
            let idx = storage.index.lock().await;
            idx.contains_key(ts)
        };

        // Download from RustFS
        let data = storage
            .get_object(key)
            .await
            .map_err(|e| EvictionError::Download(e.to_string()))?;

        // Upload to AWS S3 — mirror RustFS path under the archive prefix
        let aws_key = format!("{}{}", aws_config.prefix, key);

        let content_type = if key.ends_with(".mp4") {
            "video/mp4"
        } else {
            "image/jpeg"
        };

        let put_result = aws_client
            .put_object()
            .bucket(&aws_config.bucket)
            .key(&aws_key)
            .content_type(content_type)
            .body(ByteStream::from(data))
            .send()
            .await;

        match put_result {
            Ok(resp) => {
                let etag = resp.e_tag().unwrap_or("none");
                debug!(aws_key, etag, "uploaded to AWS S3, deleting from RustFS");

                // Delete from RustFS (also removes from in-memory index if present)
                if let Err(e) = storage.delete_object(key, *ts).await {
                    warn!(error = %e, key, "failed to delete from RustFS after S3 upload");
                }

                // Only adjust baseline for pre-existing objects not in the session index.
                // Session objects are already tracked by the index and removed by delete_object.
                if !in_session_index {
                    *baseline_bytes = baseline_bytes.saturating_sub(*size);
                    *baseline_objects = baseline_objects.saturating_sub(1);
                }

                evicted += 1;
            }
            Err(e) => {
                error!(
                    error = %e,
                    key,
                    "failed to upload to AWS S3, keeping in RustFS"
                );
                return Err(EvictionError::Upload(e.to_string()));
            }
        }

        // Check if we've brought usage below target
        let (_, session_bytes) = storage.stats().await;
        let current_total = *baseline_bytes + session_bytes;
        if current_total < target_bytes {
            info!(
                current_gb = format!("{:.3}", current_total as f64 / 1_073_741_824.0),
                target_gb = format!("{:.1}", target_bytes as f64 / 1_073_741_824.0),
                "storage below target, stopping eviction"
            );
            break;
        }
    }

    Ok(evicted)
}

/// Ensure the AWS S3 archive bucket exists, creating it if not. Runs once at startup.
async fn ensure_aws_bucket(client: &aws_sdk_s3::Client, config: &AwsS3Config) {
    match client.head_bucket().bucket(&config.bucket).send().await {
        Ok(_) => {
            info!(bucket = config.bucket, "AWS S3 bucket exists");
            return;
        }
        Err(_) => {
            info!(bucket = config.bucket, "AWS S3 bucket not found, creating");
        }
    }

    // us-east-1 must NOT include a location constraint; all other regions require one.
    let result = if config.region == "us-east-1" {
        client.create_bucket().bucket(&config.bucket).send().await
    } else {
        let constraint = BucketLocationConstraint::from(config.region.as_str());
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        client
            .create_bucket()
            .bucket(&config.bucket)
            .create_bucket_configuration(cfg)
            .send()
            .await
    };

    match result {
        Ok(_) => info!(
            bucket = config.bucket,
            region = config.region,
            "AWS S3 bucket created"
        ),
        Err(e) => warn!(
            error = %e,
            bucket = config.bucket,
            "failed to create AWS S3 bucket; eviction uploads will fail until it exists"
        ),
    }
}

async fn create_aws_s3_client(config: &AwsS3Config) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .load()
        .await;
    aws_sdk_s3::Client::new(&sdk_config)
}

#[derive(Debug, thiserror::Error)]
pub enum EvictionError {
    #[error("failed to download from RustFS: {0}")]
    Download(String),
    #[error("failed to upload to AWS S3: {0}")]
    Upload(String),
}
