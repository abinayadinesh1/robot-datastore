use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use frame_bucket_common::config::{AwsS3Config, EvictionConfig};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::Disks;
use tracing::{debug, error, info, warn};

use crate::storage::RustfsStorage;

/// Monitors disk usage and evicts oldest frames from RustFS to AWS S3.
pub async fn run_eviction_loop(
    storage: Arc<RustfsStorage>,
    eviction_config: &EvictionConfig,
    aws_config: &AwsS3Config,
) {
    let aws_s3_client = create_aws_s3_client(aws_config).await;
    let interval = Duration::from_secs(eviction_config.check_interval_secs);
    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::time::sleep(interval).await;

        let usage = disk_usage_percent();
        debug!(usage_percent = format!("{:.1}", usage), "disk check");

        if usage > eviction_config.threshold_percent {
            info!(
                usage_percent = format!("{:.1}", usage),
                threshold = format!("{:.1}", eviction_config.threshold_percent),
                "disk usage exceeds threshold, starting eviction"
            );

            match evict_batch(
                &storage,
                &aws_s3_client,
                aws_config,
                eviction_config,
            )
            .await
            {
                Ok(count) => {
                    info!(evicted = count, "eviction batch complete");
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

async fn evict_batch(
    storage: &RustfsStorage,
    aws_client: &aws_sdk_s3::Client,
    aws_config: &AwsS3Config,
    eviction_config: &EvictionConfig,
) -> Result<usize, EvictionError> {
    let entries = storage.oldest_n(eviction_config.batch_size).await;

    if entries.is_empty() {
        debug!("no frames to evict");
        return Ok(0);
    }

    let mut evicted = 0;

    for (captured_at_ms, entry) in &entries {
        // Download from RustFS
        let data = storage
            .get_object(&entry.key)
            .await
            .map_err(|e| EvictionError::Download(e.to_string()))?;

        // Upload to AWS S3
        let aws_key = format!(
            "{}{}/{}",
            aws_config.prefix, aws_config.robot_id, entry.key
        );

        let put_result = aws_client
            .put_object()
            .bucket(&aws_config.bucket)
            .key(&aws_key)
            .content_type("image/jpeg")
            .body(ByteStream::from(data))
            .send()
            .await;

        match put_result {
            Ok(resp) => {
                let etag = resp.e_tag().unwrap_or("none");
                debug!(
                    aws_key,
                    etag,
                    "uploaded to AWS S3, deleting from RustFS"
                );

                // Only delete after confirmed upload
                if let Err(e) = storage.delete_object(&entry.key, *captured_at_ms).await {
                    warn!(error = %e, key = entry.key, "failed to delete from RustFS after S3 upload");
                }
                evicted += 1;
            }
            Err(e) => {
                error!(
                    error = %e,
                    key = entry.key,
                    "failed to upload to AWS S3, keeping in RustFS"
                );
                return Err(EvictionError::Upload(e.to_string()));
            }
        }

        // Check if we've brought usage below target
        let usage = disk_usage_percent();
        if usage < eviction_config.target_percent {
            info!(
                usage_percent = format!("{:.1}", usage),
                target = format!("{:.1}", eviction_config.target_percent),
                "disk usage below target, stopping eviction"
            );
            break;
        }
    }

    Ok(evicted)
}

async fn create_aws_s3_client(config: &AwsS3Config) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.region.clone()))
        .load()
        .await;
    aws_sdk_s3::Client::new(&sdk_config)
}

/// Get current disk usage as a percentage (0-100).
fn disk_usage_percent() -> f64 {
    let disks = Disks::new_with_refreshed_list();

    // Find the root filesystem or the disk with the most total space
    let disk = disks
        .iter()
        .max_by_key(|d| d.total_space());

    match disk {
        Some(d) => {
            let total = d.total_space() as f64;
            let available = d.available_space() as f64;
            if total > 0.0 {
                ((total - available) / total) * 100.0
            } else {
                0.0
            }
        }
        None => {
            warn!("no disks found, reporting 0% usage");
            0.0
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EvictionError {
    #[error("failed to download from RustFS: {0}")]
    Download(String),
    #[error("failed to upload to AWS S3: {0}")]
    Upload(String),
}
