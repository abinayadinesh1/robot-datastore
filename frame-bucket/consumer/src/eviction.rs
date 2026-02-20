use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_types::region::Region;
use frame_bucket_common::config::{AwsS3Config, EvictionConfig};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::storage::RustfsStorage;

/// Monitors local RustFS storage usage and evicts oldest objects to AWS S3.
pub async fn run_eviction_loop(
    storage: Arc<RustfsStorage>,
    eviction_config: &EvictionConfig,
    aws_config: &AwsS3Config,
) {
    let aws_s3_client = create_aws_s3_client(aws_config).await;
    ensure_aws_bucket(&aws_s3_client, aws_config).await;
    let interval = Duration::from_secs(eviction_config.check_interval_secs);
    let mut consecutive_failures: u32 = 0;
    let threshold_bytes = (eviction_config.threshold_gb * 1_073_741_824.0) as u64;
    let target_bytes = (eviction_config.target_gb * 1_073_741_824.0) as u64;

    loop {
        tokio::time::sleep(interval).await;

        let (object_count, total_bytes) = storage.stats().await;
        let total_gb = total_bytes as f64 / 1_073_741_824.0;

        debug!(
            objects = object_count,
            total_gb = format!("{:.3}", total_gb),
            threshold_gb = format!("{:.1}", eviction_config.threshold_gb),
            "storage check"
        );

        // Log stats at info level every 10 intervals (~5 min at 30s interval)
        // to give visibility without spamming.
        {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count % 10 == 0 {
                info!(
                    objects = object_count,
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
                objects = object_count,
                "storage exceeds threshold, starting eviction"
            );

            match evict_batch(
                &storage,
                &aws_s3_client,
                aws_config,
                eviction_config,
                target_bytes,
            )
            .await
            {
                Ok(count) => {
                    let (new_objects, new_bytes) = storage.stats().await;
                    info!(
                        evicted = count,
                        remaining_objects = new_objects,
                        remaining_gb = format!("{:.3}", new_bytes as f64 / 1_073_741_824.0),
                        "eviction batch complete"
                    );
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
    target_bytes: u64,
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

        // Upload to AWS S3 — mirror RustFS path under the archive prefix
        let aws_key = format!("{}{}", aws_config.prefix, entry.key);

        let content_type = if entry.key.ends_with(".mp4") {
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
        let (_, current_bytes) = storage.stats().await;
        if current_bytes < target_bytes {
            info!(
                current_gb = format!("{:.3}", current_bytes as f64 / 1_073_741_824.0),
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
        Ok(_) => info!(bucket = config.bucket, region = config.region, "AWS S3 bucket created"),
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
