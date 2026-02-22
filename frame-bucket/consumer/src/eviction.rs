use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_types::region::Region;
use frame_bucket_common::config::{AwsS3Config, EvictionConfig};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::storage::RustfsStorage;

/// Monitors local RustFS storage usage and evicts oldest objects to AWS S3.
/// Falls back to delete-only mode when S3 is unreachable to prevent disk exhaustion.
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

    // Fallback threshold: when > 0 use it, otherwise fall back to normal threshold.
    let fallback_threshold_bytes = if eviction_config.fallback_threshold_gb > 0.0 {
        (eviction_config.fallback_threshold_gb * 1_073_741_824.0) as u64
    } else {
        threshold_bytes
    };
    // In fallback, apply the same buffer (threshold - target) below the fallback threshold.
    let fallback_target_bytes = fallback_threshold_bytes
        .saturating_sub(threshold_bytes.saturating_sub(target_bytes));

    // Fallback state
    let mut fallback_mode = false;
    let mut fallback_entered_at: Option<Instant> = None;

    // S3 health counters
    let mut s3_upload_successes: u64 = 0;
    let mut s3_upload_failures: u64 = 0;
    let mut last_successful_upload: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut objects_deleted_without_backup: u64 = 0;

    // Always scan the bucket on startup to get the true baseline.
    let (mut baseline_objects, mut baseline_bytes) = storage.bucket_stats().await;
    info!(
        objects = baseline_objects,
        total_gb = format!("{:.3}", baseline_bytes as f64 / 1_073_741_824.0),
        "scanned bucket for baseline storage stats"
    );
    write_health_file(
        &stats_path,
        baseline_objects,
        baseline_bytes,
        eviction_config.threshold_gb,
        eviction_config.fallback_threshold_gb,
        consecutive_failures,
        fallback_mode,
        s3_upload_successes,
        s3_upload_failures,
        last_successful_upload,
        objects_deleted_without_backup,
        false,
    );

    loop {
        tokio::time::sleep(interval).await;

        // If in fallback mode, periodically test whether S3 is back.
        if fallback_mode {
            if let Some(entered) = fallback_entered_at {
                if entered.elapsed()
                    >= Duration::from_secs(eviction_config.fallback_retry_secs)
                {
                    info!("fallback cooldown elapsed, attempting S3 test upload");
                    match test_s3_upload(&aws_s3_client, aws_config).await {
                        Ok(()) => {
                            info!("S3 test upload succeeded, exiting fallback mode");
                            fallback_mode = false;
                            fallback_entered_at = None;
                            consecutive_failures = 0;
                        }
                        Err(e) => {
                            warn!(error = %e, "S3 still unreachable, staying in fallback mode");
                            fallback_entered_at = Some(Instant::now());
                        }
                    }
                }
            }
        }

        // Current session objects (added since last restart).
        let (session_objects, session_bytes) = storage.stats().await;

        // Total = pre-existing baseline + new objects this session.
        let total_objects = baseline_objects + session_objects;
        let total_bytes = baseline_bytes + session_bytes;
        let total_gb = total_bytes as f64 / 1_073_741_824.0;
        let active_threshold = if fallback_mode {
            fallback_threshold_bytes
        } else {
            threshold_bytes
        };
        let is_over_threshold = total_bytes > active_threshold;

        debug!(
            baseline_objects,
            session_objects,
            total_objects,
            total_gb = format!("{:.3}", total_gb),
            threshold_gb = format!("{:.1}", eviction_config.threshold_gb),
            fallback_threshold_gb = format!("{:.1}", active_threshold as f64 / 1_073_741_824.0),
            fallback_mode,
            "storage check"
        );

        // Persist health state to disk every check.
        write_health_file(
            &stats_path,
            total_objects,
            total_bytes,
            eviction_config.threshold_gb,
            eviction_config.fallback_threshold_gb,
            consecutive_failures,
            fallback_mode,
            s3_upload_successes,
            s3_upload_failures,
            last_successful_upload,
            objects_deleted_without_backup,
            is_over_threshold,
        );

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
                    fallback_mode,
                    s3_upload_successes,
                    s3_upload_failures,
                    "storage stats"
                );
            }
        }

        if is_over_threshold {
            if fallback_mode {
                // ── FALLBACK: delete locally without S3 backup ──
                warn!(
                    total_gb = format!("{:.3}", total_gb),
                    fallback_threshold_gb = format!("{:.1}", fallback_threshold_bytes as f64 / 1_073_741_824.0),
                    fallback_target_gb = format!("{:.1}", fallback_target_bytes as f64 / 1_073_741_824.0),
                    "storage exceeds fallback threshold, evicting in FALLBACK (delete-only) mode"
                );

                match fallback_evict_batch(
                    &storage,
                    eviction_config,
                    fallback_target_bytes,
                    &mut baseline_bytes,
                    &mut baseline_objects,
                    &mut objects_deleted_without_backup,
                )
                .await
                {
                    Ok(count) => {
                        let (so, sb) = storage.stats().await;
                        let new_total_objects = baseline_objects + so;
                        let new_total_bytes = baseline_bytes + sb;
                        warn!(
                            evicted = count,
                            remaining_objects = new_total_objects,
                            remaining_gb =
                                format!("{:.3}", new_total_bytes as f64 / 1_073_741_824.0),
                            "fallback eviction complete (data NOT backed up to S3)"
                        );
                        write_health_file(
                            &stats_path,
                            new_total_objects,
                            new_total_bytes,
                            eviction_config.threshold_gb,
                            eviction_config.fallback_threshold_gb,
                            consecutive_failures,
                            fallback_mode,
                            s3_upload_successes,
                            s3_upload_failures,
                            last_successful_upload,
                            objects_deleted_without_backup,
                            new_total_bytes > fallback_threshold_bytes,
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "fallback eviction batch failed");
                    }
                }
            } else {
                // ── NORMAL: upload to S3 then delete locally ──
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
                        s3_upload_successes += count as u64;
                        if count > 0 {
                            last_successful_upload = Some(chrono::Utc::now());
                        }
                        consecutive_failures = 0;

                        let (so, sb) = storage.stats().await;
                        let new_total_objects = baseline_objects + so;
                        let new_total_bytes = baseline_bytes + sb;
                        info!(
                            evicted = count,
                            remaining_objects = new_total_objects,
                            remaining_gb =
                                format!("{:.3}", new_total_bytes as f64 / 1_073_741_824.0),
                            "eviction batch complete"
                        );
                        write_health_file(
                            &stats_path,
                            new_total_objects,
                            new_total_bytes,
                            eviction_config.threshold_gb,
                            eviction_config.fallback_threshold_gb,
                            consecutive_failures,
                            fallback_mode,
                            s3_upload_successes,
                            s3_upload_failures,
                            last_successful_upload,
                            objects_deleted_without_backup,
                            new_total_bytes > threshold_bytes,
                        );
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        s3_upload_failures += 1;
                        error!(error = %e, consecutive_failures, "eviction batch failed");

                        if consecutive_failures >= eviction_config.fallback_after_failures {
                            warn!(
                                consecutive_failures,
                                threshold = eviction_config.fallback_after_failures,
                                "entering FALLBACK DELETE-ONLY mode: S3 uploads \
                                 failing, will delete locally to prevent disk exhaustion"
                            );
                            fallback_mode = true;
                            fallback_entered_at = Some(Instant::now());
                        } else if consecutive_failures >= 3 {
                            warn!(
                                consecutive_failures,
                                "S3 upload failures, backing off 5 minutes"
                            );
                            tokio::time::sleep(Duration::from_secs(300)).await;
                        }
                    }
                }
            }
        }
    }
}

/// Write the extended health/stats JSON file to disk.
fn write_health_file(
    path: &Path,
    objects: usize,
    total_bytes: u64,
    threshold_gb: f64,
    fallback_threshold_gb: f64,
    consecutive_failures: u32,
    fallback_mode: bool,
    s3_upload_successes: u64,
    s3_upload_failures: u64,
    last_successful_upload: Option<chrono::DateTime<chrono::Utc>>,
    objects_deleted_without_backup: u64,
    is_evicting: bool,
) {
    let total_mb = total_bytes as f64 / 1_048_576.0;
    let total_gb = total_bytes as f64 / 1_073_741_824.0;
    let threshold_bytes = threshold_gb * 1_073_741_824.0;
    let usage_pct = if threshold_bytes > 0.0 {
        (total_bytes as f64 / threshold_bytes) * 100.0
    } else {
        0.0
    };

    // Derive status strings
    let eviction_state = if fallback_mode {
        "fallback"
    } else if is_evicting {
        "evicting"
    } else {
        "idle"
    };

    let s3_status = if fallback_mode {
        "unavailable"
    } else if consecutive_failures >= 3 {
        "degraded"
    } else {
        "healthy"
    };

    let rustfs_status = if usage_pct > 100.0 {
        "critical"
    } else if usage_pct > 80.0 {
        "pressure"
    } else {
        "healthy"
    };

    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let active_threshold_gb = if fallback_mode && fallback_threshold_gb > 0.0 {
        fallback_threshold_gb
    } else {
        threshold_gb
    };

    let json = serde_json::json!({
        "storage": {
            "objects": objects,
            "total_bytes": total_bytes,
            "total_mb": (total_mb * 10.0).round() / 10.0,
            "total_gb": (total_gb * 1000.0).round() / 1000.0,
            "threshold_gb": threshold_gb,
            "fallback_threshold_gb": if fallback_threshold_gb > 0.0 { fallback_threshold_gb } else { threshold_gb },
            "active_threshold_gb": active_threshold_gb,
            "usage_pct": (usage_pct * 10.0).round() / 10.0
        },
        "eviction": {
            "state": eviction_state,
            "fallback_mode": fallback_mode,
            "consecutive_failures": consecutive_failures,
            "objects_deleted_without_backup": objects_deleted_without_backup
        },
        "s3": {
            "upload_successes": s3_upload_successes,
            "upload_failures": s3_upload_failures,
            "last_successful_upload": last_successful_upload.map(|t|
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            "status": s3_status
        },
        "rustfs": {
            "status": rustfs_status
        },
        "updated_at": now
    });

    if let Err(e) = std::fs::write(path, json.to_string()) {
        warn!(error = %e, path = %path.display(), "failed to write health file");
    }
}

/// Evict a batch of objects: upload to S3, then delete from RustFS.
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

/// Fallback eviction: delete from RustFS without uploading to S3.
/// Used when S3 is unreachable to prevent local disk exhaustion.
async fn fallback_evict_batch(
    storage: &RustfsStorage,
    eviction_config: &EvictionConfig,
    target_bytes: u64,
    baseline_bytes: &mut u64,
    baseline_objects: &mut usize,
    objects_deleted_without_backup: &mut u64,
) -> Result<usize, EvictionError> {
    let entries = storage
        .list_oldest_from_bucket(eviction_config.batch_size)
        .await;

    if entries.is_empty() {
        debug!("no objects to evict in fallback mode");
        return Ok(0);
    }

    let mut evicted = 0;

    for (key, size, ts) in &entries {
        let in_session_index = {
            let idx = storage.index.lock().await;
            idx.contains_key(ts)
        };

        warn!(key, size, "FALLBACK: deleting from RustFS WITHOUT S3 backup");

        if let Err(e) = storage.delete_object(key, *ts).await {
            warn!(error = %e, key, "failed to delete from RustFS in fallback mode");
            continue;
        }

        if !in_session_index {
            *baseline_bytes = baseline_bytes.saturating_sub(*size);
            *baseline_objects = baseline_objects.saturating_sub(1);
        }

        *objects_deleted_without_backup += 1;
        evicted += 1;

        // Check if we've brought usage below target
        let (_, session_bytes) = storage.stats().await;
        let current_total = *baseline_bytes + session_bytes;
        if current_total < target_bytes {
            info!(
                current_gb = format!("{:.3}", current_total as f64 / 1_073_741_824.0),
                target_gb = format!("{:.1}", target_bytes as f64 / 1_073_741_824.0),
                "fallback eviction: below target, stopping"
            );
            break;
        }
    }

    Ok(evicted)
}

/// Upload a tiny test object to verify S3 connectivity, then clean it up.
async fn test_s3_upload(
    client: &aws_sdk_s3::Client,
    config: &AwsS3Config,
) -> Result<(), EvictionError> {
    let test_key = format!("{}__health_check", config.prefix);
    client
        .put_object()
        .bucket(&config.bucket)
        .key(&test_key)
        .content_type("text/plain")
        .body(ByteStream::from(b"ok".to_vec()))
        .send()
        .await
        .map_err(|e| EvictionError::Upload(e.to_string()))?;

    // Clean up the test object (best-effort)
    let _ = client
        .delete_object()
        .bucket(&config.bucket)
        .key(&test_key)
        .send()
        .await;

    Ok(())
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
