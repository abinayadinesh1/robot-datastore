use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use tracing::{error, warn};

use crate::AppState;

// ---------------------------------------------------------------------------
// Browse types (local to health)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct BrowseObject {
    key: String,
    size_bytes: i64,
    last_modified: String,
}

#[derive(Debug, Serialize)]
struct BrowseContainer {
    name: String,
    prefix: String,
    date_count: usize,
    recent_dates: Vec<String>,
    recent_objects: Vec<BrowseObject>,
}

#[derive(Debug, Serialize)]
struct BrowseResponse {
    bucket: String,
    containers: Vec<BrowseContainer>,
    total_objects: i64,
    total_size_bytes: i64,
    is_live: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /health — returns the pipeline's health state JSON, enriched with host disk stats.
pub async fn get_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let path = state.health_file_path.clone();
    match tokio::task::spawn_blocking(move || {
        let contents = std::fs::read_to_string(&path)?;
        let mut json: serde_json::Value =
            serde_json::from_str(&contents).unwrap_or(serde_json::Value::Null);

        if let Some(disk) = get_disk_usage(&path) {
            json["disk"] = disk;
        }

        Ok::<serde_json::Value, std::io::Error>(json)
    })
    .await
    {
        Ok(Ok(json)) => (StatusCode::OK, Json(json)).into_response(),
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "health data not available",
                "detail": "consumer has not written health state yet"
            })),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to read health file");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /health/rustfs — browse the local RustFS bucket.
pub async fn browse_rustfs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match browse_bucket(&state.s3_client, &state.rustfs_bucket, "").await {
        Ok(resp) => {
            (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response()
        }
        Err(e) => {
            error!(error = %e, "browse_rustfs failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    }
}

/// GET /health/s3 — browse the AWS S3 archive bucket.
pub async fn browse_s3(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match browse_bucket(&state.aws_s3_client, &state.aws_s3_bucket, &state.aws_s3_prefix).await {
        Ok(resp) => {
            (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response()
        }
        Err(e) => {
            warn!(error = %e, "browse_s3 failed (AWS credentials may not be configured)");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": e,
                    "hint": "AWS credentials may not be configured"
                })),
            )
                .into_response()
        }
    }
}

/// GET /health/streams — check reachability of configured robot stream endpoints.
pub async fn check_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct StreamStatus {
        robot_id: String,
        stream_url: String,
        reachable: bool,
    }

    let mut results = Vec::new();
    for (robot_id, url) in &state.robot_stream_urls {
        let reachable = check_tcp_reachable(url).await;
        results.push(StreamStatus {
            robot_id: robot_id.clone(),
            stream_url: url.clone(),
            reachable,
        });
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get disk usage for the filesystem containing the given path using statvfs.
fn get_disk_usage(path: &std::path::Path) -> Option<serde_json::Value> {
    use std::ffi::CString;
    let c_path = CString::new(path.to_str()?).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return None;
        }
        let block_size = stat.f_frsize as u64;
        let total_bytes = stat.f_blocks as u64 * block_size;
        let free_bytes = stat.f_bavail as u64 * block_size;
        let used_bytes = total_bytes - (stat.f_bfree as u64 * block_size);
        let total_gb = total_bytes as f64 / 1_073_741_824.0;
        let used_gb = used_bytes as f64 / 1_073_741_824.0;
        let free_gb = free_bytes as f64 / 1_073_741_824.0;
        let usage_pct = if total_bytes > 0 {
            (used_bytes as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };
        let status = if usage_pct > 95.0 {
            "critical"
        } else if usage_pct > 85.0 {
            "pressure"
        } else {
            "healthy"
        };
        Some(serde_json::json!({
            "total_gb": (total_gb * 10.0).round() / 10.0,
            "used_gb": (used_gb * 10.0).round() / 10.0,
            "free_gb": (free_gb * 10.0).round() / 10.0,
            "usage_pct": (usage_pct * 10.0).round() / 10.0,
            "status": status
        }))
    }
}

/// List objects from an S3-compatible bucket, grouped by top-level prefix (robot).
async fn browse_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    root_prefix: &str,
) -> Result<BrowseResponse, String> {
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let list_resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(root_prefix)
        .delimiter("/")
        .send()
        .await
        .map_err(|e| format!("list prefixes: {e}"))?;

    let top_prefixes: Vec<String> = list_resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix().map(String::from))
        .collect();

    let mut containers = Vec::new();
    let mut grand_total_objects: i64 = 0;
    let mut grand_total_bytes: i64 = 0;
    let mut latest_epoch: i64 = 0;

    let mut robot_prefixes: Vec<String> = Vec::new();
    for family_prefix in &top_prefixes {
        let sub_resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(family_prefix)
            .delimiter("/")
            .send()
            .await
            .map_err(|e| format!("list robot prefixes under {family_prefix}: {e}"))?;
        for p in sub_resp.common_prefixes() {
            if let Some(s) = p.prefix() {
                robot_prefixes.push(s.to_string());
            }
        }
    }

    for robot_prefix in &robot_prefixes {
        let camera_prefix = format!("{robot_prefix}camera/");
        let dates_resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&camera_prefix)
            .delimiter("/")
            .send()
            .await
            .map_err(|e| format!("list dates for {robot_prefix}: {e}"))?;

        let date_folders: Vec<String> = dates_resp
            .common_prefixes()
            .iter()
            .filter_map(|p| p.prefix().map(String::from))
            .collect();

        let mut recent_objects = Vec::new();
        if let Some(latest_date_prefix) = date_folders.last() {
            let objs_resp = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(latest_date_prefix)
                .max_keys(1000)
                .send()
                .await
                .map_err(|e| format!("list objects in {latest_date_prefix}: {e}"))?;

            let all_objs = objs_resp.contents();
            for obj in all_objs {
                grand_total_objects += 1;
                grand_total_bytes += obj.size().unwrap_or(0);
            }
            for obj in all_objs.iter().rev().take(3) {
                let epoch = obj.last_modified().map(|d| d.secs()).unwrap_or(0);
                if epoch > latest_epoch {
                    latest_epoch = epoch;
                }
                recent_objects.push(BrowseObject {
                    key: obj.key().unwrap_or("").to_string(),
                    size_bytes: obj.size().unwrap_or(0),
                    last_modified: obj
                        .last_modified()
                        .map(|d| {
                            d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                                .unwrap_or_default()
                        })
                        .unwrap_or_default(),
                });
            }
        }

        let name = robot_prefix
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(robot_prefix)
            .to_string();
        let recent_dates: Vec<String> = date_folders
            .iter()
            .rev()
            .take(5)
            .map(|d| {
                d.strip_prefix(&camera_prefix)
                    .unwrap_or(d)
                    .trim_end_matches('/')
                    .to_string()
            })
            .collect();

        containers.push(BrowseContainer {
            name,
            prefix: camera_prefix,
            date_count: date_folders.len(),
            recent_dates,
            recent_objects,
        });
    }

    let is_live = (now_epoch - latest_epoch) < 120;

    Ok(BrowseResponse {
        bucket: bucket.to_string(),
        containers,
        total_objects: grand_total_objects,
        total_size_bytes: grand_total_bytes,
        is_live,
    })
}

/// Try a TCP connect to the host:port extracted from an HTTP URL.
async fn check_tcp_reachable(url: &str) -> bool {
    let without_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let authority = without_scheme.split('/').next().unwrap_or("");
    let addr = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}
