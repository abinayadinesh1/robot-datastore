use std::sync::Arc;

use chrono::{TimeZone, Utc};
use frame_bucket_common::config::AnnotationConfig;
use reqwest::multipart;
use tracing::{error, info, warn};

use crate::db::SegmentDb;
use crate::storage::RustfsStorage;

/// Spawn a background task that downloads a segment from RustFS, sends it to
/// the video annotation API, and writes the resulting description back to SQLite.
///
/// This is fire-and-forget: errors are logged but never propagated.
pub fn spawn_annotation(
    config: Arc<AnnotationConfig>,
    storage: Arc<RustfsStorage>,
    db: Arc<SegmentDb>,
    segment_id: i64,
    s3_key: String,
    start_ms: i64,
    end_ms: i64,
) {
    tokio::spawn(async move {
        if let Err(e) = annotate(
            &config, &storage, &db, segment_id, &s3_key, start_ms, end_ms,
        )
        .await
        {
            error!(
                segment_id,
                s3_key,
                error = %e,
                "annotation failed"
            );
        }
    });
}

async fn annotate(
    config: &AnnotationConfig,
    storage: &RustfsStorage,
    db: &SegmentDb,
    segment_id: i64,
    s3_key: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<(), AnnotationError> {
    // 1. Download the segment bytes from RustFS.
    let data = storage
        .get_object(s3_key)
        .await
        .map_err(|e| AnnotationError::Download(e.to_string()))?;

    info!(
        segment_id,
        s3_key,
        bytes = data.len(),
        "downloaded segment for annotation"
    );

    // Determine file extension from the key for the multipart filename.
    let filename = s3_key.rsplit('/').next().unwrap_or("segment.mp4").to_string();

    let mime = if filename.ends_with(".jpg") || filename.ends_with(".jpeg") {
        "image/jpeg"
    } else {
        "video/mp4"
    };

    // 2. POST to the video annotation API as multipart form.
    let file_part = multipart::Part::bytes(data)
        .file_name(filename)
        .mime_str(mime)
        .map_err(|e| AnnotationError::Request(e.to_string()))?;

    let form = multipart::Form::new()
        .part("file", file_part)
        .text("question", config.prompt.clone())
        .text("max_num_frames", config.max_num_frames.to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post(&config.url)
        .multipart(form)
        .send()
        .await
        .map_err(|e| AnnotationError::Request(e.to_string()))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(AnnotationError::Api(format!("{status}: {body}")));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| AnnotationError::Request(e.to_string()))?;

    let model_description = body["response"]
        .as_str()
        .unwrap_or("")
        .to_string();

    if model_description.is_empty() {
        warn!(segment_id, "annotation API returned empty response");
        return Ok(());
    }

    // 3. Build final description with timestamp prefix.
    let start_fmt = format_ms(start_ms);
    let end_fmt = format_ms(end_ms);
    let description = format!("From {start_fmt} to {end_fmt}, {model_description}");

    // 4. Write back to SQLite.
    db.update_description(segment_id, &description)
        .map_err(|e| AnnotationError::Db(e.to_string()))?;

    info!(segment_id, "annotation saved");
    Ok(())
}

/// Format millisecond timestamp as a human-readable UTC string.
fn format_ms(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| ms.to_string())
}

#[derive(Debug, thiserror::Error)]
enum AnnotationError {
    #[error("failed to download segment: {0}")]
    Download(String),
    #[error("annotation request failed: {0}")]
    Request(String),
    #[error("annotation API error: {0}")]
    Api(String),
    #[error("failed to update database: {0}")]
    Db(String),
}
