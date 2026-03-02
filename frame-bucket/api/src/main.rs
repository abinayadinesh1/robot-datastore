use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{delete, get};
use axum::{Json, Router};
use frame_bucket_common::config::Config;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct AppState {
    db_dir: PathBuf,
    #[allow(dead_code)]
    rustfs_public_url: String,
    rustfs_bucket: String,
    s3_client: aws_sdk_s3::Client,
    labelled_data_bucket: String,
    health_file_path: PathBuf,
    aws_s3_client: aws_sdk_s3::Client,
    aws_s3_bucket: String,
    aws_s3_prefix: String,
    robot_stream_urls: Vec<(String, String)>, // (robot_id, stream_url)
}

// ---------------------------------------------------------------------------
// Types — Segments
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct Segment {
    id: i64,
    robot_id: String,
    #[serde(rename = "type")]
    segment_type: String,
    start_ms: i64,
    end_ms: i64,
    s3_key: String,
    size_bytes: Option<i64>,
    frame_count: Option<i64>,
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SegmentQuery {
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    #[serde(rename = "type")]
    segment_type: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PatchLabels {
    labels: Vec<String>,
}

// ---------------------------------------------------------------------------
// Types — Collections
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct CollectionResponse {
    id: i64,
    robot_id: String,
    name: String,
    description: String,
    created_at: i64,
    updated_at: i64,
    clip_count: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CreateCollection {
    name: String,
    description: Option<String>,
}

// ---------------------------------------------------------------------------
// Types — Clips
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ClipResponse {
    id: i64,
    collection_id: i64,
    robot_id: String,
    modality: String,
    clip_start_ms: i64,
    clip_end_ms: i64,
    segment_ids: Vec<i64>,
    manifest_s3_key: Option<String>,
    created_at: i64,
}

#[derive(Debug, Deserialize)]
struct CreateClip {
    clip_start_ms: i64,
    clip_end_ms: i64,
    segment_ids: Vec<i64>,
    labels: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Types — Timeline
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct TimelineResponse {
    segments: Vec<Segment>,
    time_bounds: TimeBounds,
}

#[derive(Debug, Serialize)]
struct TimeBounds {
    earliest_ms: Option<i64>,
    latest_ms: Option<i64>,
}

// ---------------------------------------------------------------------------
// Types — Download info
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct DownloadInfo {
    total_bytes: i64,
    clip_count: i64,
}

// ---------------------------------------------------------------------------
// DB helpers (sync, wrapped in spawn_blocking)
// ---------------------------------------------------------------------------

fn open_robot_db(db_dir: &Path, robot_id: &str) -> rusqlite::Result<Connection> {
    let path = db_dir.join(format!("{robot_id}.db"));
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;")?;
    Ok(conn)
}

fn row_to_segment(row: &rusqlite::Row<'_>) -> rusqlite::Result<Segment> {
    let labels_raw: String = row.get(7)?;
    let labels: Vec<String> =
        serde_json::from_str(&labels_raw).unwrap_or_default();
    Ok(Segment {
        id: row.get(0)?,
        robot_id: row.get(1)?,
        segment_type: row.get(2)?,
        start_ms: row.get(3)?,
        end_ms: row.get(4)?,
        s3_key: row.get(5)?,
        size_bytes: row.get(6)?,
        frame_count: row.get(8).ok(),
        labels,
    })
}

// ---------------------------------------------------------------------------
// Handlers — Segments (existing)
// ---------------------------------------------------------------------------

/// GET /robots — list all robots that have a .db file in db_dir
async fn list_robots(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut robots = Vec::new();
        let Ok(entries) = std::fs::read_dir(&db_dir) else {
            return robots;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("db") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    robots.push(stem.to_string());
                }
            }
        }
        robots.sort();
        robots
    })
    .await;

    match result {
        Ok(robots) => Json(robots).into_response(),
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/segments?start_ms=&end_ms=&type=&limit=
async fn list_segments(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<SegmentQuery>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Vec<Segment>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;

        let mut wheres: Vec<String> = vec!["robot_id = ?".into()];
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(robot_id)];

        if let Some(start_ms) = q.start_ms {
            param_values.push(Box::new(start_ms));
            wheres.push(format!("end_ms >= ?{}", param_values.len()));
        }
        if let Some(end_ms) = q.end_ms {
            param_values.push(Box::new(end_ms));
            wheres.push(format!("start_ms <= ?{}", param_values.len()));
        }
        if let Some(ref seg_type) = q.segment_type {
            param_values.push(Box::new(seg_type.clone()));
            wheres.push(format!("type = ?{}", param_values.len()));
        }
        let limit_clause = format!("LIMIT {}", q.limit.unwrap_or(100).min(1000));
        let sql = format!(
            "SELECT id, robot_id, type, start_ms, end_ms, s3_key, size_bytes, labels, frame_count
             FROM segments
             WHERE {}
             ORDER BY start_ms ASC
             {}",
            wheres.join(" AND "),
            limit_clause
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), row_to_segment)?;
        rows.collect()
    })
    .await;

    match result {
        Ok(Ok(segments)) => Json(segments).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/segments/:id
async fn get_segment(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Option<Segment>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT id, robot_id, type, start_ms, end_ms, s3_key, size_bytes, labels, frame_count
             FROM segments WHERE id = ?1 AND robot_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![id, robot_id], row_to_segment)?;
        Ok(rows.next().transpose()?)
    })
    .await;

    match result {
        Ok(Ok(Some(seg))) => Json(seg).into_response(),
        Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/segments/:id/video — 302 redirect to RustFS object URL
async fn video_redirect(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Option<String>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt =
            conn.prepare("SELECT s3_key FROM segments WHERE id = ?1 AND robot_id = ?2")?;
        let mut rows = stmt.query_map(params![id, robot_id], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    })
    .await;

    match result {
        Ok(Ok(Some(s3_key))) => {
            let presign_config = match PresigningConfig::expires_in(std::time::Duration::from_secs(3600)) {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "failed to create presigning config");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            let trimmed_key = s3_key.trim_start_matches('/');

            // Check AWS S3 archive first; fall back to RustFS for recent data.
            let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);
            let in_archive = state
                .aws_s3_client
                .head_object()
                .bucket(&state.aws_s3_bucket)
                .key(&aws_key)
                .send()
                .await
                .is_ok();

            let presigned = if in_archive {
                state
                    .aws_s3_client
                    .get_object()
                    .bucket(&state.aws_s3_bucket)
                    .key(&aws_key)
                    .presigned(presign_config)
                    .await
            } else {
                state
                    .s3_client
                    .get_object()
                    .bucket(&state.rustfs_bucket)
                    .key(trimmed_key)
                    .presigned(presign_config)
                    .await
            };

            match presigned {
                Ok(req) => {
                    let url = req.uri().to_string();
                    let source = if in_archive { "AWS S3 archive" } else { "RustFS" };
                    info!(url, source, "redirecting to presigned URL");
                    Redirect::temporary(&url).into_response()
                }
                Err(e) => {
                    error!(error = %e, "failed to generate presigned URL");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/segments/:id/image — proxy JPEG keyframe through API server.
///
/// Unlike /video (which issues a 302 redirect to a presigned RustFS URL), this handler
/// fetches the bytes from RustFS on the server side and streams them back to the browser.
/// This means the browser only needs to reach the API port, not the RustFS port directly.
async fn image_proxy(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let key_result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Option<String>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt =
            conn.prepare("SELECT s3_key FROM segments WHERE id = ?1 AND robot_id = ?2")?;
        let mut rows = stmt.query_map(params![id, robot_id], |row| row.get::<_, String>(0))?;
        Ok(rows.next().transpose()?)
    })
    .await;

    let s3_key = match key_result {
        // H.264 idle segments have a synthetic key "idle:start/end" — no JPEG to serve.
        Ok(Ok(Some(k))) if !k.starts_with("idle:") => k,
        Ok(Ok(Some(_))) | Ok(Ok(None)) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed in image_proxy");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed in image_proxy");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let trimmed_key = s3_key.trim_start_matches('/');

    // Try AWS S3 archive first, fall back to RustFS for recently recorded data.
    let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);
    let output = match state
        .aws_s3_client
        .get_object()
        .bucket(&state.aws_s3_bucket)
        .key(&aws_key)
        .send()
        .await
    {
        Ok(output) => output,
        Err(_) => {
            match state
                .s3_client
                .get_object()
                .bucket(&state.rustfs_bucket)
                .key(trimmed_key)
                .send()
                .await
            {
                Ok(output) => output,
                Err(e) => {
                    error!(error = %e, s3_key, "image not found in AWS S3 archive or RustFS");
                    return StatusCode::NOT_FOUND.into_response();
                }
            }
        }
    };

    let content_type = output
        .content_type()
        .unwrap_or("image/jpeg")
        .to_string();
    match output.body.collect().await {
        Ok(data) => (
            [(axum::http::header::CONTENT_TYPE, content_type)],
            data.into_bytes(),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "failed to read image body");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// PATCH /robots/:robot_id/segments/:id — update labels
async fn patch_labels(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
    Json(body): Json<PatchLabels>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let labels_json = match serde_json::to_string(&body.labels) {
        Ok(j) => j,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<usize> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        conn.execute(
            "UPDATE segments SET labels = ?1 WHERE id = ?2 AND robot_id = ?3",
            params![labels_json, id, robot_id],
        )
    })
    .await;

    match result {
        Ok(Ok(0)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite update failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers — Timeline
// ---------------------------------------------------------------------------

/// GET /robots/:robot_id/timeline?start_ms=&end_ms=
async fn get_timeline(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<SegmentQuery>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<TimelineResponse> {
        let conn = open_robot_db(&db_dir, &robot_id)?;

        // Get time bounds
        let mut bounds_stmt = conn.prepare(
            "SELECT MIN(start_ms), MAX(end_ms) FROM segments WHERE robot_id = ?1",
        )?;
        let time_bounds = bounds_stmt.query_row(params![robot_id], |row| {
            Ok(TimeBounds {
                earliest_ms: row.get(0)?,
                latest_ms: row.get(1)?,
            })
        })?;

        // Get segments in range
        let mut wheres: Vec<String> = vec!["robot_id = ?".into()];
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(robot_id)];

        if let Some(start_ms) = q.start_ms {
            param_values.push(Box::new(start_ms));
            wheres.push(format!("end_ms >= ?{}", param_values.len()));
        }
        if let Some(end_ms) = q.end_ms {
            param_values.push(Box::new(end_ms));
            wheres.push(format!("start_ms <= ?{}", param_values.len()));
        }
        let limit_clause = format!("LIMIT {}", q.limit.unwrap_or(500).min(1000));
        let sql = format!(
            "SELECT id, robot_id, type, start_ms, end_ms, s3_key, size_bytes, labels, frame_count
             FROM segments
             WHERE {}
             ORDER BY start_ms ASC
             {}",
            wheres.join(" AND "),
            limit_clause
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), row_to_segment)?;
        let segments: rusqlite::Result<Vec<Segment>> = rows.collect();

        Ok(TimelineResponse {
            segments: segments?,
            time_bounds,
        })
    })
    .await;

    match result {
        Ok(Ok(timeline)) => Json(timeline).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers — Active Dates
// ---------------------------------------------------------------------------

/// GET /robots/:robot_id/active-dates
/// Returns a list of ISO 8601 dates (YYYY-MM-DD) for which the robot has segment data.
async fn get_active_dates(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Vec<String>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT strftime('%Y-%m-%d', start_ms / 1000.0, 'unixepoch')
             FROM segments
             WHERE robot_id = ?1
             ORDER BY 1 ASC",
        )?;
        let dates: rusqlite::Result<Vec<String>> = stmt
            .query_map(params![robot_id], |row| row.get(0))?
            .collect();
        dates
    })
    .await;

    match result {
        Ok(Ok(dates)) => Json(serde_json::json!({ "dates": dates })).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers — Collections
// ---------------------------------------------------------------------------

/// GET /robots/:robot_id/collections
async fn list_collections(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Vec<CollectionResponse>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT c.id, c.robot_id, c.name, c.description, c.created_at, c.updated_at,
                    (SELECT COUNT(*) FROM collection_clips cc WHERE cc.collection_id = c.id)
             FROM collections c
             WHERE c.robot_id = ?1
             ORDER BY c.updated_at DESC",
        )?;
        let rows = stmt.query_map(params![robot_id], |row| {
            Ok(CollectionResponse {
                id: row.get(0)?,
                robot_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
                clip_count: row.get(6)?,
            })
        })?;
        rows.collect()
    })
    .await;

    match result {
        Ok(Ok(collections)) => Json(collections).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /robots/:robot_id/collections
async fn create_collection(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Json(body): Json<CreateCollection>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<CollectionResponse> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        let desc = body.description.as_deref().unwrap_or("");
        conn.execute(
            "INSERT INTO collections (robot_id, name, description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![robot_id, body.name, desc, now, now],
        )?;
        let id = conn.last_insert_rowid();
        Ok(CollectionResponse {
            id,
            robot_id,
            name: body.name,
            description: desc.to_string(),
            created_at: now,
            updated_at: now,
            clip_count: Some(0),
        })
    })
    .await;

    match result {
        Ok(Ok(collection)) => (StatusCode::CREATED, Json(collection)).into_response(),
        Ok(Err(e)) => {
            // Check for UNIQUE constraint violation
            let msg = e.to_string();
            if msg.contains("UNIQUE") {
                (StatusCode::CONFLICT, "Collection with that name already exists").into_response()
            } else {
                error!(error = %e, "SQLite insert failed");
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            }
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/collections/:id
async fn get_collection(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Option<CollectionResponse>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT c.id, c.robot_id, c.name, c.description, c.created_at, c.updated_at,
                    (SELECT COUNT(*) FROM collection_clips cc WHERE cc.collection_id = c.id)
             FROM collections c
             WHERE c.id = ?1 AND c.robot_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![id, robot_id], |row| {
            Ok(CollectionResponse {
                id: row.get(0)?,
                robot_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
                clip_count: row.get(6)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    })
    .await;

    match result {
        Ok(Ok(Some(c))) => Json(c).into_response(),
        Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// DELETE /robots/:robot_id/collections/:id
async fn delete_collection(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<usize> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        conn.execute(
            "DELETE FROM collections WHERE id = ?1 AND robot_id = ?2",
            params![id, robot_id],
        )
    })
    .await;

    match result {
        Ok(Ok(0)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite delete failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers — Clips
// ---------------------------------------------------------------------------

/// GET /robots/:robot_id/collections/:collection_id/clips
async fn list_clips(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<Vec<ClipResponse>> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT id, collection_id, robot_id, modality, clip_start_ms, clip_end_ms,
                    segment_ids, manifest_s3_key, created_at
             FROM collection_clips
             WHERE collection_id = ?1 AND robot_id = ?2
             ORDER BY clip_start_ms ASC",
        )?;
        let rows = stmt.query_map(params![collection_id, robot_id], |row| {
            let seg_ids_raw: String = row.get(6)?;
            let segment_ids: Vec<i64> = serde_json::from_str(&seg_ids_raw).unwrap_or_default();
            Ok(ClipResponse {
                id: row.get(0)?,
                collection_id: row.get(1)?,
                robot_id: row.get(2)?,
                modality: row.get(3)?,
                clip_start_ms: row.get(4)?,
                clip_end_ms: row.get(5)?,
                segment_ids,
                manifest_s3_key: row.get(7)?,
                created_at: row.get(8)?,
            })
        })?;
        rows.collect()
    })
    .await;

    match result {
        Ok(Ok(clips)) => Json(clips).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /robots/:robot_id/collections/:collection_id/clips
/// Saves a clip: builds manifest JSON and writes to labelled-data S3 bucket.
async fn create_clip(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
    Json(body): Json<CreateClip>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let seg_ids = body.segment_ids.clone();

    // Step 1: Look up collection name and segment metadata from DB
    let db_result = tokio::task::spawn_blocking(move || -> rusqlite::Result<(String, Vec<SegmentInfo>)> {
        let conn = open_robot_db(&db_dir, &rid)?;

        // Get collection name
        let collection_name: String = conn.query_row(
            "SELECT name FROM collections WHERE id = ?1 AND robot_id = ?2",
            params![collection_id, rid],
            |row| row.get(0),
        )?;

        // Get segment metadata for all referenced segments
        let mut segments = Vec::new();
        for seg_id in &seg_ids {
            let result = conn.query_row(
                "SELECT id, type, start_ms, end_ms, s3_key, size_bytes
                 FROM segments WHERE id = ?1 AND robot_id = ?2",
                params![seg_id, rid],
                |row| {
                    Ok(SegmentInfo {
                        segment_id: row.get(0)?,
                        segment_type: row.get(1)?,
                        start_ms: row.get(2)?,
                        end_ms: row.get(3)?,
                        source_key: row.get(4)?,
                        size_bytes: row.get::<_, Option<i64>>(5)?,
                    })
                },
            );
            if let Ok(seg) = result {
                segments.push(seg);
            }
        }

        Ok((collection_name, segments))
    })
    .await;

    let (collection_name, segment_infos) = match db_result {
        Ok(Ok(data)) => data,
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.contains("no rows") {
                return (StatusCode::NOT_FOUND, "Collection not found").into_response();
            }
            error!(error = %e, "SQLite query failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if segment_infos.is_empty() {
        return (StatusCode::BAD_REQUEST, "No valid segments found for given segment_ids").into_response();
    }

    // Step 2: Build manifest JSON
    let manifest_segments: Vec<serde_json::Value> = segment_infos
        .iter()
        .map(|s| {
            serde_json::json!({
                "segment_id": s.segment_id,
                "source_bucket": state.rustfs_bucket,
                "source_key": s.source_key,
                "start_ms": s.start_ms,
                "end_ms": s.end_ms,
                "type": s.segment_type,
                "size_bytes": s.size_bytes,
                "modality": "camera"
            })
        })
        .collect();

    let manifest = serde_json::json!({
        "robot_id": robot_id,
        "collection_id": collection_id,
        "collection_name": collection_name,
        "clip_start_ms": body.clip_start_ms,
        "clip_end_ms": body.clip_end_ms,
        "labels": body.labels.as_deref().unwrap_or(&[]),
        "segments": manifest_segments,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });

    let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    let safe_name = collection_name.replace(' ', "_").replace('/', "-");
    let manifest_key = format!(
        "{}/{}/{}_{}.json",
        robot_id, safe_name, body.clip_start_ms, body.clip_end_ms
    );

    // Step 3: Write manifest to labelled-data bucket
    if let Err(e) = state
        .s3_client
        .put_object()
        .bucket(&state.labelled_data_bucket)
        .key(&manifest_key)
        .content_type("application/json")
        .body(ByteStream::from(manifest_bytes))
        .send()
        .await
    {
        warn!(error = %e, "Failed to write manifest to S3 (continuing anyway)");
    }

    // Step 4: Insert clip into DB
    let db_dir2 = PathBuf::from(&state.db_dir);
    let rid2 = robot_id.clone();
    let seg_ids_json = serde_json::to_string(&body.segment_ids).unwrap();
    let manifest_key2 = manifest_key.clone();
    let clip_start = body.clip_start_ms;
    let clip_end = body.clip_end_ms;

    let insert_result = tokio::task::spawn_blocking(move || -> rusqlite::Result<i64> {
        let conn = open_robot_db(&db_dir2, &rid2)?;
        let now = chrono::Utc::now().timestamp_millis();
        conn.execute(
            "INSERT INTO collection_clips
             (collection_id, robot_id, modality, clip_start_ms, clip_end_ms, segment_ids, manifest_s3_key, created_at)
             VALUES (?1, ?2, 'camera', ?3, ?4, ?5, ?6, ?7)",
            params![collection_id, rid2, clip_start, clip_end, seg_ids_json, manifest_key2, now],
        )?;
        let id = conn.last_insert_rowid();
        // Touch collection updated_at
        conn.execute(
            "UPDATE collections SET updated_at = ?1 WHERE id = ?2",
            params![now, collection_id],
        )?;
        Ok(id)
    })
    .await;

    match insert_result {
        Ok(Ok(clip_id)) => {
            let segment_ids: Vec<i64> = body.segment_ids;
            (StatusCode::CREATED, Json(serde_json::json!({
                "id": clip_id,
                "collection_id": collection_id,
                "manifest_s3_key": manifest_key,
                "segment_ids": segment_ids,
            }))).into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "SQLite insert failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// DELETE /robots/:robot_id/collections/:collection_id/clips/:clip_id
async fn delete_clip(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, _collection_id, clip_id)): AxumPath<(String, i64, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<usize> {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        conn.execute(
            "DELETE FROM collection_clips WHERE id = ?1 AND robot_id = ?2",
            params![clip_id, robot_id],
        )
    })
    .await;

    match result {
        Ok(Ok(0)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite delete failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// GET /robots/:robot_id/collections/:collection_id/download-info
async fn download_info(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<DownloadInfo> {
        let conn = open_robot_db(&db_dir, &robot_id)?;

        // Get all segment_ids referenced by clips in this collection
        let mut stmt = conn.prepare(
            "SELECT segment_ids FROM collection_clips
             WHERE collection_id = ?1 AND robot_id = ?2",
        )?;
        let rows = stmt.query_map(params![collection_id, robot_id], |row| {
            row.get::<_, String>(0)
        })?;

        let mut all_seg_ids = std::collections::HashSet::new();
        let mut clip_count = 0i64;
        for row in rows {
            let seg_ids_json = row?;
            let ids: Vec<i64> = serde_json::from_str(&seg_ids_json).unwrap_or_default();
            for id in ids {
                all_seg_ids.insert(id);
            }
            clip_count += 1;
        }

        // Sum size_bytes for all unique segments
        let mut total_bytes = 0i64;
        for seg_id in &all_seg_ids {
            let bytes: Option<i64> = conn
                .query_row(
                    "SELECT size_bytes FROM segments WHERE id = ?1",
                    params![seg_id],
                    |row| row.get(0),
                )
                .ok()
                .flatten();
            total_bytes += bytes.unwrap_or(0);
        }

        Ok(DownloadInfo {
            total_bytes,
            clip_count,
        })
    })
    .await;

    match result {
        Ok(Ok(info)) => Json(info).into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct SegmentInfo {
    segment_id: i64,
    segment_type: String,
    start_ms: i64,
    end_ms: i64,
    source_key: String,
    size_bytes: Option<i64>,
}

// ---------------------------------------------------------------------------
// S3 helpers
// ---------------------------------------------------------------------------

async fn ensure_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => {
            info!(bucket, "labelled-data bucket exists");
        }
        Err(_) => {
            info!(bucket, "creating labelled-data bucket");
            match client.create_bucket().bucket(bucket).send().await {
                Ok(_) => info!(bucket, "bucket created"),
                Err(e) => warn!(error = %e, bucket, "failed to create bucket"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

/// GET /health — returns the consumer's health state JSON, enriched with host disk stats.
async fn get_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let path = state.health_file_path.clone();
    match tokio::task::spawn_blocking(move || {
        let contents = std::fs::read_to_string(&path)?;
        let mut json: serde_json::Value = serde_json::from_str(&contents)
            .unwrap_or(serde_json::Value::Null);

        // Enrich with host disk usage (the filesystem where data/ lives).
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

// ---------------------------------------------------------------------------
// Health: Browse RustFS / S3 / Streams
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

/// List objects from an S3-compatible bucket, grouped by top-level prefix (robot)
/// and showing recent objects from the latest date folder.
async fn browse_bucket(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    root_prefix: &str,
) -> Result<BrowseResponse, String> {
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Step 1: List top-level prefixes (robot IDs)
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

    // Keys are structured as {family}/{robot_id}/camera/{date}/... so top_prefixes
    // contains family prefixes (e.g. "reachy/"). We need one more level to get
    // the actual robot_id prefixes (e.g. "reachy/reachy-002/").
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
        // For each robot, look under {robot}/camera/
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

        // Get recent objects from the latest date folder
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
            // Accumulate totals from this page
            for obj in all_objs {
                grand_total_objects += 1;
                grand_total_bytes += obj.size().unwrap_or(0);
            }
            // Take the last 3 (most recent by key sort)
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

        // robot_prefix is "{family}/{robot_id}/"; extract the robot_id (last segment)
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

    // is_live: most recent object was modified within 120 seconds
    let is_live = (now_epoch - latest_epoch) < 120;

    Ok(BrowseResponse {
        bucket: bucket.to_string(),
        containers,
        total_objects: grand_total_objects,
        total_size_bytes: grand_total_bytes,
        is_live,
    })
}

/// GET /health/rustfs — browse the local RustFS bucket.
async fn browse_rustfs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match browse_bucket(&state.s3_client, &state.rustfs_bucket, "").await {
        Ok(resp) => (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response(),
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
async fn browse_s3(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match browse_bucket(&state.aws_s3_client, &state.aws_s3_bucket, &state.aws_s3_prefix).await {
        Ok(resp) => (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response(),
        Err(e) => {
            warn!(error = %e, "browse_s3 failed (AWS credentials may not be configured)");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": e, "hint": "AWS credentials may not be configured" })),
            )
                .into_response()
        }
    }
}

/// GET /health/streams — check reachability of configured robot stream endpoints.
async fn check_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config from {}: {e}", config_path.display());
            std::process::exit(1);
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.logging.level.parse().unwrap_or_default()),
        )
        .init();

    // Build S3 client using RustFS credentials (same pattern as consumer/storage.rs)
    let creds = Credentials::new(
        &config.rustfs.access_key,
        &config.rustfs.secret_key,
        None,
        None,
        "static",
    );
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&config.rustfs.endpoint)
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    // Ensure labelled-data bucket exists
    ensure_bucket(&s3_client, &config.api.labelled_data_bucket).await;

    // Build AWS S3 client for archive bucket (uses default credential chain)
    let aws_sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.aws_s3.region.clone()))
        .load()
        .await;
    let aws_s3_client = aws_sdk_s3::Client::new(&aws_sdk_config);

    let robot_stream_urls: Vec<(String, String)> = config
        .robots
        .iter()
        .map(|r| (r.robot_id.clone(), r.stream_url.clone()))
        .collect();

    let state = Arc::new(AppState {
        db_dir: PathBuf::from(&config.database.path),
        rustfs_public_url: config.api.rustfs_public_url.clone(),
        rustfs_bucket: config.api.rustfs_bucket.clone(),
        s3_client,
        labelled_data_bucket: config.api.labelled_data_bucket.clone(),
        health_file_path: PathBuf::from(&config.database.path).join("storage_stats.json"),
        aws_s3_client,
        aws_s3_bucket: config.aws_s3.bucket.clone(),
        aws_s3_prefix: config.aws_s3.prefix.clone(),
        robot_stream_urls,
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        // Segment routes
        .route("/robots", get(list_robots))
        .route("/robots/:robot_id/segments", get(list_segments))
        .route("/robots/:robot_id/segments/:id", get(get_segment).patch(patch_labels))
        .route("/robots/:robot_id/segments/:id/video", get(video_redirect))
        .route("/robots/:robot_id/segments/:id/image", get(image_proxy))
        // Timeline
        .route("/robots/:robot_id/timeline", get(get_timeline))
        .route("/robots/:robot_id/active-dates", get(get_active_dates))
        // Collections
        .route("/robots/:robot_id/collections", get(list_collections).post(create_collection))
        .route("/robots/:robot_id/collections/:id", get(get_collection).delete(delete_collection))
        // Clips
        .route("/robots/:robot_id/collections/:collection_id/clips", get(list_clips).post(create_clip))
        .route("/robots/:robot_id/collections/:collection_id/clips/:clip_id", delete(delete_clip))
        // Download info
        .route("/robots/:robot_id/collections/:collection_id/download-info", get(download_info))
        // Health
        .route("/health", get(get_health))
        .route("/health/rustfs", get(browse_rustfs))
        .route("/health/s3", get(browse_s3))
        .route("/health/streams", get(check_streams))
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.api.port);
    info!(addr, "frame-bucket API server starting");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("Failed to bind to {addr}: {e}");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}
