use std::path::{Path, PathBuf};
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{delete, get, patch, post};
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
    rustfs_public_url: String,
    rustfs_bucket: String,
    s3_client: aws_sdk_s3::Client,
    labelled_data_bucket: String,
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

        let mut wheres: Vec<&str> = vec!["robot_id = ?1"];
        if q.start_ms.is_some() {
            wheres.push("end_ms >= ?2");
        }
        if q.end_ms.is_some() {
            wheres.push("start_ms <= ?3");
        }
        if q.segment_type.is_some() {
            wheres.push("type = ?4");
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

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                robot_id,
                q.start_ms.unwrap_or(i64::MIN),
                q.end_ms.unwrap_or(i64::MAX),
                q.segment_type.as_deref().unwrap_or(""),
            ],
            row_to_segment,
        )?;
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
            let url = format!(
                "{}/{}/{}",
                state.rustfs_public_url.trim_end_matches('/'),
                state.rustfs_bucket,
                s3_key.trim_start_matches('/')
            );
            info!(url, "redirecting to RustFS object");
            Redirect::temporary(&url).into_response()
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
        let mut wheres: Vec<&str> = vec!["robot_id = ?1"];
        if q.start_ms.is_some() {
            wheres.push("end_ms >= ?2");
        }
        if q.end_ms.is_some() {
            wheres.push("start_ms <= ?3");
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

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                robot_id,
                q.start_ms.unwrap_or(i64::MIN),
                q.end_ms.unwrap_or(i64::MAX),
            ],
            row_to_segment,
        )?;
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
            let mut stmt = conn.prepare(
                "SELECT id, type, start_ms, end_ms, s3_key, size_bytes
                 FROM segments WHERE id = ?1 AND robot_id = ?2",
            )?;
            if let Some(seg) = stmt.query_map(params![seg_id, rid], |row| {
                Ok(SegmentInfo {
                    segment_id: row.get(0)?,
                    segment_type: row.get(1)?,
                    start_ms: row.get(2)?,
                    end_ms: row.get(3)?,
                    source_key: row.get(4)?,
                    size_bytes: row.get::<_, Option<i64>>(5)?,
                })
            })?.next() {
                segments.push(seg?);
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

    let state = Arc::new(AppState {
        db_dir: PathBuf::from(&config.database.path),
        rustfs_public_url: config.api.rustfs_public_url.clone(),
        rustfs_bucket: config.api.rustfs_bucket.clone(),
        s3_client,
        labelled_data_bucket: config.api.labelled_data_bucket.clone(),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        // Existing segment routes
        .route("/robots", get(list_robots))
        .route("/robots/{robot_id}/segments", get(list_segments))
        .route("/robots/{robot_id}/segments/{id}", get(get_segment).patch(patch_labels))
        .route("/robots/{robot_id}/segments/{id}/video", get(video_redirect))
        // Timeline
        .route("/robots/{robot_id}/timeline", get(get_timeline))
        // Collections
        .route("/robots/{robot_id}/collections", get(list_collections).post(create_collection))
        .route("/robots/{robot_id}/collections/{id}", get(get_collection).delete(delete_collection))
        // Clips
        .route("/robots/{robot_id}/collections/{collection_id}/clips", get(list_clips).post(create_clip))
        .route("/robots/{robot_id}/collections/{collection_id}/clips/{clip_id}", delete(delete_clip))
        // Download info
        .route("/robots/{robot_id}/collections/{collection_id}/download-info", get(download_info))
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
