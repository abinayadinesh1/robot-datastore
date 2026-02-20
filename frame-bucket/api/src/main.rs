use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, patch};
use axum::{Json, Router};
use frame_bucket_common::config::Config;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct AppState {
    db_dir: PathBuf,
    rustfs_public_url: String,
    rustfs_bucket: String,
}

// ---------------------------------------------------------------------------
// Types
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
// DB helpers (sync, wrapped in spawn_blocking)
// ---------------------------------------------------------------------------

fn open_robot_db(db_dir: &Path, robot_id: &str) -> rusqlite::Result<Connection> {
    let path = db_dir.join(format!("{robot_id}.db"));
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
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
// Handlers
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

        // Build dynamic WHERE clauses
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
/// Body: { "labels": ["tag1", "tag2"] }
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

    let state = Arc::new(AppState {
        db_dir: PathBuf::from(&config.database.path),
        rustfs_public_url: config.api.rustfs_public_url.clone(),
        rustfs_bucket: config.api.rustfs_bucket.clone(),
    });

    let app = Router::new()
        .route("/robots", get(list_robots))
        .route("/robots/:robot_id/segments", get(list_segments))
        .route("/robots/:robot_id/segments/:id", get(get_segment))
        .route("/robots/:robot_id/segments/:id/video", get(video_redirect))
        .route("/robots/:robot_id/segments/:id", patch(patch_labels))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.api.port);
    info!(addr, "frame-bucket API server starting");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        eprintln!("Failed to bind to {addr}: {e}");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}
