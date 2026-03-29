use std::sync::Arc;

use aws_sdk_s3::presigning::PresigningConfig;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect};
use axum::Json;
use rusqlite::params;
use tracing::{error, info, warn};

use crate::db::{open_robot_db, row_to_segment};
use crate::types::{BulkDeleteQuery, BulkDeleteResponse, PatchLabels, SegmentQuery};
use crate::AppState;

/// GET /robots — list all robots that have a .db file in db_dir
pub async fn list_robots(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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
pub async fn list_segments(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<SegmentQuery>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
            "SELECT id, robot_id, type, start_ms, end_ms, s3_key, size_bytes, labels, frame_count, description
             FROM segments
             WHERE {}
             ORDER BY start_ms ASC
             {}",
            wheres.join(" AND "),
            limit_clause
        );

        let params: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), row_to_segment)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
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
pub async fn get_segment(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt = conn.prepare(
            "SELECT id, robot_id, type, start_ms, end_ms, s3_key, size_bytes, labels, frame_count, description
             FROM segments WHERE id = ?1 AND robot_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![id, robot_id], row_to_segment)?;
        Ok::<_, rusqlite::Error>(rows.next().transpose()?)
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

/// GET /robots/:robot_id/segments/:id/video — 302 redirect to presigned object URL
pub async fn video_redirect(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt =
            conn.prepare("SELECT s3_key FROM segments WHERE id = ?1 AND robot_id = ?2")?;
        let mut rows = stmt.query_map(params![id, robot_id], |row| row.get::<_, String>(0))?;
        Ok::<_, rusqlite::Error>(rows.next().transpose()?)
    })
    .await;

    match result {
        Ok(Ok(Some(s3_key))) => {
            let presign_config =
                match PresigningConfig::expires_in(std::time::Duration::from_secs(3600)) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "failed to create presigning config");
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                };

            let trimmed_key = s3_key.trim_start_matches('/');
            let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);

            let in_archive = state
                .aws_s3_client
                .head_object()
                .bucket(&state.aws_s3_bucket)
                .key(&aws_key)
                .send()
                .await
                .is_ok();

            let in_rustfs = if !in_archive {
                state
                    .s3_client
                    .head_object()
                    .bucket(&state.rustfs_bucket)
                    .key(trimmed_key)
                    .send()
                    .await
                    .is_ok()
            } else {
                false
            };

            if !in_archive && !in_rustfs {
                warn!(
                    s3_key = trimmed_key,
                    aws_key,
                    "segment file not found in AWS S3 archive or RustFS"
                );
                return (
                    StatusCode::NOT_FOUND,
                    "segment file not found in any storage backend",
                )
                    .into_response();
            }

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
/// Unlike /video (which issues a 302 redirect to a presigned URL), this handler
/// fetches the bytes from RustFS on the server side and streams them back to the browser.
pub async fn image_proxy(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let key_result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let mut stmt =
            conn.prepare("SELECT s3_key FROM segments WHERE id = ?1 AND robot_id = ?2")?;
        let mut rows = stmt.query_map(params![id, robot_id], |row| row.get::<_, String>(0))?;
        Ok::<_, rusqlite::Error>(rows.next().transpose()?)
    })
    .await;

    let s3_key = match key_result {
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

    let content_type = output.content_type().unwrap_or("image/jpeg").to_string();
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
pub async fn patch_labels(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
    Json(body): Json<PatchLabels>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let labels_json = match serde_json::to_string(&body.labels) {
        Ok(j) => j,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let result = tokio::task::spawn_blocking(move || {
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

/// DELETE /robots/:robot_id/segments/:id — permanently delete from RustFS, AWS S3, and SQLite
pub async fn delete_segment(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &rid)?;
        let mut stmt =
            conn.prepare("SELECT s3_key FROM segments WHERE id = ?1 AND robot_id = ?2")?;
        let mut rows = stmt.query_map(params![id, rid], |row| row.get::<_, String>(0))?;
        Ok::<_, rusqlite::Error>(rows.next().transpose()?)
    })
    .await;

    let s3_key = match lookup {
        Ok(Ok(Some(key))) => key,
        Ok(Ok(None)) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "SQLite lookup failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let trimmed_key = s3_key.trim_start_matches('/');

    if let Err(e) = state
        .s3_client
        .delete_object()
        .bucket(&state.rustfs_bucket)
        .key(trimmed_key)
        .send()
        .await
    {
        warn!(error = %e, key = trimmed_key, "failed to delete from RustFS (may already be evicted)");
    } else {
        info!(key = trimmed_key, "deleted segment from RustFS");
    }

    let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);
    if let Err(e) = state
        .aws_s3_client
        .delete_object()
        .bucket(&state.aws_s3_bucket)
        .key(&aws_key)
        .send()
        .await
    {
        warn!(error = %e, key = aws_key, "failed to delete from AWS S3 archive (may not exist)");
    } else {
        info!(key = aws_key, "deleted segment from AWS S3 archive");
    }

    let db_dir2 = state.db_dir.clone();
    let rid2 = robot_id.clone();
    let db_result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir2, &rid2)?;
        conn.execute(
            "DELETE FROM segments WHERE id = ?1 AND robot_id = ?2",
            params![id, rid2],
        )
    })
    .await;

    match db_result {
        Ok(Ok(0)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Ok(_)) => {
            info!(robot_id, segment_id = id, "segment deleted permanently");
            StatusCode::NO_CONTENT.into_response()
        }
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

/// DELETE /robots/:robot_id/segments?start_ms=&end_ms=
pub async fn bulk_delete_segments(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<BulkDeleteQuery>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let start = q.start_ms;
    let end = q.end_ms;

    let lookup = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &rid)?;
        let mut stmt = conn.prepare(
            "SELECT id, s3_key FROM segments WHERE robot_id = ?1 AND start_ms >= ?2 AND end_ms <= ?3",
        )?;
        let rows = stmt.query_map(params![rid, start, end], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
    })
    .await;

    let segments = match lookup {
        Ok(Ok(segs)) => segs,
        Ok(Err(e)) => {
            error!(error = %e, "SQLite lookup failed for bulk delete");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if segments.is_empty() {
        return Json(BulkDeleteResponse { deleted: 0 }).into_response();
    }

    let ids: Vec<i64> = segments.iter().map(|(id, _)| *id).collect();
    info!(robot_id = %robot_id, count = ids.len(), "bulk deleting segments in range {}..{}", q.start_ms, q.end_ms);

    for (_id, s3_key) in &segments {
        let trimmed_key = s3_key.trim_start_matches('/');

        if let Err(e) = state
            .s3_client
            .delete_object()
            .bucket(&state.rustfs_bucket)
            .key(trimmed_key)
            .send()
            .await
        {
            warn!(error = %e, key = trimmed_key, "bulk delete: failed to delete from RustFS");
        }

        let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);
        if let Err(e) = state
            .aws_s3_client
            .delete_object()
            .bucket(&state.aws_s3_bucket)
            .key(&aws_key)
            .send()
            .await
        {
            warn!(error = %e, key = aws_key, "bulk delete: failed to delete from AWS S3");
        }
    }

    let db_dir2 = state.db_dir.clone();
    let rid2 = robot_id.clone();
    let db_result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir2, &rid2)?;
        conn.execute(
            "DELETE FROM segments WHERE robot_id = ?1 AND start_ms >= ?2 AND end_ms <= ?3",
            params![rid2, start, end],
        )
    })
    .await;

    match db_result {
        Ok(Ok(count)) => {
            info!(robot_id, count, "bulk delete completed");
            Json(BulkDeleteResponse { deleted: count }).into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "SQLite bulk delete failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
