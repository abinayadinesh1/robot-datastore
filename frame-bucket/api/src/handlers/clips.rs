use std::path::PathBuf;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rusqlite::params;
use tracing::{error, warn};

use crate::db::open_robot_db;
use crate::types::{ClipResponse, CreateClip, DownloadInfo, SegmentInfo};
use crate::AppState;

/// GET /robots/:robot_id/collections/:collection_id/clips
pub async fn list_clips(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
        rows.collect::<rusqlite::Result<Vec<_>>>()
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
pub async fn create_clip(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
    Json(body): Json<CreateClip>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let seg_ids = body.segment_ids.clone();

    let db_result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &rid)?;

        let collection_name: String = conn.query_row(
            "SELECT name FROM collections WHERE id = ?1 AND robot_id = ?2",
            params![collection_id, rid],
            |row| row.get(0),
        )?;

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

        Ok::<_, rusqlite::Error>((collection_name, segments))
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
        return (
            StatusCode::BAD_REQUEST,
            "No valid segments found for given segment_ids",
        )
            .into_response();
    }

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

    let db_dir2 = PathBuf::from(&state.db_dir);
    let rid2 = robot_id.clone();
    let seg_ids_json = serde_json::to_string(&body.segment_ids).unwrap();
    let manifest_key2 = manifest_key.clone();
    let clip_start = body.clip_start_ms;
    let clip_end = body.clip_end_ms;

    let insert_result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir2, &rid2)?;
        let now = chrono::Utc::now().timestamp_millis();
        conn.execute(
            "INSERT INTO collection_clips
             (collection_id, robot_id, modality, clip_start_ms, clip_end_ms, segment_ids, manifest_s3_key, created_at)
             VALUES (?1, ?2, 'camera', ?3, ?4, ?5, ?6, ?7)",
            params![collection_id, rid2, clip_start, clip_end, seg_ids_json, manifest_key2, now],
        )?;
        let id = conn.last_insert_rowid();
        conn.execute(
            "UPDATE collections SET updated_at = ?1 WHERE id = ?2",
            params![now, collection_id],
        )?;
        Ok::<_, rusqlite::Error>(id)
    })
    .await;

    match insert_result {
        Ok(Ok(clip_id)) => {
            let segment_ids: Vec<i64> = body.segment_ids;
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "id": clip_id,
                    "collection_id": collection_id,
                    "manifest_s3_key": manifest_key,
                    "segment_ids": segment_ids,
                })),
            )
                .into_response()
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
pub async fn delete_clip(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, _collection_id, clip_id)): AxumPath<(String, i64, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
pub async fn download_info(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;

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

        Ok::<_, rusqlite::Error>(DownloadInfo {
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
