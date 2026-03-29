use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rusqlite::params;
use tracing::{error, info, warn};

use crate::db::open_robot_db;
use crate::s3::{download_segment_file, run_ffmpeg};
use crate::types::{KeyframePath, SampledKeyframesQuery, SampledKeyframesResponse};
use crate::AppState;

/// GET /robots/:robot_id/sampled-keyframes?start_ms=&end_ms=&interval_ms=
///
/// Extracts keyframes at the requested interval from all active video segments
/// in the time range, writes them as JPEGs to a temp directory, and returns a
/// JSON array of absolute file paths. The caller is responsible for cleaning
/// up the directory when done.
pub async fn get_sampled_keyframes(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<SampledKeyframesQuery>,
) -> impl IntoResponse {
    let interval_ms = q.interval_ms.unwrap_or(1000).max(100);
    let interval_s = interval_ms as f64 / 1000.0;

    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let start = q.start_ms;
    let end = q.end_ms;

    let lookup = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &rid)?;
        let mut stmt = conn.prepare(
            "SELECT id, s3_key, start_ms, end_ms FROM segments
             WHERE robot_id = ?1 AND type = 'active'
               AND end_ms >= ?2 AND start_ms <= ?3
             ORDER BY start_ms ASC",
        )?;
        let rows = stmt.query_map(params![rid, start, end], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
    })
    .await;

    let segments = match lookup {
        Ok(Ok(segs)) => segs,
        Ok(Err(e)) => {
            error!(error = %e, "SQLite lookup failed for sampled keyframes");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if segments.is_empty() {
        return Json(SampledKeyframesResponse {
            robot_id,
            start_ms: q.start_ms,
            end_ms: q.end_ms,
            interval_ms,
            frames: vec![],
        })
        .into_response();
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    let work_dir = std::env::temp_dir().join(format!("keyframes_{job_id}"));
    if let Err(e) = tokio::fs::create_dir_all(&work_dir).await {
        error!(error = %e, "failed to create temp dir");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create temp dir",
        )
            .into_response();
    }

    let mut frames: Vec<KeyframePath> = Vec::new();

    for (seg_id, s3_key, seg_start, seg_end) in &segments {
        let local_path =
            match download_segment_file(&state, s3_key, "active", &work_dir, *seg_id).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(segment_id = seg_id, error = %e, "skipping unavailable segment");
                    continue;
                }
            };

        let effective_start = q.start_ms.max(*seg_start);
        let effective_end = q.end_ms.min(*seg_end);
        let ss_ms = effective_start - seg_start;

        let ss = format!("{:.3}", ss_ms as f64 / 1000.0);
        let duration_s = format!("{:.3}", (effective_end - effective_start) as f64 / 1000.0);
        let fps_filter = format!("fps=1/{:.3}", interval_s);
        let output_pattern = work_dir
            .join(format!("seg{seg_id}_%04d.jpg"))
            .to_string_lossy()
            .to_string();

        let input = local_path.to_str().unwrap().to_string();
        let result = run_ffmpeg(&[
            "-ss",
            &ss,
            "-i",
            &input,
            "-t",
            &duration_s,
            "-vf",
            &fps_filter,
            "-q:v",
            "2",
            "-y",
            &output_pattern,
        ])
        .await;

        let _ = tokio::fs::remove_file(&local_path).await;

        if let Err(e) = result {
            warn!(segment_id = seg_id, error = %e, "ffmpeg keyframe extraction failed");
            continue;
        }

        let mut idx = 1u32;
        loop {
            let frame_path = work_dir.join(format!("seg{seg_id}_{idx:04}.jpg"));
            if !frame_path.exists() {
                break;
            }
            let frame_offset_ms = ss_ms + ((idx as i64 - 1) * interval_ms);
            let timestamp_ms = seg_start + frame_offset_ms;
            if timestamp_ms <= q.end_ms {
                frames.push(KeyframePath {
                    path: frame_path.to_string_lossy().to_string(),
                    timestamp_ms,
                    segment_id: *seg_id,
                });
            }
            idx += 1;
        }
    }

    info!(
        robot_id,
        frame_count = frames.len(),
        interval_ms,
        "sampled keyframes extracted to {}",
        work_dir.display()
    );

    Json(SampledKeyframesResponse {
        robot_id,
        start_ms: q.start_ms,
        end_ms: q.end_ms,
        interval_ms,
        frames,
    })
    .into_response()
}
