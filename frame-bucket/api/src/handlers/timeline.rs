use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rusqlite::params;
use tracing::error;

use crate::db::{open_robot_db, row_to_segment};
use crate::types::{SegmentQuery, TimeBounds, TimelineResponse};
use crate::AppState;

/// GET /robots/:robot_id/timeline?start_ms=&end_ms=
pub async fn get_timeline(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<SegmentQuery>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;

        let mut bounds_stmt = conn.prepare(
            "SELECT MIN(start_ms), MAX(end_ms) FROM segments WHERE robot_id = ?1",
        )?;
        let time_bounds = bounds_stmt.query_row(params![robot_id], |row| {
            Ok(TimeBounds {
                earliest_ms: row.get(0)?,
                latest_ms: row.get(1)?,
            })
        })?;

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
        let segments = rows.collect::<rusqlite::Result<Vec<_>>>()?;

        Ok::<_, rusqlite::Error>(TimelineResponse {
            segments,
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

/// GET /robots/:robot_id/active-dates
/// Returns a list of ISO 8601 dates (YYYY-MM-DD) for which the robot has segment data.
pub async fn get_active_dates(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
