use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rusqlite::params;
use tracing::error;

use crate::db::open_robot_db;
use crate::types::{CollectionResponse, CreateCollection};
use crate::AppState;

/// GET /robots/:robot_id/collections
pub async fn list_collections(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
        rows.collect::<rusqlite::Result<Vec<_>>>()
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
pub async fn create_collection(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Json(body): Json<CreateCollection>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &robot_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        let desc = body.description.as_deref().unwrap_or("").to_string();
        conn.execute(
            "INSERT INTO collections (robot_id, name, description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![robot_id, body.name, desc, now, now],
        )?;
        let id = conn.last_insert_rowid();
        Ok::<_, rusqlite::Error>(CollectionResponse {
            id,
            robot_id,
            name: body.name,
            description: desc,
            created_at: now,
            updated_at: now,
            clip_count: Some(0),
        })
    })
    .await;

    match result {
        Ok(Ok(collection)) => (StatusCode::CREATED, Json(collection)).into_response(),
        Ok(Err(e)) => {
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
pub async fn get_collection(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
        Ok::<_, rusqlite::Error>(rows.next().transpose()?)
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
pub async fn delete_collection(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, id)): AxumPath<(String, i64)>,
) -> impl IntoResponse {
    let db_dir = state.db_dir.clone();
    let result = tokio::task::spawn_blocking(move || {
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
