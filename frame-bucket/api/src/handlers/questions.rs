use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct SubmitQuestion {
    pub question: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredQuestion {
    pub robot_id: String,
    pub question: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub submitted_at: String,
}

/// POST /robots/:robot_id/questions
pub async fn submit_question(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Json(body): Json<SubmitQuestion>,
) -> impl IntoResponse {
    let question = StoredQuestion {
        robot_id: robot_id.clone(),
        question: body.question,
        start_ms: body.start_ms,
        end_ms: body.end_ms,
        submitted_at: Utc::now().to_rfc3339(),
    };

    let path = state.db_dir.join("questions.json");

    let mut questions: Vec<StoredQuestion> = if path.exists() {
        match tokio::fs::read_to_string(&path).await {
            Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
            Err(_) => vec![],
        }
    } else {
        vec![]
    };

    questions.push(question);

    match tokio::fs::write(&path, serde_json::to_string_pretty(&questions).unwrap()).await {
        Ok(_) => {
            info!(robot_id, "question saved ({} total)", questions.len());
            (
                StatusCode::CREATED,
                Json(serde_json::json!({ "saved": true, "total": questions.len() })),
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "failed to write questions.json");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
