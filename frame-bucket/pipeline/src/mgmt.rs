use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use axum::{Json, Router};
use frame_bucket_common::config::{Config, RobotConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

// ---------------------------------------------------------------------------
// Commands sent from HTTP handlers → main loop
// ---------------------------------------------------------------------------

pub enum Command {
    /// Spawn a new robot pipeline. Reply indicates success/failure.
    AddRobot(RobotConfig, oneshot::Sender<Result<(), String>>),
    /// Stop and remove a running robot pipeline.
    RemoveRobot(String, oneshot::Sender<Result<(), String>>),
}

// ---------------------------------------------------------------------------
// Shared state for handlers
// ---------------------------------------------------------------------------

struct MgmtState {
    config_path: PathBuf,
    cmd_tx: mpsc::Sender<Command>,
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct StreamResponse {
    robot_id: String,
    stream_url: String,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    h264_url: Option<String>,
    fps: f64,
    quality: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    webrtc_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_id: Option<String>,
}

impl From<RobotConfig> for StreamResponse {
    fn from(r: RobotConfig) -> Self {
        Self {
            robot_id: r.robot_id,
            stream_url: r.stream_url,
            mode: r.mode,
            h264_url: r.h264_url,
            fps: r.fps,
            quality: r.quality,
            label: r.label,
            webrtc_url: r.webrtc_url,
            peer_id: r.peer_id,
        }
    }
}

#[derive(Debug, Serialize)]
struct StreamsListResponse {
    streams: Vec<StreamResponse>,
}

#[derive(Debug, Deserialize)]
pub struct CreateStreamRequest {
    pub robot_id: String,
    pub stream_url: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    pub h264_url: Option<String>,
    #[serde(default = "default_quality")]
    pub quality: u32,
    #[serde(default = "default_fps")]
    pub fps: f64,
    pub label: Option<String>,
    pub webrtc_url: Option<String>,
    pub peer_id: Option<String>,
}

fn default_mode() -> String { "mjpeg".into() }
fn default_quality() -> u32 { 80 }
fn default_fps() -> f64 { 10.0 }

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// GET /streams — list all configured robots from config.toml
async fn list_streams(State(state): State<Arc<MgmtState>>) -> impl IntoResponse {
    let config_path = state.config_path.clone();
    let result = tokio::task::spawn_blocking(move || Config::load(&config_path)).await;

    match result {
        Ok(Ok(config)) => {
            let streams: Vec<StreamResponse> =
                config.robots.into_iter().map(Into::into).collect();
            Json(StreamsListResponse { streams }).into_response()
        }
        Ok(Err(e)) => {
            error!(error = %e, "failed to read config for /streams");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking panicked");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// POST /streams — add a new robot: persist to config.toml, then spawn its pipeline
async fn create_stream(
    State(state): State<Arc<MgmtState>>,
    Json(body): Json<CreateStreamRequest>,
) -> impl IntoResponse {
    let robot = RobotConfig {
        robot_id: body.robot_id,
        stream_url: body.stream_url,
        mode: body.mode,
        h264_url: body.h264_url,
        quality: body.quality,
        fps: body.fps,
        label: body.label,
        webrtc_url: body.webrtc_url,
        peer_id: body.peer_id,
    };

    // 1. Persist to config.toml
    let config_path = state.config_path.clone();
    let robot_for_write = robot.clone();
    let write_result = tokio::task::spawn_blocking(move || {
        // Check for duplicate
        let config = Config::load(&config_path).map_err(|e| e.to_string())?;
        if config.robots.iter().any(|r| r.robot_id == robot_for_write.robot_id) {
            return Err(format!("robot_id '{}' already exists in config", robot_for_write.robot_id));
        }
        frame_bucket_common::config::append_robot_to_config(&config_path, &robot_for_write)
            .map_err(|e| e.to_string())
    })
    .await;

    match write_result {
        Ok(Err(msg)) => {
            let status = if msg.contains("already exists") {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            return (status, msg).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking panicked");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Ok(Ok(())) => {}
    }

    // 2. Tell the main loop to spawn the pipeline immediately
    let (reply_tx, reply_rx) = oneshot::channel();
    if state.cmd_tx.send(Command::AddRobot(robot.clone(), reply_tx)).await.is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "pipeline event loop is down").into_response();
    }

    match reply_rx.await {
        Ok(Ok(())) => {
            info!(robot_id = robot.robot_id, "robot added and pipeline spawned");
            (StatusCode::CREATED, Json(StreamResponse::from(robot))).into_response()
        }
        Ok(Err(msg)) => (StatusCode::CONFLICT, msg).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "no reply from pipeline").into_response(),
    }
}

/// DELETE /streams/:robot_id — stop pipeline and remove from config.toml
async fn delete_stream(
    State(state): State<Arc<MgmtState>>,
    AxumPath(robot_id): AxumPath<String>,
) -> impl IntoResponse {
    // 1. Remove from config.toml
    let config_path = state.config_path.clone();
    let rid = robot_id.clone();
    let write_result = tokio::task::spawn_blocking(move || {
        frame_bucket_common::config::remove_robot_from_config(&config_path, &rid)
    })
    .await;

    match write_result {
        Ok(Ok(false)) => return StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            error!(error = %e, "failed to remove robot from config");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        Err(e) => {
            error!(error = %e, "spawn_blocking panicked");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Ok(Ok(true)) => {}
    }

    // 2. Tell the main loop to stop the pipeline
    let (reply_tx, reply_rx) = oneshot::channel();
    if state.cmd_tx.send(Command::RemoveRobot(robot_id, reply_tx)).await.is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "pipeline event loop is down").into_response();
    }

    match reply_rx.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(msg)) => {
            // Config was removed but pipeline wasn't running — still success
            info!(msg, "robot removed from config (pipeline was not running)");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "no reply from pipeline").into_response(),
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub async fn serve(port: u16, config_path: PathBuf, cmd_tx: mpsc::Sender<Command>) {
    let state = Arc::new(MgmtState { config_path, cmd_tx });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/streams", get(list_streams).post(create_stream))
        .route("/streams/:robot_id", delete(delete_stream))
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    info!(addr, "pipeline management API starting");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        error!(addr, error = %e, "failed to bind management API");
        std::process::exit(1);
    });
    axum::serve(listener, app).await.unwrap();
}
