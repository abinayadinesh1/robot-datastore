mod capture;
mod db;
mod eviction;
mod filter;
mod recorder;
mod storage;

use frame_bucket_common::config::{Config, RobotConfig};
use frame_bucket_common::frame::TimestampedFrame;
use recorder::RecordingStateMachine;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

mod mgmt;

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

    if config.robots.is_empty() {
        warn!("no [[robots]] entries in config; pipeline will wait for robots to be added");
    }

    info!(
        robot_count = config.robots.len(),
        codec = config.recording.codec,
        segment_secs = config.recording.segment_duration_secs,
        phash_threshold = config.filter.phash_threshold,
        rustfs_endpoint = config.rustfs.endpoint,
        "starting frame-bucket pipeline"
    );

    // Check ffmpeg availability (encoding will fail without it).
    recorder::encoder::check_ffmpeg_available().await;

    // Initialize shared RustFS storage
    let rustfs_storage = Arc::new(storage::RustfsStorage::new(&config.rustfs).await);
    if let Err(e) = rustfs_storage.ensure_bucket().await {
        error!(error = %e, "failed to ensure RustFS bucket exists");
        std::process::exit(1);
    }

    // Spawn shared eviction background task
    let eviction_storage = Arc::clone(&rustfs_storage);
    let eviction_config = config.eviction.clone();
    let aws_config = config.aws_s3.clone();
    let stats_path = std::path::Path::new(&config.database.path).join("storage_stats.json");
    tokio::spawn(async move {
        eviction::run_eviction_loop(eviction_storage, &eviction_config, &aws_config, stats_path)
            .await;
    });

    // Channel for the management API to send commands to the main loop
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<mgmt::Command>(16);

    // Start management HTTP server
    let mgmt_port = config.api.mgmt_port;
    tokio::spawn(mgmt::serve(mgmt_port, config_path.clone(), cmd_tx));

    // Track running robot pipelines by robot_id
    let mut running: HashMap<String, JoinHandle<()>> = HashMap::new();

    // Spawn initial robots
    for robot in &config.robots {
        let handle = spawn_robot_task(
            robot.clone(),
            Arc::clone(&rustfs_storage),
            config.filter.clone(),
            config.recording.clone(),
            config.database.path.clone(),
            config.rustfs.prefix.clone(),
        );
        running.insert(robot.robot_id.clone(), handle);
    }

    info!(
        robots = ?running.keys().collect::<Vec<_>>(),
        "initial robot pipelines started"
    );

    // Event loop: react to management commands
    while let Some(cmd) = cmd_rx.recv().await {
        // Clean up any finished tasks first
        running.retain(|id, handle| {
            if handle.is_finished() {
                warn!(robot_id = %id, "robot pipeline has exited, removing from tracking");
                false
            } else {
                true
            }
        });

        match cmd {
            mgmt::Command::AddRobot(robot, reply) => {
                if running.contains_key(&robot.robot_id) {
                    let _ = reply.send(Err(format!(
                        "robot '{}' is already running",
                        robot.robot_id
                    )));
                } else {
                    info!(robot_id = robot.robot_id, "spawning new robot pipeline");
                    let handle = spawn_robot_task(
                        robot.clone(),
                        Arc::clone(&rustfs_storage),
                        config.filter.clone(),
                        config.recording.clone(),
                        config.database.path.clone(),
                        config.rustfs.prefix.clone(),
                    );
                    running.insert(robot.robot_id.clone(), handle);
                    let _ = reply.send(Ok(()));
                }
            }
            mgmt::Command::RemoveRobot(robot_id, reply) => {
                if let Some(handle) = running.remove(&robot_id) {
                    info!(robot_id, "aborting robot pipeline");
                    handle.abort();
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(format!("robot '{}' is not running", robot_id)));
                }
            }
        }
    }
}

fn spawn_robot_task(
    robot: RobotConfig,
    rustfs: Arc<storage::RustfsStorage>,
    filter_config: frame_bucket_common::config::FilterConfig,
    recording_config: frame_bucket_common::config::RecordingConfig,
    db_path: String,
    rustfs_prefix: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_robot_pipeline(robot, rustfs, filter_config, recording_config, db_path, rustfs_prefix)
            .await;
    })
}

async fn run_robot_pipeline(
    robot: RobotConfig,
    rustfs: Arc<storage::RustfsStorage>,
    filter_config: frame_bucket_common::config::FilterConfig,
    recording_config: frame_bucket_common::config::RecordingConfig,
    db_path: String,
    rustfs_prefix: String,
) {
    let robot_id = robot.robot_id.clone();
    info!(robot_id, mode = robot.mode, stream_url = robot.stream_url, "starting robot pipeline");

    // Per-robot SQLite database
    let db_dir = std::path::Path::new(&db_path);
    let segment_db = match db::SegmentDb::open(db_dir, &robot_id) {
        Ok(d) => {
            info!(robot_id, "SQLite segment DB opened");
            Some(Arc::new(d))
        }
        Err(e) => {
            error!(robot_id, error = %e, "failed to open SQLite segment DB; metadata will not be persisted");
            None
        }
    };

    // Per-robot recording state machine
    let mut state_machine = RecordingStateMachine::new(
        recording_config,
        filter_config.phash_threshold,
        filter_config.phash_hash_size,
        filter_config.spike_ratio,
        Arc::clone(&rustfs),
        segment_db,
        rustfs_prefix,
        robot_id.clone(),
    );

    // Bounded channel for backpressure between capture and processing
    let (tx, mut rx) = tokio::sync::mpsc::channel::<TimestampedFrame>(64);

    // Spawn capture task based on stream mode
    let capture_robot_id = robot_id.clone();
    let capture_handle = tokio::spawn(async move {
        match robot.mode.as_str() {
            "mjpeg" => {
                let url = format!(
                    "{}?quality={}&fps={}",
                    robot.stream_url, robot.quality, robot.fps
                );
                capture::run_mjpeg_capture(&url, tx, &capture_robot_id).await;
            }
            "polling" => {
                let url = format!(
                    "{}?quality={}",
                    robot.stream_url.replace("/stream", "/frame"),
                    robot.quality
                );
                let interval = Duration::from_secs_f64(1.0 / robot.fps);
                capture::run_polling_capture(&url, tx, interval, &capture_robot_id).await;
            }
            "h264" => {
                let addr = robot.h264_url.as_deref().unwrap_or_else(|| {
                    error!(robot_id = capture_robot_id, "h264_url is required when mode = \"h264\"");
                    std::process::exit(1);
                });
                capture::run_h264_capture(addr, tx, &capture_robot_id).await;
            }
            other => {
                error!(robot_id = capture_robot_id, mode = other, "unknown stream mode");
            }
        }
    });

    // Process frames from channel
    let mut total: u64 = 0;
    while let Some(frame) = rx.recv().await {
        total += 1;
        if total % 100 == 0 {
            debug!(robot_id, total, "frames processed");
        }
        state_machine.process_frame(&frame).await;
    }

    warn!(robot_id, "capture channel closed, pipeline stopping");
    capture_handle.abort();
}
