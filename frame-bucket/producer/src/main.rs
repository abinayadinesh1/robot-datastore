mod h264;
mod mjpeg;

use frame_bucket_common::config::Config;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error("failed to create Kafka producer: {0}")]
    KafkaCreate(String),
    #[error("HTTP connection failed: {0}")]
    HttpConnect(reqwest::Error),
    #[error("HTTP stream error: {0}")]
    HttpStream(reqwest::Error),
    #[error("HTTP status {0}")]
    HttpStatus(u16),
    #[error("TCP connection failed: {0}")]
    TcpConnect(String),
    #[error("TCP stream error: {0}")]
    TcpStream(String),
    #[error("config error: {0}")]
    Config(#[from] frame_bucket_common::config::ConfigError),
}

#[tokio::main]
async fn main() {
    // Optional args: `frame-bucket-producer [robot_id] [stream_url]`
    let robot_id_arg = std::env::args().nth(1);
    let stream_url_arg = std::env::args().nth(2);

    let config_path = PathBuf::from("config.toml");
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

    let robot_id = robot_id_arg.as_deref().unwrap_or(&config.aws_s3.robot_id);
    let stream_url = stream_url_arg.as_deref().unwrap_or(&config.stream.url);

    info!(
        brokers = config.kafka.brokers,
        topic = config.kafka.topic,
        mode = config.stream.mode,
        robot_id,
        stream_url,
        "starting frame-bucket producer"
    );

    let producer = match mjpeg::create_producer(&config.kafka.brokers, &config.kafka.compression) {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "failed to create Kafka producer");
            std::process::exit(1);
        }
    };

    match config.stream.mode.as_str() {
        "mjpeg" => {
            let url = format!(
                "{}?quality={}&fps={}",
                stream_url, config.stream.quality, config.stream.fps
            );
            mjpeg::run_mjpeg_producer(&url, &config.kafka.topic, &producer, robot_id).await.ok();
        }
        // hitting the /api/camera/frame endpoint instead of keeping an HTTP connection, only good as a fallback
        "polling" => {
            let url = format!(
                "{}?quality={}",
                stream_url.replace("/stream", "/frame"),
                config.stream.quality
            );
            let interval = Duration::from_secs_f64(1.0 / config.stream.fps);
            mjpeg::run_polling_producer(&url, &config.kafka.topic, &producer, interval, robot_id)
                .await
                .ok();
        }
        "h264" => {
            let addr = config.stream.h264_url
                .as_deref()
                .unwrap_or_else(|| {
                    error!("h264_url is required when mode = \"h264\"");
                    std::process::exit(1);
                });
            info!(addr, "using H.264 TCP mode");
            h264::run_h264_producer(addr, &config.kafka.topic, &producer, robot_id).await.ok();
        }
        other => {
            error!(mode = other, "unknown stream mode, expected 'mjpeg', 'polling', or 'h264'");
            std::process::exit(1);
        }
    }
}
