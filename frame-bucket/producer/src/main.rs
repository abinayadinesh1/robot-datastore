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
    #[error("config error: {0}")]
    Config(#[from] frame_bucket_common::config::ConfigError),
}

#[tokio::main]
async fn main() {
    // Optional robot_id override: `frame-bucket-producer [robot_id]`
    let robot_id_arg = std::env::args().nth(1);

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

    info!(
        brokers = config.kafka.brokers,
        topic = config.kafka.topic,
        mode = config.stream.mode,
        robot_id,
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
                config.stream.url, config.stream.quality, config.stream.fps
            );
            mjpeg::run_mjpeg_producer(&url, &config.kafka.topic, &producer, robot_id).await.ok();
        }
        "polling" => {
            let url = format!(
                "{}?quality={}",
                config.stream.url.replace("/stream", "/frame"),
                config.stream.quality
            );
            let interval = Duration::from_secs_f64(1.0 / config.stream.fps);
            mjpeg::run_polling_producer(&url, &config.kafka.topic, &producer, interval, robot_id)
                .await
                .ok();
        }
        other => {
            error!(mode = other, "unknown stream mode, expected 'mjpeg' or 'polling'");
            std::process::exit(1);
        }
    }
}
