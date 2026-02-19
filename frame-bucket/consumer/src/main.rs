mod db;
mod eviction;
mod filter;
mod recorder;
mod storage;

use frame_bucket_common::config::Config;
use frame_bucket_common::frame::TimestampedFrame;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use recorder::RecordingStateMachine;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

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

    info!(
        brokers = config.kafka.brokers,
        topic = config.kafka.topic,
        group_id = config.kafka.group_id,
        codec = config.recording.codec,
        segment_secs = config.recording.segment_duration_secs,
        phash_threshold = config.filter.phash_threshold,
        rustfs_endpoint = config.rustfs.endpoint,
        "starting frame-bucket consumer"
    );

    // Check ffmpeg availability (encoding will fail without it).
    recorder::encoder::check_ffmpeg_available().await;

    // Initialize RustFS storage
    let rustfs_storage = Arc::new(storage::RustfsStorage::new(&config.rustfs).await);
    if let Err(e) = rustfs_storage.ensure_bucket().await {
        error!(error = %e, "failed to ensure RustFS bucket exists");
        std::process::exit(1);
    }

    // Create Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.brokers)
        .set("group.id", &config.kafka.group_id)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("max.partition.fetch.bytes", "10485760")
        .create()
        .expect("failed to create Kafka consumer");

    consumer
        .subscribe(&[&config.kafka.topic])
        .expect("failed to subscribe to Kafka topic");

    info!(topic = config.kafka.topic, "subscribed to Kafka topic");

    // Open per-robot SQLite database for segment metadata.
    let robot_id = config.aws_s3.robot_id.clone();
    let db_dir = std::path::Path::new(&config.database.path);
    let segment_db = match db::SegmentDb::open(db_dir, &robot_id) {
        Ok(d) => {
            info!(path = config.database.path, robot_id, "SQLite segment DB opened");
            Some(Arc::new(d))
        }
        Err(e) => {
            error!(error = %e, "failed to open SQLite segment DB; metadata will not be persisted");
            None
        }
    };

    // Build the recording state machine.
    let state_machine = RecordingStateMachine::new(
        config.recording.clone(),
        config.filter.phash_threshold,
        config.filter.phash_hash_size,
        Arc::clone(&rustfs_storage),
        segment_db,
        config.rustfs.prefix.clone(),
        robot_id,
    );

    // Spawn eviction background task
    let eviction_storage = Arc::clone(&rustfs_storage);
    let eviction_config = config.eviction.clone();
    let aws_config = config.aws_s3.clone();
    tokio::spawn(async move {
        eviction::run_eviction_loop(eviction_storage, &eviction_config, &aws_config).await;
    });

    // Main consumption loop
    info!("entering main consumption loop");
    run_consumer_loop(consumer, state_machine).await;
}

/// Extract the robot_id from a Kafka message key of the form `{robot_id}:{timestamp_ms}`.
/// Falls back to "unknown" if the key is missing or malformed.
fn robot_id_from_key(key: Option<&[u8]>) -> String {
    key.and_then(|k| std::str::from_utf8(k).ok())
        .and_then(|s| s.splitn(2, ':').next())
        .unwrap_or("unknown")
        .to_string()
}

async fn run_consumer_loop(
    consumer: StreamConsumer,
    mut state_machine: RecordingStateMachine,
) {
    use futures_util::StreamExt;
    let mut stream = consumer.stream();
    let mut total: u64 = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                let _robot_id = robot_id_from_key(msg.key());

                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        debug!("empty Kafka message, skipping");
                        continue;
                    }
                };

                let frame = match TimestampedFrame::deserialize(payload) {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize frame, skipping");
                        continue;
                    }
                };

                total += 1;
                if total % 100 == 0 {
                    debug!(total, "frames processed");
                }

                state_machine.process_frame(&frame).await;
            }
            Err(e) => {
                warn!(error = %e, "Kafka consume error");
            }
        }
    }
}
