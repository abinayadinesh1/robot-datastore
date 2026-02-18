mod eviction;
mod filter;
mod storage;

use filter::phash::PHashFilter;
use filter::traits::FrameFilter;
use frame_bucket_common::config::Config;
use frame_bucket_common::frame::TimestampedFrame;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
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
        filter = config.filter.primary,
        rustfs_endpoint = config.rustfs.endpoint,
        "starting frame-bucket consumer"
    );

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

    // Create filter
    let frame_filter: Arc<Mutex<Box<dyn FrameFilter>>> = match config.filter.primary.as_str() {
        "phash" => Arc::new(Mutex::new(Box::new(PHashFilter::new(
            config.filter.phash_hash_size,
            config.filter.phash_threshold,
        )))),
        "histogram" => Arc::new(Mutex::new(Box::new(
            filter::histogram::HistogramFilter::new(config.filter.histogram_threshold),
        ))),
        other => {
            error!(filter = other, "unknown filter type");
            std::process::exit(1);
        }
    };

    // Spawn eviction background task
    let eviction_storage = Arc::clone(&rustfs_storage);
    let eviction_config = config.eviction.clone();
    let aws_config = config.aws_s3.clone();
    tokio::spawn(async move {
        eviction::run_eviction_loop(eviction_storage, &eviction_config, &aws_config).await;
    });

    // Main consumption loop
    info!("entering main consumption loop");
    run_consumer_loop(consumer, frame_filter, rustfs_storage, &config.rustfs.prefix).await;
}

async fn run_consumer_loop(
    consumer: StreamConsumer,
    filter: Arc<Mutex<Box<dyn FrameFilter>>>,
    storage: Arc<storage::RustfsStorage>,
    prefix: &str,
) {
    use futures_util::StreamExt;
    let mut stream = consumer.stream();
    let mut accepted: u64 = 0;
    let mut rejected: u64 = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
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

                // Run filter in a blocking task to avoid blocking the async runtime
                let jpeg_data = frame.jpeg_data.clone();
                let filter_clone = Arc::clone(&filter);
                let should_store = tokio::task::spawn_blocking(move || {
                    let mut f = filter_clone.blocking_lock();
                    f.should_store(&jpeg_data)
                })
                .await
                .unwrap_or(false);

                if should_store {
                    let object_key = frame.object_key(prefix);
                    match storage
                        .put_frame(&object_key, frame.jpeg_data, frame.captured_at_ms)
                        .await
                    {
                        Ok(()) => {
                            accepted += 1;
                            debug!(
                                key = object_key,
                                accepted,
                                rejected,
                                "frame stored"
                            );
                        }
                        Err(e) => {
                            error!(error = %e, key = object_key, "failed to store frame");
                        }
                    }
                } else {
                    rejected += 1;
                    if rejected % 100 == 0 {
                        debug!(accepted, rejected, "frame filter stats");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Kafka consume error");
            }
        }
    }
}
