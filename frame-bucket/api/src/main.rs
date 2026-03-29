use std::path::PathBuf;
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_types::region::Region;
use axum::http::header;
use axum::routing::{delete, get};
use axum::Router;
use frame_bucket_common::config::Config;
use tokio::sync::Semaphore;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub mod db;
pub mod handlers;
pub mod llm_client;
pub mod qa_handler;
pub mod qa_tree;
pub mod s3;
pub mod types;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

pub struct AppState {
    pub db_dir: PathBuf,
    #[allow(dead_code)]
    pub rustfs_public_url: String,
    pub rustfs_bucket: String,
    pub s3_client: aws_sdk_s3::Client,
    pub labelled_data_bucket: String,
    pub health_file_path: PathBuf,
    pub aws_s3_client: aws_sdk_s3::Client,
    pub aws_s3_bucket: String,
    pub aws_s3_prefix: String,
    pub robot_stream_urls: Vec<(String, String)>, // (robot_id, stream_url)
    pub stitch_semaphore: Arc<Semaphore>,
    pub llm_client: llm_client::LlmClient,
    pub qa_semaphore: Arc<Semaphore>,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

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

    let creds = Credentials::new(
        &config.rustfs.access_key,
        &config.rustfs.secret_key,
        None,
        None,
        "static",
    );
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&config.rustfs.endpoint)
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    ensure_bucket(&s3_client, &config.api.labelled_data_bucket).await;

    let aws_sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(config.aws_s3.region.clone()))
        .load()
        .await;
    let aws_s3_client = aws_sdk_s3::Client::new(&aws_sdk_config);

    let robot_stream_urls: Vec<(String, String)> = config
        .robots
        .iter()
        .map(|r| (r.robot_id.clone(), r.stream_url.clone()))
        .collect();

    let state = Arc::new(AppState {
        db_dir: PathBuf::from(&config.database.path),
        rustfs_public_url: config.api.rustfs_public_url.clone(),
        rustfs_bucket: config.api.rustfs_bucket.clone(),
        s3_client,
        labelled_data_bucket: config.api.labelled_data_bucket.clone(),
        health_file_path: PathBuf::from(&config.database.path).join("storage_stats.json"),
        aws_s3_client,
        aws_s3_bucket: config.aws_s3.bucket.clone(),
        aws_s3_prefix: config.aws_s3.prefix.clone(),
        robot_stream_urls,
        stitch_semaphore: Arc::new(Semaphore::new(2)),
        llm_client: llm_client::LlmClient::new(
            std::env::var("LLM_BASE_URL").unwrap_or_else(|_| {
                "https://abinayadinesh1--example-vllm-inference-serve.modal.run".to_string()
            }),
            std::env::var("LLM_MODEL").unwrap_or_else(|_| "llm".to_string()),
            std::env::var("VIDEO_LLM_BASE_URL").unwrap_or_else(|_| {
                "https://abinayadinesh1--videochat-flash-inference-serve.modal.run".to_string()
            }),
        ),
        qa_semaphore: Arc::new(Semaphore::new(4)),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
        .expose_headers([
            header::CONTENT_LENGTH,
            header::CONTENT_DISPOSITION,
            header::CONTENT_TYPE,
            "X-Missing-Segments".parse().unwrap(),
        ]);

    use handlers::{
        clips, collections, download, health, keyframes, questions, segments, timeline,
    };

    let app = Router::new()
        // Segments
        .route("/robots", get(segments::list_robots))
        .route(
            "/robots/:robot_id/segments",
            get(segments::list_segments).delete(segments::bulk_delete_segments),
        )
        .route(
            "/robots/:robot_id/segments/:id",
            get(segments::get_segment)
                .patch(segments::patch_labels)
                .delete(segments::delete_segment),
        )
        .route(
            "/robots/:robot_id/segments/:id/video",
            get(segments::video_redirect),
        )
        .route(
            "/robots/:robot_id/segments/:id/image",
            get(segments::image_proxy),
        )
        // Sampled keyframes
        .route(
            "/robots/:robot_id/sampled-keyframes",
            get(keyframes::get_sampled_keyframes),
        )
        // Timeline
        .route("/robots/:robot_id/timeline", get(timeline::get_timeline))
        .route(
            "/robots/:robot_id/active-dates",
            get(timeline::get_active_dates),
        )
        // Collections
        .route(
            "/robots/:robot_id/collections",
            get(collections::list_collections).post(collections::create_collection),
        )
        .route(
            "/robots/:robot_id/collections/:id",
            get(collections::get_collection).delete(collections::delete_collection),
        )
        // Clips
        .route(
            "/robots/:robot_id/collections/:collection_id/clips",
            get(clips::list_clips).post(clips::create_clip),
        )
        .route(
            "/robots/:robot_id/collections/:collection_id/clips/:clip_id",
            delete(clips::delete_clip),
        )
        // Download
        .route(
            "/robots/:robot_id/collections/:collection_id/download-info",
            get(clips::download_info),
        )
        .route(
            "/robots/:robot_id/collections/:collection_id/download",
            get(download::download_collection),
        )
        // Questions
        .route(
            "/robots/:robot_id/questions",
            axum::routing::post(questions::submit_question),
        )
        .route(
            "/robots/:robot_id/questions/stream",
            get(qa_handler::question_stream),
        )
        // Health
        .route("/health", get(health::get_health))
        .route("/health/rustfs", get(health::browse_rustfs))
        .route("/health/s3", get(health::browse_s3))
        .route("/health/streams", get(health::check_streams))
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.api.port);
    info!(addr, "frame-bucket API server starting");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {addr}: {e}");
            std::process::exit(1);
        });
    axum::serve(listener, app).await.unwrap();
}

async fn ensure_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    use tracing::warn;
    match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => {
            info!(bucket, "labelled-data bucket exists");
        }
        Err(_) => {
            info!(bucket, "creating labelled-data bucket");
            match client.create_bucket().bucket(bucket).send().await {
                Ok(_) => info!(bucket, "bucket created"),
                Err(e) => warn!(error = %e, bucket, "failed to create bucket"),
            }
        }
    }
}
