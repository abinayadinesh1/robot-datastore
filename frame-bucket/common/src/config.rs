use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub stream: StreamConfig,
    pub filter: FilterConfig,
    pub rustfs: RustfsConfig,
    pub eviction: EvictionConfig,
    pub aws_s3: AwsS3Config,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub recording: RecordingConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    #[serde(default = "default_topic")]
    pub topic: String,
    #[serde(default = "default_group_id")]
    pub group_id: String,
    #[serde(default = "default_compression")]
    pub compression: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub url: String,
    #[serde(default = "default_quality")]
    pub quality: u32,
    #[serde(default = "default_fps")]
    pub fps: f64,
    #[serde(default = "default_mode")]
    pub mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FilterConfig {
    #[serde(default = "default_filter_primary")]
    pub primary: String,
    #[serde(default = "default_phash_threshold")]
    pub phash_threshold: u32,
    #[serde(default = "default_phash_hash_size")]
    pub phash_hash_size: u32,
    #[serde(default = "default_histogram_threshold")]
    pub histogram_threshold: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RustfsConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_rustfs_bucket")]
    pub bucket: String,
    #[serde(default = "default_rustfs_prefix")]
    pub prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvictionConfig {
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
    #[serde(default = "default_threshold_gb")]
    pub threshold_gb: f64,
    #[serde(default = "default_target_gb")]
    pub target_gb: f64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AwsS3Config {
    pub bucket: String,
    #[serde(default = "default_aws_prefix")]
    pub prefix: String,
    #[serde(default = "default_robot_id")]
    pub robot_id: String,
    #[serde(default = "default_region")]
    pub region: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::ReadFile(path.display().to_string(), e))?;
        let config: Config =
            toml::from_str(&content).map_err(|e| ConfigError::Parse(e.to_string()))?;
        Ok(config)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {0}: {1}")]
    ReadFile(String, std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(String),
}

// Default value functions
fn default_topic() -> String {
    "camera.frames".into()
}
fn default_group_id() -> String {
    "frame-filter-group".into()
}
fn default_compression() -> String {
    "snappy".into()
}
fn default_quality() -> u32 {
    80
}
fn default_fps() -> f64 {
    10.0
}
fn default_mode() -> String {
    "mjpeg".into()
}
fn default_filter_primary() -> String {
    "phash".into()
}
fn default_phash_threshold() -> u32 {
    26
}
fn default_phash_hash_size() -> u32 {
    16
}
fn default_histogram_threshold() -> f64 {
    0.15
}
fn default_rustfs_bucket() -> String {
    "camera-frames".into()
}
fn default_rustfs_prefix() -> String {
    "frames/".into()
}
fn default_check_interval() -> u64 {
    30
}
fn default_threshold_gb() -> f64 {
    50.0
}
fn default_target_gb() -> f64 {
    40.0
}
fn default_batch_size() -> usize {
    50
}
fn default_aws_prefix() -> String {
    "archive/".into()
}
fn default_robot_id() -> String {
    "reachy-001".into()
}
fn default_region() -> String {
    "us-west-2".into()
}
fn default_log_level() -> String {
    "info".into()
}

// Recording defaults
fn default_segment_duration() -> u64 {
    60
}
fn default_codec() -> String {
    "h264".into()
}
fn default_crf() -> u32 {
    23
}
fn default_preset() -> String {
    "fast".into()
}
fn default_recording_fps() -> f64 {
    10.0
}
fn default_active_to_idle() -> u32 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecordingConfig {
    #[serde(default = "default_segment_duration")]
    pub segment_duration_secs: u64,
    #[serde(default = "default_codec")]
    pub codec: String,
    #[serde(default = "default_crf")]
    pub crf: u32,
    #[serde(default = "default_preset")]
    pub preset: String,
    #[serde(default = "default_recording_fps")]
    pub fps: f64,
    #[serde(default = "default_active_to_idle")]
    pub active_to_idle_consecutive_frames: u32,
}

fn default_db_path() -> String {
    "data/".into()
}
fn default_api_port() -> u16 {
    8080
}
fn default_rustfs_public_url() -> String {
    "http://localhost:9000".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self { path: default_db_path() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_rustfs_public_url")]
    pub rustfs_public_url: String,
    #[serde(default = "default_rustfs_bucket")]
    pub rustfs_bucket: String,
    #[serde(default = "default_labelled_data_bucket")]
    pub labelled_data_bucket: String,
}

fn default_labelled_data_bucket() -> String {
    "labelled-data".into()
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            port: default_api_port(),
            rustfs_public_url: default_rustfs_public_url(),
            rustfs_bucket: default_rustfs_bucket(),
            labelled_data_bucket: default_labelled_data_bucket(),
        }
    }
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            segment_duration_secs: default_segment_duration(),
            codec: default_codec(),
            crf: default_crf(),
            preset: default_preset(),
            fps: default_recording_fps(),
            active_to_idle_consecutive_frames: default_active_to_idle(),
        }
    }
}
