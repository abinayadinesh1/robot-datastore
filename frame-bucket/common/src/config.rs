use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub robots: Vec<RobotConfig>,
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
    #[serde(default)]
    pub annotation: Option<AnnotationConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RobotConfig {
    pub robot_id: String,
    pub stream_url: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    /// TCP address for H.264 MPEG-TS stream (e.g., "100.107.96.29:9001").
    /// Required when mode = "h264".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub h264_url: Option<String>,
    #[serde(default = "default_quality")]
    pub quality: u32,
    #[serde(default = "default_fps")]
    pub fps: f64,
    // Viewer fields (optional, used by stream-viewer)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webrtc_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
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
    /// Motion ratio threshold for H.264 P-frame activity detection (MB skip-count).
    /// A P-frame is "active" if motion_ratio > motion_threshold.
    /// motion_ratio = 1.0 - (first_skip_run / total_mbs).
    #[serde(default = "default_motion_threshold", alias = "spike_ratio")]
    pub motion_threshold: f64,
    /// Quiet threshold for H.264 ACTIVE→IDLE detection (MB skip-count).
    /// A P-frame is "quiet" if motion_ratio < quiet_threshold.
    /// Should be lower than motion_threshold to create a dead-zone that prevents jitter.
    #[serde(default = "default_quiet_threshold", alias = "quiet_ratio")]
    pub quiet_threshold: f64,
}

/// Shared, atomically-updated filter parameters that running pipelines read
/// on every frame. The management API writes new values; no restart required.
pub struct LiveFilterParams {
    pub phash_threshold: AtomicU32,
    pub phash_hash_size: AtomicU32,
    pub motion_threshold: AtomicU64, // f64 bits via to_bits/from_bits
    pub quiet_threshold: AtomicU64,
    pub histogram_threshold: AtomicU64,
}

impl LiveFilterParams {
    pub fn from_config(fc: &FilterConfig) -> Arc<Self> {
        Arc::new(Self {
            phash_threshold: AtomicU32::new(fc.phash_threshold),
            phash_hash_size: AtomicU32::new(fc.phash_hash_size),
            motion_threshold: AtomicU64::new(fc.motion_threshold.to_bits()),
            quiet_threshold: AtomicU64::new(fc.quiet_threshold.to_bits()),
            histogram_threshold: AtomicU64::new(fc.histogram_threshold.to_bits()),
        })
    }

    pub fn get_phash_threshold(&self) -> u32 {
        self.phash_threshold.load(Ordering::Relaxed)
    }

    pub fn get_phash_hash_size(&self) -> u32 {
        self.phash_hash_size.load(Ordering::Relaxed)
    }

    pub fn get_motion_threshold(&self) -> f64 {
        f64::from_bits(self.motion_threshold.load(Ordering::Relaxed))
    }

    pub fn get_quiet_threshold(&self) -> f64 {
        f64::from_bits(self.quiet_threshold.load(Ordering::Relaxed))
    }

    pub fn get_histogram_threshold(&self) -> f64 {
        f64::from_bits(self.histogram_threshold.load(Ordering::Relaxed))
    }

    pub fn set_phash_threshold(&self, val: u32) {
        self.phash_threshold.store(val, Ordering::Relaxed);
    }

    pub fn set_phash_hash_size(&self, val: u32) {
        self.phash_hash_size.store(val, Ordering::Relaxed);
    }

    pub fn set_motion_threshold(&self, val: f64) {
        self.motion_threshold.store(val.to_bits(), Ordering::Relaxed);
    }

    pub fn set_quiet_threshold(&self, val: f64) {
        self.quiet_threshold.store(val.to_bits(), Ordering::Relaxed);
    }

    pub fn set_histogram_threshold(&self, val: f64) {
        self.histogram_threshold.store(val.to_bits(), Ordering::Relaxed);
    }

    /// Snapshot current values as a serializable struct.
    pub fn snapshot(&self) -> FilterConfigSnapshot {
        FilterConfigSnapshot {
            phash_threshold: self.get_phash_threshold(),
            phash_hash_size: self.get_phash_hash_size(),
            motion_threshold: self.get_motion_threshold(),
            quiet_threshold: self.get_quiet_threshold(),
            histogram_threshold: self.get_histogram_threshold(),
        }
    }
}

impl std::fmt::Debug for LiveFilterParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveFilterParams")
            .field("phash_threshold", &self.get_phash_threshold())
            .field("phash_hash_size", &self.get_phash_hash_size())
            .field("motion_threshold", &self.get_motion_threshold())
            .field("quiet_threshold", &self.get_quiet_threshold())
            .field("histogram_threshold", &self.get_histogram_threshold())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfigSnapshot {
    pub phash_threshold: u32,
    pub phash_hash_size: u32,
    pub motion_threshold: f64,
    pub quiet_threshold: f64,
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
    /// How many consecutive S3 upload failures before switching to delete-only mode.
    #[serde(default = "default_fallback_after_failures")]
    pub fallback_after_failures: u32,
    /// Seconds to wait in fallback mode before retrying S3.
    #[serde(default = "default_fallback_retry_secs")]
    pub fallback_retry_secs: u64,
    /// In fallback mode, only delete locally when storage exceeds this (GB).
    /// Should be higher than threshold_gb to keep data locally as long as possible.
    /// Defaults to 0, meaning "use threshold_gb" (same as normal eviction).
    #[serde(default)]
    pub fallback_threshold_gb: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AwsS3Config {
    pub bucket: String,
    #[serde(default = "default_aws_prefix")]
    pub prefix: String,
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
    #[error("failed to write config file {0}: {1}")]
    WriteFile(String, std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(String),
}

/// Append a new `[[robots]]` entry to the config file, preserving existing
/// comments and formatting via `toml_edit`.
pub fn append_robot_to_config(path: &Path, robot: &RobotConfig) -> Result<(), ConfigError> {
    use toml_edit::{value, ArrayOfTables, DocumentMut, Item, Table};

    let content = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::ReadFile(path.display().to_string(), e))?;
    let mut doc: DocumentMut = content
        .parse()
        .map_err(|e: toml_edit::TomlError| ConfigError::Parse(e.to_string()))?;

    let mut table = Table::new();
    table["robot_id"] = value(&robot.robot_id);
    table["stream_url"] = value(&robot.stream_url);
    table["mode"] = value(&robot.mode);
    if let Some(ref h264) = robot.h264_url {
        table["h264_url"] = value(h264);
    }
    table["fps"] = value(robot.fps);
    table["quality"] = value(robot.quality as i64);
    if let Some(ref label) = robot.label {
        table["label"] = value(label);
    }
    if let Some(ref webrtc) = robot.webrtc_url {
        table["webrtc_url"] = value(webrtc);
    }
    if let Some(ref peer) = robot.peer_id {
        table["peer_id"] = value(peer);
    }

    let robots = doc
        .entry("robots")
        .or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));
    if let Item::ArrayOfTables(arr) = robots {
        arr.push(table);
    }

    std::fs::write(path, doc.to_string())
        .map_err(|e| ConfigError::WriteFile(path.display().to_string(), e))?;
    Ok(())
}

/// Remove the `[[robots]]` entry with the given `robot_id` from the config file.
/// Returns `true` if an entry was removed.
pub fn remove_robot_from_config(path: &Path, robot_id: &str) -> Result<bool, ConfigError> {
    use toml_edit::{DocumentMut, Item};

    let content = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::ReadFile(path.display().to_string(), e))?;
    let mut doc: DocumentMut = content
        .parse()
        .map_err(|e: toml_edit::TomlError| ConfigError::Parse(e.to_string()))?;

    let removed = if let Some(Item::ArrayOfTables(arr)) = doc.get_mut("robots") {
        let before = arr.len();
        arr.retain(|t| {
            t.get("robot_id")
                .and_then(|v| v.as_str())
                .map_or(true, |id| id != robot_id)
        });
        arr.len() < before
    } else {
        false
    };

    if removed {
        std::fs::write(path, doc.to_string())
            .map_err(|e| ConfigError::WriteFile(path.display().to_string(), e))?;
    }
    Ok(removed)
}

// Default value functions
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
fn default_motion_threshold() -> f64 {
    0.05
}
fn default_quiet_threshold() -> f64 {
    0.02
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
fn default_fallback_after_failures() -> u32 {
    10
}
fn default_fallback_retry_secs() -> u64 {
    600
}
fn default_aws_prefix() -> String {
    "archive/".into()
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
fn default_mgmt_port() -> u16 {
    8081
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
    #[serde(default = "default_mgmt_port")]
    pub mgmt_port: u16,
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
            mgmt_port: default_mgmt_port(),
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

#[derive(Debug, Clone, Deserialize)]
pub struct AnnotationConfig {
    /// URL of the video annotation API (e.g. Modal VideoChat endpoint).
    pub url: String,
    /// Prompt sent to the video model.
    #[serde(default = "default_annotation_prompt")]
    pub prompt: String,
    /// Max number of frames the model should sample from the video.
    #[serde(default = "default_annotation_max_frames")]
    pub max_num_frames: u32,
}

fn default_annotation_prompt() -> String {
    "Give a detailed description of what goes on in this video.".into()
}

fn default_annotation_max_frames() -> u32 {
    128
}
