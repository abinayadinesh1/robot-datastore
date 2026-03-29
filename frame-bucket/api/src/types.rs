use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Segments
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Segment {
    pub id: i64,
    pub robot_id: String,
    #[serde(rename = "type")]
    pub segment_type: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub s3_key: String,
    pub size_bytes: Option<i64>,
    pub frame_count: Option<i64>,
    pub labels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SegmentQuery {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    #[serde(rename = "type")]
    pub segment_type: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BulkDeleteQuery {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Deserialize)]
pub struct PatchLabels {
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkDeleteResponse {
    pub deleted: usize,
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CollectionResponse {
    pub id: i64,
    pub robot_id: String,
    pub name: String,
    pub description: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub clip_count: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateCollection {
    pub name: String,
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Clips
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ClipResponse {
    pub id: i64,
    pub collection_id: i64,
    pub robot_id: String,
    pub modality: String,
    pub clip_start_ms: i64,
    pub clip_end_ms: i64,
    pub segment_ids: Vec<i64>,
    pub manifest_s3_key: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateClip {
    pub clip_start_ms: i64,
    pub clip_end_ms: i64,
    pub segment_ids: Vec<i64>,
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct DownloadInfo {
    pub total_bytes: i64,
    pub clip_count: i64,
}

// ---------------------------------------------------------------------------
// Timeline
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TimelineResponse {
    pub segments: Vec<Segment>,
    pub time_bounds: TimeBounds,
}

#[derive(Debug, Serialize)]
pub struct TimeBounds {
    pub earliest_ms: Option<i64>,
    pub latest_ms: Option<i64>,
}

// ---------------------------------------------------------------------------
// Internal: segment metadata used across clips / download / stitch
// ---------------------------------------------------------------------------

pub struct SegmentInfo {
    pub segment_id: i64,
    pub segment_type: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub source_key: String,
    pub size_bytes: Option<i64>,
}

// ---------------------------------------------------------------------------
// Sampled keyframes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SampledKeyframesQuery {
    pub start_ms: i64,
    pub end_ms: i64,
    /// Sampling interval in milliseconds (e.g. 1000 = one frame per second).
    pub interval_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SampledKeyframesResponse {
    pub robot_id: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub interval_ms: i64,
    pub frames: Vec<KeyframePath>,
}

#[derive(Debug, Serialize)]
pub struct KeyframePath {
    /// Absolute path to the extracted JPEG on disk.
    pub path: String,
    /// Absolute timestamp in ms (epoch) for this frame.
    pub timestamp_ms: i64,
    /// The segment this frame was extracted from.
    pub segment_id: i64,
}

// ---------------------------------------------------------------------------
// Download / stitch
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct DownloadQuery {
    pub format: Option<String>, // "zip" or "mp4"
}

#[derive(Debug)]
pub enum StitchError {
    Db(String),
    S3Download(String),
    TempIo(String),
    FfmpegSpawn(String),
    FfmpegFailed(String),
    NoContent,
}

impl std::fmt::Display for StitchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StitchError::Db(e) => write!(f, "database error: {e}"),
            StitchError::S3Download(e) => write!(f, "S3 download error: {e}"),
            StitchError::TempIo(e) => write!(f, "temp file I/O error: {e}"),
            StitchError::FfmpegSpawn(e) => write!(f, "failed to spawn ffmpeg: {e}"),
            StitchError::FfmpegFailed(e) => write!(f, "ffmpeg failed: {e}"),
            StitchError::NoContent => write!(f, "no downloadable content"),
        }
    }
}

impl IntoResponse for StitchError {
    fn into_response(self) -> Response {
        let status = match &self {
            StitchError::NoContent => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
