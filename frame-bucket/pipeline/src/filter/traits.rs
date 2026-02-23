/// Lambda-like filter interface for frame scene-change detection.
///
/// Implementations receive raw JPEG bytes and decide whether the frame
/// represents a meaningful change from the previous scene.
pub trait FrameFilter: Send + Sync {
    /// Returns `true` if this frame should be stored (scene changed).
    /// Returns `false` to skip (scene unchanged).
    fn should_store(&mut self, jpeg_data: &[u8]) -> bool;

    /// Human-readable name for logging.
    fn name(&self) -> &str {
        "unnamed"
    }
}
