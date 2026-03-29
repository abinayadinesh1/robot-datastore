use std::collections::VecDeque;

/// Estimates the actual FPS of an incoming frame stream by tracking
/// inter-frame timestamp deltas over a sliding window.
pub struct FpsEstimator {
    /// Recent frame timestamps (Unix ms), kept in arrival order.
    timestamps: VecDeque<i64>,
    /// Maximum number of timestamps to keep.
    window: usize,
    /// Fallback FPS if we don't have enough samples yet.
    fallback_fps: f64,
}

impl FpsEstimator {
    pub fn new(window: usize, fallback_fps: f64) -> Self {
        Self {
            timestamps: VecDeque::with_capacity(window + 1),
            window,
            fallback_fps,
        }
    }

    /// Record a frame's capture timestamp.
    pub fn push(&mut self, captured_at_ms: i64) {
        self.timestamps.push_back(captured_at_ms);
        if self.timestamps.len() > self.window {
            self.timestamps.pop_front();
        }
    }

    /// Compute the estimated FPS from the sliding window.
    /// Returns the fallback value if fewer than 2 samples are available.
    pub fn fps(&self) -> f64 {
        let len = self.timestamps.len();
        if len < 2 {
            return self.fallback_fps;
        }

        let first = self.timestamps[0];
        let last = self.timestamps[len - 1];
        let span_ms = last - first;

        if span_ms <= 0 {
            return self.fallback_fps;
        }

        // (N-1) intervals span `span_ms` milliseconds
        let fps = (len - 1) as f64 / (span_ms as f64 / 1000.0);

        // Sanity clamp: reject obviously wrong values
        if fps < 1.0 || fps > 120.0 {
            return self.fallback_fps;
        }

        fps
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_fallback_with_no_data() {
        let est = FpsEstimator::new(30, 10.0);
        assert_eq!(est.fps(), 10.0);
    }

    #[test]
    fn returns_fallback_with_one_sample() {
        let mut est = FpsEstimator::new(30, 10.0);
        est.push(1000);
        assert_eq!(est.fps(), 10.0);
    }

    #[test]
    fn computes_correct_fps() {
        let mut est = FpsEstimator::new(100, 10.0);
        // Simulate 15 fps: frames every ~66.67ms
        for i in 0..31 {
            est.push(i * 67); // ~67ms apart
        }
        let fps = est.fps();
        assert!((fps - 15.0).abs() < 1.0, "expected ~15 fps, got {fps}");
    }

    #[test]
    fn sliding_window_drops_old_timestamps() {
        let mut est = FpsEstimator::new(10, 10.0);
        // Push 20 timestamps at 33ms apart (~30fps)
        for i in 0..20 {
            est.push(i * 33);
        }
        assert_eq!(est.timestamps.len(), 10);
        let fps = est.fps();
        assert!((fps - 30.0).abs() < 2.0, "expected ~30 fps, got {fps}");
    }
}
