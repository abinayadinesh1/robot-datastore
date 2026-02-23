use tracing::debug;

/// Frame-size heuristic filter for H.264 streams.
///
/// In a static scene, H.264 P-frames are tiny because the encoder says
/// "nothing changed" (small residuals). When motion occurs, P-frames grow
/// significantly because residuals are large. This lets us detect activity
/// without decoding any pixels.
///
/// IDR keyframes are always large and are excluded from the EMA calculation
/// (they don't carry motion information — they're full intra-coded frames).
pub struct FrameSizeFilter {
    /// Exponential moving average of recent P-frame sizes.
    avg_p_frame_size: f64,
    /// EMA smoothing factor (0..1). Lower = smoother/slower adaptation.
    alpha: f64,
    /// A P-frame is "active" if `size > spike_ratio * avg_p_frame_size`.
    spike_ratio: f64,
    /// Total frames seen (for warmup period).
    frames_seen: u64,
    /// Number of frames before the filter starts rejecting (warmup).
    warmup_frames: u64,
}

impl FrameSizeFilter {
    pub fn new(spike_ratio: f64) -> Self {
        Self {
            avg_p_frame_size: 0.0,
            alpha: 0.05,
            spike_ratio,
            frames_seen: 0,
            warmup_frames: 30,
        }
    }

    /// Returns `true` if the frame indicates scene activity (motion).
    ///
    /// - IDR keyframes (nal_type == 5) always return `true` — they mark a
    ///   new GOP and should be stored as segment boundaries.
    /// - P-frames (nal_type == 1) return `true` if their size exceeds
    ///   `spike_ratio * EMA(p_frame_sizes)`.
    /// - During warmup, all frames are accepted while the EMA stabilizes.
    pub fn is_active(&mut self, frame_size: usize, nal_type: u8) -> bool {
        self.frames_seen += 1;

        // IDR keyframes are always significant
        if nal_type == 5 {
            debug!(frame_size, nal_type, "keyframe — always active");
            return true;
        }

        // During warmup, accept everything and build up the EMA
        if self.frames_seen <= self.warmup_frames {
            self.update_ema(frame_size);
            debug!(
                frame_size,
                ema = self.avg_p_frame_size,
                frames_seen = self.frames_seen,
                "warmup — accepting"
            );
            return true;
        }

        let is_spike = self.avg_p_frame_size > 0.0
            && (frame_size as f64) > self.spike_ratio * self.avg_p_frame_size;

        debug!(
            frame_size,
            ema = format!("{:.0}", self.avg_p_frame_size),
            threshold = format!("{:.0}", self.spike_ratio * self.avg_p_frame_size),
            is_spike,
            "P-frame size check"
        );

        // Always update the EMA (even for spikes, so we adapt to new baselines)
        self.update_ema(frame_size);

        is_spike
    }

    /// Returns `true` if the P-frame is small relative to the EMA,
    /// indicating the scene is static. Used for ACTIVE→IDLE transitions.
    pub fn is_quiet(&self, frame_size: usize) -> bool {
        if self.avg_p_frame_size <= 0.0 {
            return false;
        }
        (frame_size as f64) <= self.spike_ratio * self.avg_p_frame_size
    }

    fn update_ema(&mut self, frame_size: usize) {
        let size = frame_size as f64;
        if self.avg_p_frame_size == 0.0 {
            self.avg_p_frame_size = size;
        } else {
            self.avg_p_frame_size = self.alpha * size + (1.0 - self.alpha) * self.avg_p_frame_size;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_accepts_all() {
        let mut filter = FrameSizeFilter::new(4.0);
        // During warmup, everything is accepted
        for i in 0..30 {
            assert!(filter.is_active(1000, 1), "frame {} should be accepted during warmup", i);
        }
    }

    #[test]
    fn idr_always_active() {
        let mut filter = FrameSizeFilter::new(4.0);
        // Even after warmup, IDR frames are always active
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        assert!(filter.is_active(1000, 5));
    }

    #[test]
    fn spike_detected() {
        let mut filter = FrameSizeFilter::new(4.0);
        // Warmup with small P-frames
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // EMA should be ~1000. A 5000-byte frame is a 5x spike (> 4.0 ratio)
        assert!(filter.is_active(5000, 1));
    }

    #[test]
    fn small_frame_not_active() {
        let mut filter = FrameSizeFilter::new(4.0);
        // Warmup
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // A frame roughly the same size as EMA is not a spike
        assert!(!filter.is_active(1200, 1));
    }

    #[test]
    fn is_quiet_check() {
        let mut filter = FrameSizeFilter::new(4.0);
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        assert!(filter.is_quiet(1000));
        assert!(!filter.is_quiet(5000));
    }
}
