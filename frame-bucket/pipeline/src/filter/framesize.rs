use tracing::{debug, info};

pub struct FrameSizeFilter {
    baseline_ema: f64,
    alpha: f64,
    spike_ratio: f64,
    quiet_ratio: f64,
    frames_seen: u64,
    warmup_frames: u64,
}

impl FrameSizeFilter {
    pub fn new(spike_ratio: f64, quiet_ratio: f64) -> Self {
        Self {
            baseline_ema: 0.0,
            alpha: 0.05,
            spike_ratio,
            quiet_ratio,
            frames_seen: 0,
            warmup_frames: 30,
        }
    }

    pub fn is_active(&mut self, frame_size: usize, nal_type: u8) -> bool {
        self.frames_seen += 1;

        let nal_label = match nal_type {
            1 => "P/B",
            5 => "IDR",
            _ => "other",
        };
        if nal_type == 1 {
            info!(
                frame_size,
                nal_type,
                nal_label,
                ema = format!("{:.0}", self.baseline_ema),
                "raw frame"
            );
        }

        if nal_type == 5 {
            return true;
        }

        if self.frames_seen <= self.warmup_frames {
            self.update_ema(frame_size);
            debug!(
                frame_size,
                ema = self.baseline_ema,
                frames_seen = self.frames_seen,
                "warmup — accepting"
            );
            return true;
        }

        let is_spike = self.baseline_ema > 0.0
            && (frame_size as f64) > self.spike_ratio * self.baseline_ema;

        debug!(
            frame_size,
            ema = format!("{:.0}", self.baseline_ema),
            threshold = format!("{:.0}", self.spike_ratio * self.baseline_ema),
            is_spike,
            "P-frame size check"
        );

        if !is_spike {
            self.update_ema(frame_size);
        }

        is_spike
    }

    pub fn is_quiet(&self, frame_size: usize) -> bool {
        if self.baseline_ema <= 0.0 {
            return false;
        }
        (frame_size as f64) <= self.quiet_ratio * self.baseline_ema
    }

    fn update_ema(&mut self, frame_size: usize) {
        let size = frame_size as f64;
        if self.baseline_ema == 0.0 {
            self.baseline_ema = size;
        } else {
            self.baseline_ema = self.alpha * size + (1.0 - self.alpha) * self.baseline_ema;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_accepts_all() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // During warmup, everything is accepted
        for i in 0..30 {
            assert!(filter.is_active(1000, 1), "frame {} should be accepted during warmup", i);
        }
    }

    #[test]
    fn idr_always_active() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // Even after warmup, IDR frames are always active
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        assert!(filter.is_active(1000, 5));
    }

    #[test]
    fn spike_detected() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // Warmup with small P-frames
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // EMA should be ~1000. A 5000-byte frame is a 5x spike (> 4.0 ratio)
        assert!(filter.is_active(5000, 1));
    }

    #[test]
    fn small_frame_not_active() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // Warmup
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // A frame roughly the same size as EMA is not a spike
        assert!(!filter.is_active(1200, 1));
    }

    #[test]
    fn is_quiet_check() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // EMA ~1000, quiet_ratio 1.5 → threshold 1500
        assert!(filter.is_quiet(1000));   // well below 1500
        assert!(filter.is_quiet(1400));   // still below 1500
        assert!(!filter.is_quiet(1600));  // above 1500
        assert!(!filter.is_quiet(5000));  // way above
    }

    #[test]
    fn spike_does_not_inflate_baseline() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // Warmup with small P-frames → EMA ≈ 1000
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        let ema_before = filter.baseline_ema;

        // A spike should NOT move the baseline
        assert!(filter.is_active(5000, 1));
        assert_eq!(filter.baseline_ema, ema_before, "spike should not change baseline EMA");

        // A non-spike SHOULD move the baseline
        filter.is_active(1100, 1);
        assert!(filter.baseline_ema > ema_before, "non-spike should update baseline EMA");
    }

    #[test]
    fn consecutive_spikes_stay_active() {
        let mut filter = FrameSizeFilter::new(4.0, 1.5);
        // Warmup
        for _ in 0..31 {
            filter.is_active(1000, 1);
        }
        // Multiple motion frames in a row should all register as spikes
        // because the baseline doesn't get inflated
        for _ in 0..10 {
            assert!(filter.is_active(5000, 1), "consecutive spike should stay active");
        }
    }
}
