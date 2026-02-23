use image::ImageReader;
use std::io::Cursor;
use tracing::{debug, warn};

use super::traits::FrameFilter;

const NUM_BINS: usize = 64;
const DOWNSAMPLE_SIZE: u32 = 64;

/// Histogram-based scene change filter.
///
/// Downsamples to 64x64 grayscale, computes a 64-bin histogram,
/// then compares via chi-squared distance.
pub struct HistogramFilter {
    last_histogram: Option<[f64; NUM_BINS]>,
    threshold: f64,
}

impl HistogramFilter {
    pub fn new(threshold: f64) -> Self {
        Self {
            last_histogram: None,
            threshold,
        }
    }

    fn compute_histogram(jpeg_data: &[u8]) -> Option<[f64; NUM_BINS]> {
        let img = ImageReader::new(Cursor::new(jpeg_data))
            .with_guessed_format()
            .ok()?
            .decode()
            .ok()?;

        let gray = img
            .resize_exact(
                DOWNSAMPLE_SIZE,
                DOWNSAMPLE_SIZE,
                image::imageops::FilterType::Nearest,
            )
            .to_luma8();

        let mut bins = [0u64; NUM_BINS];
        let total_pixels = gray.pixels().len() as f64;

        for pixel in gray.pixels() {
            let bin = (pixel.0[0] as usize * NUM_BINS) / 256;
            bins[bin.min(NUM_BINS - 1)] += 1;
        }

        // Normalize
        let mut hist = [0.0f64; NUM_BINS];
        for (i, &count) in bins.iter().enumerate() {
            hist[i] = count as f64 / total_pixels;
        }
        Some(hist)
    }

    /// Chi-squared distance between two histograms.
    fn chi_squared(a: &[f64; NUM_BINS], b: &[f64; NUM_BINS]) -> f64 {
        let mut sum = 0.0;
        for i in 0..NUM_BINS {
            let denom = a[i] + b[i];
            if denom > 1e-10 {
                let diff = a[i] - b[i];
                sum += (diff * diff) / denom;
            }
        }
        sum
    }
}

impl FrameFilter for HistogramFilter {
    fn should_store(&mut self, jpeg_data: &[u8]) -> bool {
        let hist = match Self::compute_histogram(jpeg_data) {
            Some(h) => h,
            None => {
                warn!("failed to compute histogram, skipping frame");
                return false;
            }
        };

        match &self.last_histogram {
            None => {
                debug!("first frame, accepting unconditionally");
                self.last_histogram = Some(hist);
                true
            }
            Some(prev) => {
                let distance = Self::chi_squared(prev, &hist);
                let accepted = distance > self.threshold;
                debug!(
                    distance = format!("{:.4}", distance),
                    threshold = format!("{:.4}", self.threshold),
                    accepted,
                    "histogram comparison"
                );
                if accepted {
                    self.last_histogram = Some(hist);
                }
                accepted
            }
        }
    }

    fn name(&self) -> &str {
        "histogram"
    }
}
