use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tracing::{debug, error, info, warn};

pub struct SegmentEncoder {
    child: Child,
    stdin: ChildStdin,
    output_path: PathBuf,
    frame_count: u32,
    pub start_ms: i64,
}

pub struct FinishedSegment {
    pub mp4_bytes: Vec<u8>,
    pub frame_count: u32,
    #[allow(dead_code)]
    pub start_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum EncoderError {
    #[error("failed to spawn ffmpeg: {0}")]
    Spawn(String),
    #[error("failed to write frame to ffmpeg stdin: {0}")]
    Write(String),
    #[error("failed to wait for ffmpeg: {0}")]
    Wait(String),
    #[error("ffmpeg exited with non-zero status: {0}")]
    FfmpegFailed(String),
    #[error("failed to read ffmpeg output file: {0}")]
    ReadOutput(String),
}

impl SegmentEncoder {
    /// Spawn an ffmpeg subprocess ready to receive MJPEG frames on stdin.
    /// The output MP4 is written to a temp file at /tmp/segment_{start_ms}.mp4.
    pub async fn start(
        start_ms: i64,
        codec: &str,
        crf: u32,
        preset: &str,
        fps: f64,
    ) -> Result<Self, EncoderError> {
        let output_path = std::env::temp_dir().join(format!("segment_{start_ms}.mp4"));

        let vcodec = match codec {
            "h265" => "libx265",
            _ => "libx264",
        };

        let fps_str = fps.to_string();
        let crf_str = crf.to_string();

        let mut cmd = Command::new("ffmpeg");
        cmd.args([
            "-f", "image2pipe",
            "-vcodec", "mjpeg",
            "-r", &fps_str,
            "-i", "pipe:0",
            "-c:v", vcodec,
            "-preset", preset,
            "-crf", &crf_str,
            "-movflags", "+faststart",
            "-y",
            output_path.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| EncoderError::Spawn(e.to_string()))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| EncoderError::Spawn("could not get stdin handle".into()))?;

        debug!(
            codec = vcodec,
            crf,
            preset,
            fps,
            output = output_path.display().to_string(),
            "ffmpeg encoder started"
        );

        Ok(Self {
            child,
            stdin,
            output_path,
            frame_count: 0,
            start_ms,
        })
    }

    /// Write a single JPEG frame to ffmpeg's stdin pipe.
    pub async fn push_frame(&mut self, jpeg_data: &[u8]) -> Result<(), EncoderError> {
        self.stdin
            .write_all(jpeg_data)
            .await
            .map_err(|e| EncoderError::Write(e.to_string()))?;
        self.frame_count += 1;
        debug!(frame_count = self.frame_count, "pushed frame to encoder");
        Ok(())
    }

    /// Finalize the segment: close stdin, wait for ffmpeg to finish, read the output MP4.
    /// Deletes the temp file after reading.
    pub async fn finish(self) -> Result<FinishedSegment, EncoderError> {
        // Close stdin so ffmpeg knows there are no more frames.
        drop(self.stdin);

        let output = self
            .child
            .wait_with_output()
            .await
            .map_err(|e| EncoderError::Wait(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(stderr = %stderr, "ffmpeg exited with error");
            // Clean up temp file on failure
            let _ = tokio::fs::remove_file(&self.output_path).await;
            return Err(EncoderError::FfmpegFailed(stderr.into_owned()));
        }

        let mp4_bytes = tokio::fs::read(&self.output_path)
            .await
            .map_err(|e| EncoderError::ReadOutput(e.to_string()))?;

        // Delete temp file
        if let Err(e) = tokio::fs::remove_file(&self.output_path).await {
            warn!(path = self.output_path.display().to_string(), error = %e, "failed to delete temp segment file");
        }

        info!(
            frame_count = self.frame_count,
            bytes = mp4_bytes.len(),
            start_ms = self.start_ms,
            "segment encoding complete"
        );

        Ok(FinishedSegment {
            mp4_bytes,
            frame_count: self.frame_count,
            start_ms: self.start_ms,
        })
    }

    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }
}

/// Check whether ffmpeg is available on PATH. Logs a warning if not found.
pub async fn check_ffmpeg_available() {
    match Command::new("ffmpeg").arg("-version").output().await {
        Ok(out) if out.status.success() => {
            debug!("ffmpeg is available");
        }
        Ok(_) => {
            warn!("ffmpeg returned non-zero for -version; encoding may fail");
        }
        Err(e) => {
            warn!(
                error = %e,
                "ffmpeg not found on PATH; ACTIVE mode encoding will fail. \
                 Install ffmpeg with libx264/libx265 support."
            );
        }
    }
}
