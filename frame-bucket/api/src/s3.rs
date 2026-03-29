use std::path::{Path, PathBuf};

use tracing::warn;

use crate::types::{ClipResponse, SegmentInfo, StitchError};
use crate::AppState;

/// Download a segment file from S3 (try AWS archive first, fall back to RustFS).
pub async fn download_segment_file(
    state: &AppState,
    s3_key: &str,
    segment_type: &str,
    work_dir: &Path,
    segment_id: i64,
) -> Result<PathBuf, StitchError> {
    let trimmed_key = s3_key.trim_start_matches('/');
    let ext = if segment_type == "active" { "mp4" } else { "jpg" };
    let local_path = work_dir.join(format!("seg_{segment_id}.{ext}"));

    let aws_key = format!("{}{}", state.aws_s3_prefix, trimmed_key);
    let data = match state
        .aws_s3_client
        .get_object()
        .bucket(&state.aws_s3_bucket)
        .key(&aws_key)
        .send()
        .await
    {
        Ok(output) => output
            .body
            .collect()
            .await
            .map_err(|e| StitchError::S3Download(e.to_string()))?,
        Err(_) => {
            state
                .s3_client
                .get_object()
                .bucket(&state.rustfs_bucket)
                .key(trimmed_key)
                .send()
                .await
                .map_err(|e| StitchError::S3Download(format!("not in archive or RustFS: {e}")))?
                .body
                .collect()
                .await
                .map_err(|e| StitchError::S3Download(e.to_string()))?
        }
    };

    tokio::fs::write(&local_path, data.into_bytes())
        .await
        .map_err(|e| StitchError::TempIo(e.to_string()))?;

    Ok(local_path)
}

pub async fn run_ffmpeg(args: &[&str]) -> Result<(), StitchError> {
    let output = tokio::process::Command::new("ffmpeg")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| StitchError::FfmpegSpawn(e.to_string()))?
        .wait_with_output()
        .await
        .map_err(|e| StitchError::FfmpegFailed(e.to_string()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(StitchError::FfmpegFailed(stderr.into_owned()));
    }
    Ok(())
}

/// Returns (width, height, fps) from the first video stream.
pub async fn run_ffprobe(path: &Path) -> Result<(u32, u32, f64), StitchError> {
    let output = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,r_frame_rate",
            "-of",
            "json",
        ])
        .arg(path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .await
        .map_err(|e| StitchError::FfmpegSpawn(format!("ffprobe: {e}")))?;

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| StitchError::FfmpegFailed(format!("ffprobe parse: {e}")))?;

    let stream = json["streams"]
        .get(0)
        .ok_or_else(|| StitchError::FfmpegFailed("no video stream found".into()))?;

    let width = stream["width"].as_u64().unwrap_or(640) as u32;
    let height = stream["height"].as_u64().unwrap_or(480) as u32;

    let fps_str = stream["r_frame_rate"].as_str().unwrap_or("30/1");
    let fps = if let Some((n, d)) = fps_str.split_once('/') {
        let num: f64 = n.parse().unwrap_or(30.0);
        let den: f64 = d.parse().unwrap_or(1.0);
        if den > 0.0 { num / den } else { 30.0 }
    } else {
        fps_str.parse().unwrap_or(30.0)
    };

    Ok((width, height, fps))
}

/// Stitch a single clip's segments into one MP4 file.
pub async fn stitch_clip(
    state: &AppState,
    clip: &ClipResponse,
    segments: &[SegmentInfo],
    work_dir: &Path,
    clip_index: usize,
) -> Result<PathBuf, StitchError> {
    let mut piece_paths: Vec<PathBuf> = Vec::new();
    let mut piece_index: usize = 0;

    let mut width = 640u32;
    let mut height = 480u32;
    let mut fps = 30.0f64;
    let mut first_active_probed = false;

    for seg in segments {
        if seg.segment_type == "idle"
            && (seg.source_key.is_empty() || seg.source_key.starts_with("idle:"))
        {
            continue;
        }

        let raw_path = match download_segment_file(
            state,
            &seg.source_key,
            &seg.segment_type,
            work_dir,
            seg.segment_id,
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                warn!(segment_id = seg.segment_id, error = %e, "skipping unavailable segment");
                continue;
            }
        };

        if seg.segment_type == "active" {
            if !first_active_probed {
                if let Ok((w, h, f)) = run_ffprobe(&raw_path).await {
                    width = w;
                    height = h;
                    fps = f;
                }
                first_active_probed = true;
            }

            let trim_start_ms = if clip.clip_start_ms > seg.start_ms {
                clip.clip_start_ms - seg.start_ms
            } else {
                0
            };
            let seg_duration_ms = seg.end_ms - seg.start_ms;
            let trim_end_ms = if clip.clip_end_ms < seg.end_ms {
                clip.clip_end_ms - seg.start_ms
            } else {
                seg_duration_ms
            };

            let needs_trim = trim_start_ms > 0 || trim_end_ms < seg_duration_ms;
            let piece = work_dir.join(format!("piece_{clip_index}_{piece_index:03}.mp4"));

            if needs_trim {
                let ss = format!("{:.3}", trim_start_ms as f64 / 1000.0);
                let to = format!("{:.3}", trim_end_ms as f64 / 1000.0);
                let input = raw_path.to_str().unwrap();
                let output = piece.to_str().unwrap();
                run_ffmpeg(&[
                    "-i", input, "-ss", &ss, "-to", &to, "-c", "copy",
                    "-avoid_negative_ts", "make_zero", "-y", output,
                ])
                .await?;
            } else {
                tokio::fs::rename(&raw_path, &piece)
                    .await
                    .map_err(|e| StitchError::TempIo(e.to_string()))?;
            }
            piece_paths.push(piece);
        } else {
            // Idle segment with real JPEG → 0.5s video
            let piece = work_dir.join(format!("piece_{clip_index}_{piece_index:03}.mp4"));
            let input = raw_path.to_str().unwrap();
            let output = piece.to_str().unwrap();
            let scale = format!("scale={}:{}", width, height);
            let fps_str = format!("{:.0}", fps);
            run_ffmpeg(&[
                "-loop", "1", "-i", input, "-c:v", "libx264", "-t", "0.5",
                "-pix_fmt", "yuv420p", "-r", &fps_str, "-vf", &scale, "-y", output,
            ])
            .await?;
            piece_paths.push(piece);
        }
        piece_index += 1;
    }

    if piece_paths.is_empty() {
        return Err(StitchError::NoContent);
    }

    if piece_paths.len() == 1 {
        return Ok(piece_paths.remove(0));
    }

    let concat_path = work_dir.join(format!("concat_{clip_index}.txt"));
    let mut concat_content = String::new();
    for p in &piece_paths {
        concat_content.push_str(&format!("file '{}'\n", p.display()));
    }
    tokio::fs::write(&concat_path, &concat_content)
        .await
        .map_err(|e| StitchError::TempIo(e.to_string()))?;

    let output = work_dir.join(format!("clip_{clip_index}.mp4"));
    let concat_input = concat_path.to_str().unwrap();
    let output_str = output.to_str().unwrap();
    run_ffmpeg(&[
        "-f", "concat", "-safe", "0", "-i", concat_input,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart", "-y", output_str,
    ])
    .await?;

    Ok(output)
}
