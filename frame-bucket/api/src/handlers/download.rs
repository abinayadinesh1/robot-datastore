use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::header;
use axum::response::Response;
use rusqlite::params;
use tracing::warn;

use crate::db::open_robot_db;
use crate::s3::{download_segment_file, stitch_clip};
use crate::types::{ClipResponse, DownloadQuery, SegmentInfo, StitchError};
use crate::AppState;

/// GET /robots/:robot_id/collections/:collection_id/download?format=zip|mp4
pub async fn download_collection(
    State(state): State<Arc<AppState>>,
    AxumPath((robot_id, collection_id)): AxumPath<(String, i64)>,
    Query(q): Query<DownloadQuery>,
) -> Result<Response, StitchError> {
    let format = q.format.as_deref().unwrap_or("zip");

    let _permit = state
        .stitch_semaphore
        .acquire()
        .await
        .map_err(|_| StitchError::NoContent)?;

    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let (collection_name, clips, all_segments) =
        tokio::task::spawn_blocking(move || -> Result<(String, Vec<ClipResponse>, Vec<Vec<SegmentInfo>>), StitchError> {
            let conn = open_robot_db(&db_dir, &rid)
                .map_err(|e| StitchError::Db(e.to_string()))?;

            let collection_name: String = conn
                .query_row(
                    "SELECT name FROM collections WHERE id = ?1 AND robot_id = ?2",
                    params![collection_id, rid],
                    |row| row.get(0),
                )
                .map_err(|e| StitchError::Db(format!("collection not found: {e}")))?;

            let mut stmt = conn
                .prepare(
                    "SELECT id, collection_id, robot_id, modality, clip_start_ms, clip_end_ms,
                            segment_ids, manifest_s3_key, created_at
                     FROM collection_clips
                     WHERE collection_id = ?1 AND robot_id = ?2
                     ORDER BY clip_start_ms ASC",
                )
                .map_err(|e| StitchError::Db(e.to_string()))?;

            let clips: Vec<ClipResponse> = stmt
                .query_map(params![collection_id, rid], |row| {
                    let seg_ids_raw: String = row.get(6)?;
                    let segment_ids: Vec<i64> =
                        serde_json::from_str(&seg_ids_raw).unwrap_or_default();
                    Ok(ClipResponse {
                        id: row.get(0)?,
                        collection_id: row.get(1)?,
                        robot_id: row.get(2)?,
                        modality: row.get(3)?,
                        clip_start_ms: row.get(4)?,
                        clip_end_ms: row.get(5)?,
                        segment_ids,
                        manifest_s3_key: row.get(7)?,
                        created_at: row.get(8)?,
                    })
                })
                .map_err(|e| StitchError::Db(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StitchError::Db(e.to_string()))?;

            if clips.is_empty() {
                return Err(StitchError::NoContent);
            }

            let mut all_segments = Vec::new();
            for clip in &clips {
                let mut segments = Vec::new();
                for seg_id in &clip.segment_ids {
                    let result = conn.query_row(
                        "SELECT id, type, start_ms, end_ms, s3_key, size_bytes
                         FROM segments WHERE id = ?1 AND robot_id = ?2",
                        params![seg_id, rid],
                        |row| {
                            Ok(SegmentInfo {
                                segment_id: row.get(0)?,
                                segment_type: row.get(1)?,
                                start_ms: row.get(2)?,
                                end_ms: row.get(3)?,
                                source_key: row.get(4)?,
                                size_bytes: row.get::<_, Option<i64>>(5)?,
                            })
                        },
                    );
                    if let Ok(seg) = result {
                        segments.push(seg);
                    }
                }
                all_segments.push(segments);
            }

            Ok((collection_name, clips, all_segments))
        })
        .await
        .map_err(|e| StitchError::Db(e.to_string()))??;

    let job_id = uuid::Uuid::new_v4().to_string();
    let work_dir = std::env::temp_dir().join(format!("stitch_{job_id}"));
    tokio::fs::create_dir_all(&work_dir)
        .await
        .map_err(|e| StitchError::TempIo(e.to_string()))?;

    struct CleanupDir(PathBuf);
    impl Drop for CleanupDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = CleanupDir(work_dir.clone());

    let safe_name = collection_name.replace(' ', "_").replace('/', "-");
    let mut missing_count = 0u32;

    match format {
        "mp4" => {
            let mut stitched_files: Vec<PathBuf> = Vec::new();
            let mut current_size: u64 = 0;
            const MAX_PART_BYTES: u64 = 2 * 1024 * 1024 * 1024;

            for (i, (clip, segments)) in clips.iter().zip(all_segments.iter()).enumerate() {
                let before_count = segments.len();
                match stitch_clip(&state, clip, segments, &work_dir, i).await {
                    Ok(path) => {
                        let meta = tokio::fs::metadata(&path).await.unwrap_or_else(|_| {
                            std::fs::metadata(&path).unwrap()
                        });
                        current_size += meta.len();
                        stitched_files.push(path);
                    }
                    Err(StitchError::NoContent) => {
                        missing_count += before_count as u32;
                        continue;
                    }
                    Err(e) => {
                        warn!(clip_id = clip.id, error = %e, "failed to stitch clip, skipping");
                        missing_count += 1;
                        continue;
                    }
                }
            }

            if stitched_files.is_empty() {
                return Err(StitchError::NoContent);
            }

            if stitched_files.len() == 1 && current_size <= MAX_PART_BYTES {
                let data = tokio::fs::read(&stitched_files[0])
                    .await
                    .map_err(|e| StitchError::TempIo(e.to_string()))?;
                let filename = format!("{safe_name}.mp4");
                let mut builder = Response::builder()
                    .header(header::CONTENT_TYPE, "video/mp4")
                    .header(
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{filename}\""),
                    )
                    .header(header::CONTENT_LENGTH, data.len());
                if missing_count > 0 {
                    builder =
                        builder.header("X-Missing-Segments", missing_count.to_string());
                }
                return Ok(builder.body(Body::from(data)).unwrap());
            }

            // Multiple clips or over 2 GB: package as zip
            let zip_path = work_dir.join(format!("{safe_name}.zip"));
            let zip_file = std::fs::File::create(&zip_path)
                .map_err(|e| StitchError::TempIo(e.to_string()))?;
            let mut zip_writer = zip::ZipWriter::new(zip_file);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);

            for (i, path) in stitched_files.iter().enumerate() {
                let clip = &clips[i.min(clips.len() - 1)];
                let entry_name = format!("clip_{:03}_{}.mp4", i + 1, clip.clip_start_ms);
                let data = tokio::fs::read(path)
                    .await
                    .map_err(|e| StitchError::TempIo(e.to_string()))?;
                zip_writer
                    .start_file(&entry_name, options)
                    .map_err(|e| StitchError::TempIo(e.to_string()))?;
                std::io::Write::write_all(&mut zip_writer, &data)
                    .map_err(|e| StitchError::TempIo(e.to_string()))?;
            }
            zip_writer
                .finish()
                .map_err(|e| StitchError::TempIo(e.to_string()))?;

            let data = tokio::fs::read(&zip_path)
                .await
                .map_err(|e| StitchError::TempIo(e.to_string()))?;
            let filename = format!("{safe_name}.zip");
            let mut builder = Response::builder()
                .header(header::CONTENT_TYPE, "application/zip")
                .header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{filename}\""),
                )
                .header(header::CONTENT_LENGTH, data.len());
            if missing_count > 0 {
                builder = builder.header("X-Missing-Segments", missing_count.to_string());
            }
            Ok(builder.body(Body::from(data)).unwrap())
        }

        _ => {
            // ZIP mode: download raw segments, package in zip
            let zip_path = work_dir.join(format!("{safe_name}.zip"));
            let zip_file = std::fs::File::create(&zip_path)
                .map_err(|e| StitchError::TempIo(e.to_string()))?;
            let mut zip_writer = zip::ZipWriter::new(zip_file);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);

            let mut file_index = 0usize;
            for segments in all_segments.iter() {
                for seg in segments {
                    if seg.segment_type == "idle"
                        && (seg.source_key.is_empty() || seg.source_key.starts_with("idle:"))
                    {
                        continue;
                    }

                    let raw_path = match download_segment_file(
                        &state,
                        &seg.source_key,
                        &seg.segment_type,
                        &work_dir,
                        seg.segment_id,
                    )
                    .await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(segment_id = seg.segment_id, error = %e, "skipping unavailable segment");
                            missing_count += 1;
                            continue;
                        }
                    };

                    let ext = if seg.segment_type == "active" { "mp4" } else { "jpg" };
                    let entry_name = format!(
                        "{:03}_{}_{}.{}",
                        file_index + 1,
                        seg.segment_type,
                        seg.start_ms,
                        ext
                    );

                    let data = tokio::fs::read(&raw_path)
                        .await
                        .map_err(|e| StitchError::TempIo(e.to_string()))?;

                    zip_writer
                        .start_file(&entry_name, options)
                        .map_err(|e| StitchError::TempIo(e.to_string()))?;
                    std::io::Write::write_all(&mut zip_writer, &data)
                        .map_err(|e| StitchError::TempIo(e.to_string()))?;

                    file_index += 1;
                }
            }

            zip_writer
                .finish()
                .map_err(|e| StitchError::TempIo(e.to_string()))?;

            if file_index == 0 {
                return Err(StitchError::NoContent);
            }

            let data = tokio::fs::read(&zip_path)
                .await
                .map_err(|e| StitchError::TempIo(e.to_string()))?;
            let filename = format!("{safe_name}_segments.zip");
            let mut builder = Response::builder()
                .header(header::CONTENT_TYPE, "application/zip")
                .header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{filename}\""),
                )
                .header(header::CONTENT_LENGTH, data.len());
            if missing_count > 0 {
                builder = builder.header("X-Missing-Segments", missing_count.to_string());
            }
            Ok(builder.body(Body::from(data)).unwrap())
        }
    }
}
