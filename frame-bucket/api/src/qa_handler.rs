use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use rusqlite::params;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use crate::qa_tree::{self, NodeType, SegmentForTree, SummaryTree};
use crate::{open_robot_db, AppState};

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct QuestionQuery {
    pub question: String,
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(default = "default_branching_factor")]
    pub branching_factor: usize,
    #[serde(default = "default_beam_width")]
    pub beam_width: usize,
    #[serde(default)]
    pub force_rebuild: bool,
}

fn default_branching_factor() -> usize { 2 }
fn default_beam_width() -> usize { 1 }

// ---------------------------------------------------------------------------
// SSE event helpers
// ---------------------------------------------------------------------------

fn sse_json(value: &serde_json::Value) -> Event {
    Event::default().data(serde_json::to_string(value).unwrap_or_default())
}

fn sse_event(event_type: &str, data: serde_json::Value) -> serde_json::Value {
    let mut obj = match data {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    obj.insert("type".to_string(), serde_json::Value::String(event_type.to_string()));
    serde_json::Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Beam search state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Beam {
    cumulative_prob: f64,
    node_id: usize,
}

impl PartialEq for Beam {
    fn eq(&self, other: &Self) -> bool {
        self.cumulative_prob == other.cumulative_prob
    }
}
impl Eq for Beam {}
impl PartialOrd for Beam {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Beam {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cumulative_prob
            .partial_cmp(&other.cumulative_prob)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

// ---------------------------------------------------------------------------
// SSE handler
// ---------------------------------------------------------------------------

pub async fn question_stream(
    State(state): State<Arc<AppState>>,
    AxumPath(robot_id): AxumPath<String>,
    Query(q): Query<QuestionQuery>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let (tx, rx) = mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        if let Err(e) = run_qa_pipeline(state, robot_id, q, tx.clone()).await {
            let _ = tx
                .send(sse_json(&sse_event("error", serde_json::json!({ "message": e }))))
                .await;
        }
    });

    Sse::new(ReceiverStream::new(rx).map(Ok)).keep_alive(KeepAlive::default())
}

// We need the map combinator
use futures::StreamExt;

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

async fn run_qa_pipeline(
    state: Arc<AppState>,
    robot_id: String,
    q: QuestionQuery,
    tx: mpsc::Sender<Event>,
) -> Result<(), String> {
    // Validate branching factor
    let bf = if q.branching_factor < 2 { 2 } else { q.branching_factor };
    let beam_width = q.beam_width.max(1);

    // Helper to send SSE events
    let send = |val: serde_json::Value| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(sse_json(&val)).await;
        }
    };

    // ── Step 1: Query active segments ────────────────────────────────────

    let db_dir = state.db_dir.clone();
    let rid = robot_id.clone();
    let start = q.start_ms;
    let end = q.end_ms;

    let mut segments: Vec<SegmentForTree> = tokio::task::spawn_blocking(move || {
        let conn = open_robot_db(&db_dir, &rid)
            .map_err(|e| format!("DB error: {e}"))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, s3_key, start_ms, end_ms, description
                 FROM segments
                 WHERE robot_id = ?1
                   AND type = 'active'
                   AND end_ms >= ?2
                   AND start_ms <= ?3
                 ORDER BY start_ms ASC",
            )
            .map_err(|e| format!("SQL prepare error: {e}"))?;

        let rows = stmt
            .query_map(params![rid, start, end], |row| {
                Ok(SegmentForTree {
                    id: row.get(0)?,
                    s3_key: row.get(1)?,
                    start_ms: row.get(2)?,
                    end_ms: row.get(3)?,
                    description: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                })
            })
            .map_err(|e| format!("SQL query error: {e}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("SQL row error: {e}"))?;

        Ok::<_, String>(rows)
    })
    .await
    .map_err(|e| format!("spawn_blocking error: {e}"))??;

    if segments.is_empty() {
        return Err("No active segments found in the selected time range.".into());
    }

    send(sse_event(
        "segments_found",
        serde_json::json!({
            "count": segments.len(),
            "start_ms": q.start_ms,
            "end_ms": q.end_ms,
        }),
    ))
    .await;

    // ── Step 2: Check descriptions; backfill if ≤ 10 missing ───────────

    let missing_idxs: Vec<usize> = segments
        .iter()
        .enumerate()
        .filter(|(_, s)| s.description.is_empty())
        .map(|(i, _)| i)
        .collect();

    if !missing_idxs.is_empty() {
        if missing_idxs.len() > 10 {
            let ids: Vec<i64> = missing_idxs.iter().map(|&i| segments[i].id).collect();
            return Err(format!(
                "{} segment(s) are missing descriptions (IDs: {:?}). \
                 This robot may not have annotation enabled yet.",
                ids.len(),
                &ids[..ids.len().min(5)]
            ));
        }

        // Dynamic backfill: annotate the few missing segments on the fly
        let missing_ids: Vec<i64> = missing_idxs.iter().map(|&i| segments[i].id).collect();
        send(sse_event(
            "backfilling_descriptions",
            serde_json::json!({
                "count": missing_idxs.len(),
                "segment_ids": missing_ids,
            }),
        ))
        .await;

        let backfill_dir = std::env::temp_dir().join(format!("qa_backfill_{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&backfill_dir)
            .await
            .map_err(|e| format!("failed to create backfill temp dir: {e}"))?;

        for (progress_i, &idx) in missing_idxs.iter().enumerate() {
            let seg = &segments[idx];
            let seg_id = seg.id;

            send(sse_event(
                "annotating_segment",
                serde_json::json!({
                    "segment_id": seg_id,
                    "progress": format!("{}/{}", progress_i + 1, missing_idxs.len()),
                }),
            ))
            .await;

            // Download video
            let path = crate::download_segment_file(
                &state,
                &seg.s3_key,
                "active",
                &backfill_dir,
                seg_id,
            )
            .await
            .map_err(|e| format!("failed to download segment {seg_id} for annotation: {e}"))?;

            let video_bytes = tokio::fs::read(&path)
                .await
                .map_err(|e| format!("failed to read segment {seg_id} video: {e}"))?;

            // Call video LLM for description
            let model_desc = state
                .llm_client
                .ask_video(
                    video_bytes,
                    "Give a detailed description of everything happening in this video.",
                    128,
                )
                .await
                .map_err(|e| format!("failed to annotate segment {seg_id}: {e}"))?;

            // Format with timestamps matching the annotation pipeline
            let start_fmt = format_ms(seg.start_ms);
            let end_fmt = format_ms(seg.end_ms);
            let description = format!("From {start_fmt} to {end_fmt}, {model_desc}");

            // Write to SQLite
            let db_dir2 = state.db_dir.clone();
            let rid2 = robot_id.clone();
            let desc_clone = description.clone();
            tokio::task::spawn_blocking(move || {
                let conn = open_robot_db(&db_dir2, &rid2)
                    .map_err(|e| format!("DB error: {e}"))?;
                conn.execute(
                    "UPDATE segments SET description = ?1 WHERE id = ?2",
                    params![desc_clone, seg_id],
                )
                .map_err(|e| format!("SQL update error: {e}"))?;
                Ok::<_, String>(())
            })
            .await
            .map_err(|e| format!("spawn_blocking error: {e}"))??;

            // Update in-memory
            segments[idx].description = description;
        }

        let _ = tokio::fs::remove_dir_all(&backfill_dir).await;

        send(sse_event(
            "backfill_complete",
            serde_json::json!({ "count": missing_idxs.len() }),
        ))
        .await;
    }

    // ── Step 3: Check cache or build tree ────────────────────────────────

    let ck = qa_tree::cache_key(&robot_id, q.start_ms, q.end_ms, bf);
    let cp = qa_tree::cache_path(&state.db_dir, &robot_id, &ck);

    let tree: SummaryTree = if !q.force_rebuild {
        if let Some(cached) = qa_tree::load_cached_tree(&cp).await {
            let current_seg_ids: Vec<i64> = segments.iter().map(|s| s.id).collect();
            if cached.segment_ids != current_seg_ids {
                info!(cache_key = %ck, "cached tree segments changed, rebuilding");
                build_and_cache(&state, &robot_id, q.start_ms, q.end_ms, segments, bf, &cp, &tx, None).await?
            } else if cached.complete {
                info!(cache_key = %ck, "using cached complete summary tree");
                send(sse_event("cache_hit", serde_json::json!({ "tree_id": ck }))).await;
                cached
            } else {
                info!(
                    cache_key = %ck,
                    nodes_built = cached.nodes.len(),
                    resume_level = cached.current_build_level,
                    "resuming incomplete cached tree"
                );
                send(sse_event("cache_resuming", serde_json::json!({
                    "tree_id": ck,
                    "nodes_built": cached.nodes.len(),
                    "resume_level": cached.current_build_level,
                }))).await;
                build_and_cache(&state, &robot_id, q.start_ms, q.end_ms, segments, bf, &cp, &tx, Some(cached)).await?
            }
        } else {
            build_and_cache(&state, &robot_id, q.start_ms, q.end_ms, segments, bf, &cp, &tx, None).await?
        }
    } else {
        build_and_cache(&state, &robot_id, q.start_ms, q.end_ms, segments, bf, &cp, &tx, None).await?
    };

    // ── Step 4: Beam-search traversal ────────────────────────────────────

    send(sse_event(
        "traversal_start",
        serde_json::json!({ "question": q.question }),
    ))
    .await;

    let mut beams = BinaryHeap::new();
    beams.push(Beam {
        cumulative_prob: 1.0,
        node_id: tree.root_id,
    });

    let mut level = 0usize;
    let mut leaf_beams: Vec<Beam> = Vec::new();

    loop {
        if beams.is_empty() {
            break;
        }

        let mut next_beams = BinaryHeap::new();
        let mut any_internal = false;

        // Drain current beams
        let current: Vec<Beam> = beams.into_sorted_vec();
        // into_sorted_vec gives ascending order, we want descending
        let current: Vec<Beam> = current.into_iter().rev().collect();

        for beam in current {
            let node = &tree.nodes[beam.node_id];
            match &node.node_type {
                NodeType::Leaf { .. } => {
                    leaf_beams.push(beam);
                }
                NodeType::Internal { children } => {
                    any_internal = true;

                    // Build children summaries for LLM
                    let children_summaries: Vec<(usize, &str)> = children
                        .iter()
                        .map(|&cid| (cid, tree.nodes[cid].summary.as_str()))
                        .collect();

                    let probs = state
                        .llm_client
                        .choose_path(&q.question, &children_summaries)
                        .await
                        .map_err(|e| format!("LLM choose_path error: {e}"))?;

                    for (child_id, prob) in &probs {
                        let new_prob = beam.cumulative_prob * prob;
                        next_beams.push(Beam {
                            cumulative_prob: new_prob,
                            node_id: *child_id,
                        });

                        send(sse_event(
                            "path_chosen",
                            serde_json::json!({
                                "level": level,
                                "chosen_child_id": child_id,
                                "probability": prob,
                                "cumulative_probability": new_prob,
                            }),
                        ))
                        .await;
                    }
                }
            }
        }

        if !any_internal {
            break;
        }

        // Prune to top beam_width
        let mut sorted: Vec<Beam> = next_beams.into_sorted_vec();
        sorted.reverse();
        sorted.truncate(beam_width);

        beams = BinaryHeap::new();
        for b in sorted {
            beams.push(b);
        }

        level += 1;
    }

    // Collect leaf nodes (sorted by probability descending)
    leaf_beams.sort_by(|a, b| b.cumulative_prob.partial_cmp(&a.cumulative_prob).unwrap_or(std::cmp::Ordering::Equal));
    leaf_beams.truncate(beam_width);

    if leaf_beams.is_empty() {
        return Err("Beam search produced no leaf nodes.".into());
    }

    // ── Step 5: Download video segment(s) ────────────────────────────────

    // Collect leaf segment info
    struct LeafInfo {
        segment_id: i64,
        s3_key: String,
        start_ms: i64,
        end_ms: i64,
    }

    let mut leaves: Vec<LeafInfo> = Vec::new();
    for beam in &leaf_beams {
        let node = &tree.nodes[beam.node_id];
        if let NodeType::Leaf {
            segment_id,
            s3_key,
            start_ms,
            end_ms,
        } = &node.node_type
        {
            send(sse_event(
                "leaf_reached",
                serde_json::json!({
                    "segment_id": segment_id,
                    "s3_key": s3_key,
                    "probability": beam.cumulative_prob,
                }),
            ))
            .await;

            leaves.push(LeafInfo {
                segment_id: *segment_id,
                s3_key: s3_key.clone(),
                start_ms: *start_ms,
                end_ms: *end_ms,
            });
        }
    }

    // Create temp dir for downloads
    let work_dir = std::env::temp_dir().join(format!("qa_stitch_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&work_dir)
        .await
        .map_err(|e| format!("failed to create temp dir: {e}"))?;

    // Download segment videos
    let mut video_paths = Vec::new();
    for leaf in &leaves {
        send(sse_event(
            "downloading_video",
            serde_json::json!({ "segment_id": leaf.segment_id }),
        ))
        .await;

        let path = crate::download_segment_file(
            &state,
            &leaf.s3_key,
            "active",
            &work_dir,
            leaf.segment_id,
        )
        .await
        .map_err(|e| format!("failed to download segment {}: {e}", leaf.segment_id))?;

        video_paths.push(path);
    }

    // ── Step 6: Stitch if multiple, then ask video LLM ───────────────────

    let final_video_path = if video_paths.len() == 1 {
        video_paths.remove(0)
    } else {
        // Stitch multiple videos together using ffmpeg concat
        send(sse_event(
            "stitching_videos",
            serde_json::json!({ "count": video_paths.len() }),
        ))
        .await;

        let concat_path = work_dir.join("concat.txt");
        let mut concat_content = String::new();
        for p in &video_paths {
            concat_content.push_str(&format!("file '{}'\n", p.display()));
        }
        tokio::fs::write(&concat_path, &concat_content)
            .await
            .map_err(|e| format!("failed to write concat file: {e}"))?;

        let output = work_dir.join("stitched.mp4");
        let concat_str = concat_path.to_str().unwrap();
        let output_str = output.to_str().unwrap();

        crate::run_ffmpeg(&[
            "-f", "concat", "-safe", "0", "-i", concat_str,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart", "-y", output_str,
        ])
        .await
        .map_err(|e| format!("ffmpeg stitch error: {e}"))?;

        output
    };

    // Read video bytes
    let video_bytes = tokio::fs::read(&final_video_path)
        .await
        .map_err(|e| format!("failed to read stitched video: {e}"))?;

    send(sse_event(
        "calling_video_llm",
        serde_json::json!({ "question": q.question }),
    ))
    .await;

    // Try video verification asynchronously; fall back to text summary if it fails
    let video_result = state
        .llm_client
        .ask_video(
            video_bytes,
            &format!("Give me the answer to this question: {}", q.question),
            128,
        )
        .await;

    let leaf_info: Vec<serde_json::Value> = leaves
        .iter()
        .map(|l| {
            serde_json::json!({
                "segment_id": l.segment_id,
                "start_ms": l.start_ms,
                "end_ms": l.end_ms,
            })
        })
        .collect();

    match video_result {
        Ok(answer) => {
            send(sse_event(
                "answer_ready",
                serde_json::json!({
                    "answer": answer,
                    "segments": leaf_info,
                    "verified": true,
                }),
            ))
            .await;
        }
        Err(e) => {
            warn!(error = %e, "video verification failed, falling back to text summary");

            // Build a text-based answer from the leaf node summaries
            let leaf_summaries: Vec<&str> = leaf_beams
                .iter()
                .map(|b| tree.nodes[b.node_id].summary.as_str())
                .collect();
            let fallback_answer = format!(
                "Video verification was unavailable ({}). Based on the segment descriptions, \
                 here is what was found:\n\n{}",
                e,
                leaf_summaries.join("\n\n")
            );

            send(sse_event(
                "answer_ready",
                serde_json::json!({
                    "answer": fallback_answer,
                    "segments": leaf_info,
                    "verified": false,
                }),
            ))
            .await;
        }
    }

    // Cleanup temp dir (best effort)
    let _ = tokio::fs::remove_dir_all(&work_dir).await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Build + cache helper
// ---------------------------------------------------------------------------

async fn build_and_cache(
    state: &AppState,
    robot_id: &str,
    start_ms: i64,
    end_ms: i64,
    segments: Vec<SegmentForTree>,
    branching_factor: usize,
    cache_path: &Path,
    sse_tx: &mpsc::Sender<Event>,
    partial: Option<SummaryTree>,
) -> Result<SummaryTree, String> {
    // Create a channel to forward tree progress events as SSE
    let (progress_tx, mut progress_rx) = mpsc::channel::<serde_json::Value>(64);
    let sse_tx_clone = sse_tx.clone();
    tokio::spawn(async move {
        while let Some(val) = progress_rx.recv().await {
            let _ = sse_tx_clone.send(sse_json(&val)).await;
        }
    });

    let tree = qa_tree::build_tree(
        robot_id,
        start_ms,
        end_ms,
        segments,
        branching_factor,
        &state.llm_client,
        &state.qa_semaphore,
        &progress_tx,
        Some(cache_path),
        partial,
    )
    .await?;

    Ok(tree)
}

/// Format millisecond timestamp as UTC string, matching the annotation pipeline.
fn format_ms(ms: i64) -> String {
    chrono::TimeZone::timestamp_millis_opt(&chrono::Utc, ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| ms.to_string())
}
