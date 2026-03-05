use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tracing::info;

use crate::llm_client::LlmClient;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryTree {
    pub robot_id: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub branching_factor: usize,
    pub created_at: String,
    pub segment_ids: Vec<i64>,
    pub nodes: Vec<TreeNode>,
    pub root_id: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    pub id: usize,
    pub node_type: NodeType,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NodeType {
    Leaf {
        segment_id: i64,
        s3_key: String,
        start_ms: i64,
        end_ms: i64,
    },
    Internal {
        children: Vec<usize>,
    },
}

/// Segment data needed for tree building (queried from SQLite).
#[derive(Debug, Clone)]
pub struct SegmentForTree {
    pub id: i64,
    pub s3_key: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub description: String,
}

/// SSE progress events emitted during tree building.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum TreeEvent {
    #[serde(rename = "tree_building_start")]
    BuildStart { total_leaves: usize, height: usize },
    #[serde(rename = "node_summary")]
    NodeSummary {
        node_id: usize,
        level: usize,
        total_at_level: usize,
        summary_preview: String,
    },
    #[serde(rename = "tree_complete")]
    Complete { node_count: usize, height: usize },
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

pub fn cache_key(robot_id: &str, start_ms: i64, end_ms: i64, branching_factor: usize) -> String {
    let input = format!("{robot_id}:{start_ms}:{end_ms}:{branching_factor}");
    let hash = Sha256::digest(input.as_bytes());
    hex::encode(&hash[..8]) // 16 hex chars
}

/// We inline a tiny hex encoder to avoid adding the `hex` crate.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

pub fn cache_dir(db_dir: &Path, robot_id: &str) -> PathBuf {
    db_dir.join(format!("{robot_id}_trees"))
}

pub fn cache_path(db_dir: &Path, robot_id: &str, key: &str) -> PathBuf {
    cache_dir(db_dir, robot_id).join(format!("{key}.json"))
}

pub async fn load_cached_tree(path: &Path) -> Option<SummaryTree> {
    let data = tokio::fs::read_to_string(path).await.ok()?;
    serde_json::from_str(&data).ok()
}

pub async fn save_tree(tree: &SummaryTree, path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let json = serde_json::to_string_pretty(tree)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    tokio::fs::write(path, json).await
}

// ---------------------------------------------------------------------------
// Tree building
// ---------------------------------------------------------------------------

/// Build a summarization tree from segment descriptions.
///
/// Constructs a tree bottom-up: leaves are individual segment descriptions,
/// and each internal node summarizes its `branching_factor` children via an
/// LLM call. Nodes at the same level are summarized concurrently (bounded
/// by the provided semaphore).
pub async fn build_tree(
    robot_id: &str,
    start_ms: i64,
    end_ms: i64,
    segments: Vec<SegmentForTree>,
    branching_factor: usize,
    llm: &LlmClient,
    semaphore: &tokio::sync::Semaphore,
    progress_tx: &mpsc::Sender<serde_json::Value>,
) -> Result<SummaryTree, String> {
    let n = segments.len();
    if n == 0 {
        return Err("no segments to build tree from".into());
    }

    let height = if n <= 1 {
        0
    } else {
        ((n as f64).log(branching_factor as f64).ceil() as usize).max(1)
    };

    // Send build start event
    let _ = progress_tx
        .send(serde_json::to_value(TreeEvent::BuildStart {
            total_leaves: n,
            height,
        }).unwrap())
        .await;

    let mut all_nodes: Vec<TreeNode> = Vec::new();

    // Create leaf nodes
    for seg in &segments {
        let id = all_nodes.len();
        all_nodes.push(TreeNode {
            id,
            node_type: NodeType::Leaf {
                segment_id: seg.id,
                s3_key: seg.s3_key.clone(),
                start_ms: seg.start_ms,
                end_ms: seg.end_ms,
            },
            summary: seg.description.clone(),
        });
    }

    // Build up level by level
    let mut current_level_ids: Vec<usize> = (0..n).collect();
    let mut level: usize = 1;

    while current_level_ids.len() > 1 {
        // Pad to multiple of branching_factor by repeating last node
        while current_level_ids.len() % branching_factor != 0 {
            current_level_ids.push(*current_level_ids.last().unwrap());
        }

        let chunks: Vec<Vec<usize>> = current_level_ids
            .chunks(branching_factor)
            .map(|c| c.to_vec())
            .collect();

        let total_at_level = chunks.len();
        let mut parent_results: Vec<(usize, Vec<usize>, String)> = Vec::new();

        // Process chunks sequentially with semaphore-bounded concurrency
        for chunk_ids in &chunks {
            let child_summaries: Vec<String> = chunk_ids
                .iter()
                .map(|&id| all_nodes[id].summary.clone())
                .collect();
            let chunk_ids = chunk_ids.clone();
            let permit = semaphore
                .acquire()
                .await
                .map_err(|e| format!("semaphore error: {e}"))?;

            // We can't easily move llm into spawned tasks since it's not Clone,
            // so we do sequential-within-level but with semaphore limiting
            let refs: Vec<&str> = child_summaries.iter().map(|s| s.as_str()).collect();
            let summary = llm
                .summarize_text(&refs)
                .await
                .map_err(|e| format!("LLM summarize error: {e}"))?;

            drop(permit);

            // Deduplicate child IDs (from padding)
            let mut unique_children: Vec<usize> = Vec::new();
            for &id in &chunk_ids {
                if unique_children.last() != Some(&id) {
                    unique_children.push(id);
                }
            }

            parent_results.push((0, unique_children, summary)); // id will be assigned below
        }

        // Assign IDs and insert nodes
        let mut next_level_ids = Vec::new();
        for (_, children, summary) in parent_results {
            let node_id = all_nodes.len();
            let preview = summary.chars().take(100).collect::<String>();

            all_nodes.push(TreeNode {
                id: node_id,
                node_type: NodeType::Internal { children },
                summary,
            });
            next_level_ids.push(node_id);

            let _ = progress_tx
                .send(serde_json::to_value(TreeEvent::NodeSummary {
                    node_id,
                    level,
                    total_at_level,
                    summary_preview: preview,
                }).unwrap())
                .await;
        }

        current_level_ids = next_level_ids;
        level += 1;
    }

    let root_id = current_level_ids[0];
    let actual_height = level - 1;

    let tree = SummaryTree {
        robot_id: robot_id.to_string(),
        start_ms,
        end_ms,
        branching_factor,
        created_at: chrono::Utc::now().to_rfc3339(),
        segment_ids: segments.iter().map(|s| s.id).collect(),
        nodes: all_nodes,
        root_id,
    };

    let _ = progress_tx
        .send(serde_json::to_value(TreeEvent::Complete {
            node_count: tree.nodes.len(),
            height: actual_height,
        }).unwrap())
        .await;

    info!(
        robot_id,
        root_id,
        node_count = tree.nodes.len(),
        height = actual_height,
        "summary tree built"
    );

    Ok(tree)
}
