use std::collections::HashMap;

use rusqlite::params;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tracing::{info, warn};

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
    /// Whether the tree is fully built. Partial trees can be resumed.
    #[serde(default = "default_true")]
    pub complete: bool,
    /// Node IDs at the current frontier (needed to resume an incomplete build).
    #[serde(default)]
    pub next_level_ids: Vec<usize>,
    /// The next level number to build (used on resume).
    #[serde(default)]
    pub current_build_level: usize,
}

fn default_true() -> bool {
    true
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
        /// Whether this summary was served from the node-level cache.
        cached: bool,
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
// Node-level summary cache (for subtree reuse across trees)
// ---------------------------------------------------------------------------

/// Compute a content-addressed cache key for an internal node based on its
/// direct children's time ranges. This is intrinsic to the data: same robot,
/// same branching factor, same child intervals → same cache key.
pub fn node_cache_key(robot_id: &str, bf: usize, child_ranges: &[(i64, i64)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(robot_id.as_bytes());
    hasher.update(bf.to_le_bytes());
    for &(start, end) in child_ranges {
        hasher.update(start.to_le_bytes());
        hasher.update(end.to_le_bytes());
    }
    hex::encode(&hasher.finalize()[..8])
}

/// Create the node summary cache table if it doesn't exist.
pub fn ensure_node_cache_table(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS node_summary_cache (
            key     TEXT PRIMARY KEY,
            summary TEXT NOT NULL
        );"
    )
}

/// Load all cached node summaries into a HashMap for fast lookup during build.
pub fn load_node_cache(conn: &rusqlite::Connection) -> rusqlite::Result<HashMap<String, String>> {
    let mut stmt = conn.prepare("SELECT key, summary FROM node_summary_cache")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map = HashMap::new();
    for row in rows {
        let (k, v) = row?;
        map.insert(k, v);
    }
    Ok(map)
}

/// Flush new cache entries to SQLite. Uses INSERT OR REPLACE so concurrent
/// builds and force-rebuilds produce consistent results.
pub fn flush_node_cache(
    conn: &rusqlite::Connection,
    entries: &[(String, String)],
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO node_summary_cache (key, summary) VALUES (?1, ?2)"
    )?;
    for (k, v) in entries {
        stmt.execute(params![k, v])?;
    }
    Ok(())
}

/// Recompute the (start_ms, end_ms) range for each node from the existing
/// node list. Used when resuming a partial tree build.
pub fn recompute_node_ranges(nodes: &[TreeNode]) -> Vec<(i64, i64)> {
    let mut ranges = Vec::with_capacity(nodes.len());
    for node in nodes {
        match &node.node_type {
            NodeType::Leaf { start_ms, end_ms, .. } => {
                ranges.push((*start_ms, *end_ms));
            }
            NodeType::Internal { children } => {
                let start = children.iter().map(|&c| ranges[c].0).min().unwrap_or(0);
                let end = children.iter().map(|&c| ranges[c].1).max().unwrap_or(0);
                ranges.push((start, end));
            }
        }
    }
    ranges
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
///
/// If `cache_path` is provided, the tree is saved to disk after each level
/// so that a partially-built tree can be resumed later.
///
/// If `partial` is provided, the build resumes from that saved state instead
/// of starting from scratch.
pub async fn build_tree(
    robot_id: &str,
    start_ms: i64,
    end_ms: i64,
    segments: Vec<SegmentForTree>,
    branching_factor: usize,
    llm: &LlmClient,
    semaphore: &tokio::sync::Semaphore,
    progress_tx: &mpsc::Sender<serde_json::Value>,
    cache_path: Option<&Path>,
    partial: Option<SummaryTree>,
    node_cache: &mut HashMap<String, String>,
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

    let segment_ids: Vec<i64> = segments.iter().map(|s| s.id).collect();

    // Either resume from a partial tree or start fresh
    let (mut all_nodes, mut current_level_ids, start_level) = if let Some(p) = partial {
        info!(
            level = p.current_build_level,
            existing_nodes = p.nodes.len(),
            frontier_size = p.next_level_ids.len(),
            "resuming partial tree build"
        );
        let _ = progress_tx
            .send(serde_json::to_value(TreeEvent::BuildStart {
                total_leaves: n,
                height,
            }).unwrap())
            .await;
        (p.nodes, p.next_level_ids, p.current_build_level)
    } else {
        let _ = progress_tx
            .send(serde_json::to_value(TreeEvent::BuildStart {
                total_leaves: n,
                height,
            }).unwrap())
            .await;

        // Create leaf nodes
        let mut nodes: Vec<TreeNode> = Vec::new();
        for seg in &segments {
            let id = nodes.len();
            nodes.push(TreeNode {
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

        let ids: Vec<usize> = (0..n).collect();

        // Save partial tree with just leaves
        save_partial(
            cache_path, robot_id, start_ms, end_ms, branching_factor,
            &segment_ids, &nodes, &ids, 1,
        ).await;

        (nodes, ids, 1)
    };

    // Track each node's (start_ms, end_ms) range for node-level cache keys.
    // On resume, recompute from existing nodes; on fresh build, already set
    // from leaf creation above.
    let mut node_ranges: Vec<(i64, i64)> = recompute_node_ranges(&all_nodes);

    // Build up level by level
    let mut level = start_level;
    let mut cache_hits = 0usize;
    let mut cache_misses = 0usize;

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
        let mut next_level_ids = Vec::new();

        // Process chunks sequentially with semaphore-bounded concurrency
        for chunk_ids in &chunks {
            let chunk_ids = chunk_ids.clone();

            // Deduplicate child IDs (from padding)
            let mut unique_children: Vec<usize> = Vec::new();
            for &id in &chunk_ids {
                if unique_children.last() != Some(&id) {
                    unique_children.push(id);
                }
            }

            // Compute cache key from direct children's time ranges
            let child_ranges: Vec<(i64, i64)> = unique_children
                .iter()
                .map(|&cid| node_ranges[cid])
                .collect();
            let nck = node_cache_key(robot_id, branching_factor, &child_ranges);

            // Check node-level cache before calling LLM
            let (summary, was_cached) = if let Some(cached) = node_cache.get(&nck) {
                cache_hits += 1;
                (cached.clone(), true)
            } else {
                // Cache miss — call LLM
                let child_summaries: Vec<String> = chunk_ids
                    .iter()
                    .map(|&id| all_nodes[id].summary.clone())
                    .collect();
                let permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| format!("semaphore error: {e}"))?;

                let refs: Vec<&str> = child_summaries.iter().map(|s| s.as_str()).collect();
                let summary = llm
                    .summarize_text(&refs)
                    .await
                    .map_err(|e| format!("LLM summarize error: {e}"))?;

                drop(permit);

                node_cache.insert(nck, summary.clone());
                cache_misses += 1;
                (summary, false)
            };

            let node_id = all_nodes.len();
            let preview = summary.chars().take(100).collect::<String>();

            // Compute this node's range from its children
            let node_start = child_ranges.iter().map(|r| r.0).min().unwrap_or(0);
            let node_end = child_ranges.iter().map(|r| r.1).max().unwrap_or(0);
            node_ranges.push((node_start, node_end));

            all_nodes.push(TreeNode {
                id: node_id,
                node_type: NodeType::Internal { children: unique_children },
                summary,
            });
            next_level_ids.push(node_id);

            let _ = progress_tx
                .send(serde_json::to_value(TreeEvent::NodeSummary {
                    node_id,
                    level,
                    total_at_level,
                    summary_preview: preview,
                    cached: was_cached,
                }).unwrap())
                .await;
        }

        current_level_ids = next_level_ids;
        level += 1;

        // Save partial tree after each level completes
        save_partial(
            cache_path, robot_id, start_ms, end_ms, branching_factor,
            &segment_ids, &all_nodes, &current_level_ids, level,
        ).await;
    }

    let root_id = current_level_ids[0];
    let actual_height = level - 1;

    let tree = SummaryTree {
        robot_id: robot_id.to_string(),
        start_ms,
        end_ms,
        branching_factor,
        created_at: chrono::Utc::now().to_rfc3339(),
        segment_ids,
        nodes: all_nodes,
        root_id,
        complete: true,
        next_level_ids: vec![],
        current_build_level: 0,
    };

    // Save the complete tree
    if let Some(cp) = cache_path {
        if let Err(e) = save_tree(&tree, cp).await {
            warn!(error = %e, "failed to save complete tree to cache");
        }
    }

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
        cache_hits,
        cache_misses,
        "summary tree built"
    );

    Ok(tree)
}

/// Persist a partial (incomplete) tree so it can be resumed later.
async fn save_partial(
    cache_path: Option<&Path>,
    robot_id: &str,
    start_ms: i64,
    end_ms: i64,
    branching_factor: usize,
    segment_ids: &[i64],
    nodes: &[TreeNode],
    next_level_ids: &[usize],
    current_build_level: usize,
) {
    let Some(cp) = cache_path else { return };
    let partial = SummaryTree {
        robot_id: robot_id.to_string(),
        start_ms,
        end_ms,
        branching_factor,
        created_at: chrono::Utc::now().to_rfc3339(),
        segment_ids: segment_ids.to_vec(),
        nodes: nodes.to_vec(),
        root_id: 0,
        complete: false,
        next_level_ids: next_level_ids.to_vec(),
        current_build_level,
    };
    if let Err(e) = save_tree(&partial, cp).await {
        warn!(error = %e, "failed to save partial tree");
    }
}
