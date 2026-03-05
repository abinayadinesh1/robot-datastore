# Hierarchical Video QA Feature

Ask natural-language questions about a time interval of robot video and get answers backed by LLM-guided search through a summarization tree.

## How it works

### Part 1 — Build a summarization tree

1. All **active** segments in the selected time range are fetched from SQLite.
2. Every segment must already have a text `description` (from the annotation pipeline). If any are missing, the request aborts with an error.
3. Segment descriptions become **leaf nodes** of a tree.
4. Leaves are grouped into chunks of `branching_factor` (default 2). Each chunk is summarized by the text LLM into a **parent node**.
5. This repeats bottom-up until a single **root** remains. Tree height is O(log_k(n)).
6. The tree is cached to `data/{robot_id}_trees/{hash}.json`. Identical queries skip the build.

### Part 2 — Beam-search traversal

1. Starting at the root, the LLM assigns a probability to each child indicating how likely it contains the answer.
2. The top `beam_width` paths are kept (default 1 = greedy).
3. This recurses down to the leaves.
4. The leaf segment video(s) are downloaded. If multiple leaves, they are stitched into one MP4 via ffmpeg.
5. The stitched video + user question are sent to the VideoChat-Flash model for a final verified answer.
6. If the video model is unreachable, the leaf text summaries are returned as an unverified fallback.

All progress is streamed to the browser via **Server-Sent Events (SSE)**.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `LLM_BASE_URL` | `https://abinayadinesh1--example-vllm-inference-serve-dev.modal.run` | Base URL for the vLLM text model (used for summarization and tree traversal routing) |
| `LLM_MODEL` | `llm` | Model name/alias on the vLLM server (currently Qwen3-4B) |
| `VIDEO_LLM_BASE_URL` | `https://abinayadinesh1--videochat-flash-inference-serve.modal.run` | Base URL for the VideoChat-Flash video model (used for final answer verification) |

## API

### `GET /robots/:robot_id/questions/stream`

SSE endpoint. Query parameters:

| Param | Type | Default | Description |
|---|---|---|---|
| `question` | string | *required* | The question to answer |
| `start_ms` | i64 | *required* | Start of time range (epoch ms) |
| `end_ms` | i64 | *required* | End of time range (epoch ms) |
| `branching_factor` | usize | `2` | Children per tree node (must be >= 2) |
| `beam_width` | usize | `1` | Number of paths to track during traversal |
| `force_rebuild` | bool | `false` | Ignore cached tree and rebuild |

SSE event types (each is a JSON object with a `type` field):

```
segments_found      → { count, start_ms, end_ms }
cache_hit           → { tree_id }
tree_building_start → { total_leaves, height }
node_summary        → { node_id, level, total_at_level, summary_preview }
tree_complete       → { node_count, height }
traversal_start     → { question }
path_chosen         → { level, chosen_child_id, probability, cumulative_probability }
leaf_reached        → { segment_id, s3_key, probability }
downloading_video   → { segment_id }
stitching_videos    → { count }
calling_video_llm   → { question }
answer_ready        → { answer, segments: [...], verified: bool }
error               → { message }
```

## Files

| File | Purpose |
|---|---|
| `src/llm_client.rs` | HTTP client for vLLM (`/v1/chat/completions`) and VideoChat-Flash (`/v1/video/chat`) |
| `src/qa_tree.rs` | Tree data structures, bottom-up build algorithm, SHA-256 cache key, JSON disk cache |
| `src/qa_handler.rs` | SSE endpoint handler, beam-search traversal, video download + ffmpeg stitch |
| `src/main.rs` | Wiring: mod declarations, AppState fields, route registration |
| `../../stream-viewer/robot.html` | Frontend: EventSource SSE consumer, progress panel, answer display |

## End-to-end test

### Prerequisites

- The API server is running (`cargo run` from `frame-bucket/api/`)
- A robot DB exists with **annotated** active segments (segments must have a non-null `description` column)
- The vLLM server is reachable (check: `curl https://abinayadinesh1--example-vllm-inference-serve-dev.modal.run/health`)
- `ffmpeg` is installed (needed for video stitching when beam_width > 1)

### 1. Verify the LLM endpoints are up

```bash
# Text LLM (vLLM)
curl -s https://abinayadinesh1--example-vllm-inference-serve-dev.modal.run/health

# Video LLM (VideoChat-Flash) — may take ~60s to cold-start
curl -s https://abinayadinesh1--videochat-flash-inference-serve.modal.run/health
```

Both should return a 200 OK.

### 2. Check that a robot has annotated segments

```bash
# Replace reachy-003 with your robot ID
python frame-bucket/sqlite_peek.py reachy-003
```

You should see segments with non-empty descriptions. Note the time range.

### 3. Test the SSE endpoint directly with curl

```bash
# Replace robot ID and time range with real values from step 2
curl -N "http://localhost:3001/robots/reachy-003/questions/stream?\
question=What%20did%20the%20robot%20do?\
&start_ms=1700000000000\
&end_ms=1700003600000\
&branching_factor=2\
&beam_width=1"
```

You should see a stream of JSON SSE events:

```
data: {"type":"segments_found","count":8,"start_ms":1700000000000,"end_ms":1700003600000}

data: {"type":"tree_building_start","total_leaves":8,"height":3}

data: {"type":"node_summary","node_id":8,"level":1,...}
...
data: {"type":"tree_complete","node_count":15,"height":3}

data: {"type":"traversal_start","question":"What did the robot do?"}

data: {"type":"path_chosen","level":0,"chosen_child_id":12,"probability":0.75,...}
...
data: {"type":"leaf_reached","segment_id":104,...}

data: {"type":"downloading_video","segment_id":104}

data: {"type":"calling_video_llm","question":"What did the robot do?"}

data: {"type":"answer_ready","answer":"The robot picked up...","segments":[...],"verified":true}
```

### 4. Test cache hit

Run the same curl command again. You should see `cache_hit` instead of `tree_building_start`:

```
data: {"type":"cache_hit","tree_id":"a3f1b2c4d5e6f789"}
```

### 5. Test via the browser UI

1. Open `stream-viewer/robot.html?id=reachy-003&mode=view`
2. Select a date range with annotated segments
3. Click "Ask a Question"
4. Type a question and click Submit
5. Watch the progress panel update in real time
6. Verify the answer appears (with or without the "unverified" tag)

### 6. Test error cases

```bash
# No segments in range → should get error event
curl -N "http://localhost:3001/robots/reachy-003/questions/stream?\
question=test&start_ms=0&end_ms=1"

# Missing descriptions → should get error about unannotated segments
# (use a robot/range where annotation hasn't run)
```
