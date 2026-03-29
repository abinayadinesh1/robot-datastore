# frame-bucket

Multi-robot camera frame pipeline for streaming, storing, searching, and labelling video data. Captures MJPEG or H.264 frames from robot cameras, filters out redundant frames using perceptual hashing (JPEG) or frame-size spike detection (H.264), and stores unique frames in RustFS (S3-compatible object storage). A background eviction task archives old data to AWS S3 when local disk exceeds the configured threshold.

See [the root README](../README.md) for setup and reproduction steps.

---

## Architecture

```
[Robot A camera :8000]                   [RustFS :9000]            [AWS S3]
        | MJPEG / H.264 stream                ^  |                      ^
        v                                     |  |                      |
[Robot B camera :8000]                        |  |                      |
        | MJPEG / H.264 stream                |  |                      |
        v                                     |  |                      |
+-- Pipeline (multi-robot) -----------------+-+  |                      |
| Per robot (tokio task):                    |    |                      |
|   Capture stream (MJPEG/polling/H.264)     |    |                      |
|   -> mpsc channel (backpressure)           |    |                      |
|   -> Filter (aHash or frame-size spike)    |    |                      |
|   -> Record: ACTIVE (MP4) / IDLE (JPEG)    |    |                      |
|   -> Store to RustFS                       |    |                      |
|   -> SQLite: {robot_id}.db per robot       |    |                      |
| Shared:                                    |    |                      |
|   Eviction loop -> archive to AWS S3       |    |                      |
+--------------------------------------------+   |                      |
                                                  |                      |
+-- API Server :8080 ----------------------------+|                      |
| /robots/{id}/segments   - list/query segments   |                      |
| /robots/{id}/timeline   - scrubber time range   |                      |
| /robots/{id}/collections - CRUD collections     |                      |
| /robots/{id}/collections/{id}/clips - CRUD clips|                      |
| Proxies video URLs -> RustFS                    |                      |
| Writes clip manifests -> labelled-data bucket   |                      |
+-------------------------------------------------+                      |
        |                                                                |
+-- Stream Viewer :3000 -------+                                         |
| index.html - live grid view  |                                         |
| robot.html - live + playback |                                         |
|   View Mode: scrubber, clips |                                         |
|   collections, labeling      |                                         |
+------------------------------+                                         |
```

**Pipeline** — a single binary that manages all robots. Each robot gets its own tokio task with a capture loop, bounded channel for backpressure, filter, recording state machine, and SQLite database. Robots are configured via `[[robots]]` entries in `config.toml`. Supports three capture modes: MJPEG streaming, HTTP polling, and H.264 TCP (MPEG-TS).

**Filtering** — JPEG frames are filtered using aHash perceptual hashing (16x16 grid = 256 bits, hamming distance comparison). H.264 frames use P-frame size spike detection to identify scene changes without decoding pixels.

**Recording** — the state machine switches between IDLE (stores a single representative JPEG) and ACTIVE (encodes frames to 60-second MP4 segments via ffmpeg). Transitions are based on filter output.

**Eviction** — a shared background task monitors RustFS disk usage and archives the oldest objects to AWS S3 when the configured threshold is exceeded. Falls back to delete-only mode if S3 is unreachable.

---

## Project Structure

```
frame-bucket/
├── config.toml              # runtime configuration
├── docker-compose.yml       # RustFS container
├── pipeline/                # capture -> filter -> RustFS (single binary)
│   └── src/
│       ├── capture/
│       │   ├── mjpeg.rs     # MJPEG stream + HTTP polling capture
│       │   └── h264.rs      # H.264 TCP (MPEG-TS) capture
│       ├── filter/
│       │   ├── phash.rs     # perceptual hash (primary for JPEG)
│       │   ├── framesize.rs # P-frame spike detection (for H.264)
│       │   └── histogram.rs # histogram comparison (alt)
│       ├── recorder/
│       │   ├── state.rs     # IDLE/ACTIVE state machine
│       │   ├── encoder.rs   # FFmpeg MP4 segment encoding
│       │   └── keys.rs      # RustFS object key generation
│       ├── db.rs            # SQLite: segments, collections, clips
│       ├── storage.rs       # RustFS S3 client
│       └── eviction.rs      # disk monitor + AWS S3 archival
├── api/                     # HTTP API server (Axum)
│   └── src/main.rs          # REST endpoints for segments, collections, clips
├── common/                  # shared config + frame types
├── check_bucket.py          # inspect stored frames
└── phash_compare.py         # compare two images with aHash

stream-viewer/               # frontend (vanilla HTML/JS/CSS)
├── index.html               # live grid view of all robot streams
├── robot.html               # single robot: live feed + View Mode
├── sources.json             # stream sources configuration
└── styles.css               # shared styles
```

---

## Where Are Frames Stored?

Frames are stored in RustFS (S3-compatible, running at `localhost:9000`) in the bucket `camera-frames`, organized by robot ID, modality, and date:

```
s3://camera-frames/{robot_id}/camera/{YYYY-MM-DD}/{YYYYMMDD}T{HHMMSS}{ms}Z_{seq:06}.jpg
```

Example with two robots:
```
reachy-001/camera/2026-02-18/20260218T093616735Z_000008.jpg
reachy-001/camera/2026-02-18/20260218T093617030Z_000010.jpg
bracketbot-001/camera/2026-02-18/20260218T093616882Z_000009.jpg
```

AWS S3 archives mirror this structure under the configured prefix:
```
archive/reachy-001/camera/2026-02-18/20260218T093616735Z_000008.jpg
archive/bracketbot-001/camera/2026-02-18/20260218T093616882Z_000009.jpg
```

The robot-first hierarchy means you can efficiently list all data for a robot across all modalities with a single prefix query (`reachy-001/`), or narrow to a specific sensor (`reachy-001/camera/`).

Browse stored frames at the RustFS console: **http://localhost:9001** (login: `rustfsadmin` / `rustfsadmin`).

---

## Configuration Reference

Edit `config.toml` to tune behavior. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `robots[].robot_id` | — | Robot identifier. Used as the RustFS path prefix (`{robot_id}/camera/`). Must be unique per robot. |
| `robots[].stream_url` | — | Camera stream URL. Reachy: `http://<ip>:8000/api/camera/stream`. |
| `robots[].mode` | `"mjpeg"` | `"mjpeg"` for streaming, `"polling"` for single-frame polling, `"h264"` for H.264 TCP. |
| `robots[].fps` | 10.0 | Target FPS for stream/poll rate. |
| `filter.phash_threshold` | 26 | Hamming distance threshold (out of 256 bits). Higher = stricter filtering. 26 ~ 10% difference. |
| `filter.spike_ratio` | 4.0 | H.264 P-frame size spike ratio for activity detection. |
| `eviction.threshold_gb` | 50.0 | RustFS storage size (GB) that triggers eviction to AWS S3. |
| `eviction.target_gb` | 40.0 | Evict until storage drops below this (GB). |
| `recording.segment_duration_secs` | 120 | Length of each MP4 segment in ACTIVE mode. |
| `recording.codec` | `"h264"` | Codec for segment encoding: `"h264"` or `"h265"`. |
| `recording.crf` | 23 | Quality setting; lower = better. Typical range 18–28. |

---

## How Filtering Works

### JPEG / MJPEG — Perceptual Hash (aHash)

Each incoming frame is resized to a 16×16 grayscale grid and its average hash (aHash) computed — 256 bits total. The hamming distance between the new frame's hash and the last stored frame's hash determines whether the frame is novel enough to keep.

- Distance < `phash_threshold` → frame is redundant, dropped
- Distance ≥ `phash_threshold` → frame is unique, stored

A threshold of 26 (≈10% of 256 bits) works well for typical robot camera footage. Lower the threshold if too many similar frames are being stored; raise it if too many unique frames are being dropped.

Use `phash_compare.py` to inspect the distance between any two images:
```bash
python3 phash_compare.py ./frame1.jpg ./frame2.jpg
# Hash size:  16x16 = 256 bits
# Hamming distance: 31 / 256  (12.1%)
```

Or compare two frames from the live camera with a delay:
```bash
python3 phash_compare.py --camera --delay 2
```

### H.264 — Frame-Size Spike Detection

For H.264 streams, decoding every frame to compute a pixel hash is expensive. Instead, the pipeline monitors the encoded size of P-frames. A large P-frame (relative to recent average) signals a scene change.

- P-frame size / rolling average > `spike_ratio` → activity detected, transition to ACTIVE
- Otherwise → IDLE

This avoids full decoding while still detecting meaningful motion.

---

## How the IDLE/ACTIVE State Machine Works

Each robot's recording state machine has two modes:

- **IDLE** — the scene is static. The pipeline stores a single representative JPEG snapshot. No video is being encoded. Transitions to ACTIVE when the filter detects novel frames.
- **ACTIVE** — the scene is changing. Frames are piped into ffmpeg and encoded into MP4 segments of configurable duration (`recording.segment_duration_secs`). After `active_to_idle_consecutive_frames` consecutive non-novel frames, transitions back to IDLE.

This means storage is proportional to actual activity — a robot sitting still generates one JPEG per interval, not gigabytes of static video.

---

## How Eviction Works

The eviction task runs every `check_interval_secs` seconds and checks the total size of objects in RustFS:

1. If size > `threshold_gb`: begin evicting the oldest objects (by timestamp in the key) in batches of `batch_size`.
2. Each evicted object is uploaded to AWS S3 under the configured prefix, then deleted from RustFS.
3. Eviction continues until RustFS size drops below `target_gb`.
4. If S3 is unreachable: falls back to delete-only mode. Objects are only deleted locally once RustFS exceeds `fallback_threshold_gb` — this preserves data as long as possible while preventing the disk from filling.

Data is **never deleted locally until it has been successfully confirmed uploaded to S3** (unless in fallback mode and over the fallback threshold).

---

## Helper Scripts

**`check_bucket.py`** — lists all objects in RustFS with sizes and timestamps. Useful for verifying the pipeline is storing frames.

**`phash_compare.py`** — computes the aHash hamming distance between two images. Use this to calibrate `filter.phash_threshold` for your camera.

Download a frame for inspection:
```bash
aws --endpoint-url http://localhost:9000 --no-sign-request \
  s3 cp s3://camera-frames/reachy-001/camera/2026-02-18/20260218T093616735Z_000008.jpg ./sample.jpg
```

Sync all frames for a robot locally:
```bash
aws --endpoint-url http://localhost:9000 --no-sign-request \
  s3 sync s3://camera-frames/reachy-001/ ./downloaded-frames/reachy-001/
```
