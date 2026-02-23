# frame-bucket

Multi-robot camera frame pipeline for streaming, storing, searching, and labelling video data. Captures MJPEG or H.264 frames from robot cameras, filters out redundant frames using perceptual hashing (JPEG) or frame-size spike detection (H.264), and stores unique frames in RustFS (S3-compatible object storage). A background eviction task archives old data to AWS S3 when local disk exceeds the configured threshold.

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

You can browse stored frames at the RustFS console: **http://localhost:9001** (login: `rustfsadmin` / `rustfsadmin`).

## Prerequisites

- Docker & Docker Compose
- Rust toolchain (`cargo`)
- Python 3 with `boto3`, `opencv-python`, `numpy` (for helper scripts)
- Camera daemon running on the robot (Reachy: port 8000, BracketBot: port 8003)
- **ffmpeg** with libx264/libx265 support (required for ACTIVE mode video encoding):

```bash
# Ubuntu / Debian (including Jetson)
sudo apt install ffmpeg

# Verify H.264 and H.265 encoders are available
ffmpeg -encoders 2>/dev/null | grep -E "libx26[45]"
# Expected output:
#  V..... libx264     libx264 H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10
#  V..... libx265     libx265 H.265 / HEVC
```

If ffmpeg is missing or encoders are absent, the pipeline will log a warning at startup and fall back to IDLE-only mode (no MP4 encoding).

## Running

### 1. Start infrastructure

```bash
cd frame-bucket
docker compose up -d
```

This starts:
- **RustFS** on `localhost:9000` (S3 API) / `localhost:9001` (console)

### 2. Build

```bash
cargo build --release
```

### 3. Configure robots

Edit `config.toml` to add your robots. Each `[[robots]]` entry defines a robot and its camera stream:

```toml
[[robots]]
robot_id = "reachy-001"
stream_url = "http://100.107.96.29:8000/api/camera/stream"
mode = "h264"                    # "mjpeg", "polling", or "h264"
h264_url = "100.107.96.29:9001"  # required when mode = "h264"
fps = 30.0
quality = 80

[[robots]]
robot_id = "bracketbot-001"
stream_url = "http://192.168.1.42:8003/stream"
mode = "mjpeg"
fps = 10.0
```

### 4. Run the pipeline

A single binary handles all robots:

```bash
RUST_LOG=info ./target/release/frame-bucket-pipeline
```

The pipeline spawns a tokio task per robot. Each task captures frames, filters, encodes, and stores to RustFS independently. Add or remove robots by editing `config.toml` and restarting.

### 5. Run the API server

The API server provides REST endpoints for querying segments, managing collections/clips, and proxying video URLs. It reads the same `config.toml` and connects to RustFS + the per-robot SQLite databases created by the pipeline.

```bash
cd frame-bucket
RUST_LOG=info cargo run --release --package frame-bucket-api
```

The API starts on port 8080 (configurable via `config.toml` `[api] port`). Verify it's running:

```bash
curl http://localhost:8080/robots
```

### 6. Serve the stream viewer

The stream viewer is a static HTML/JS frontend. Serve it on a different port (the API is on 8080):

```bash
cd stream-viewer
python3 -m http.server 3000
```

Then open:
- **http://localhost:3000** — live grid view of all streams
- **http://localhost:3000/robot.html?label=MyRobot&url=http://...&robot_id=reachy-001** — single robot view

In the single robot view, click "View Mode" to access the scrubber, clip selection, and collection management. This requires the API server to be running on port 8080.

## Configuration

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


## Verifying Stored Images

### Browse the RustFS console

Open **http://localhost:9001** in a browser. Log in with `rustfsadmin` / `rustfsadmin`. Navigate to the `camera-frames` bucket to browse and preview stored JPEGs, organized under `{robot_id}/`.

### List frames with the helper script

```bash
python3 check_bucket.py
```

Output:
```
Buckets: ['camera-frames']
Objects in camera-frames: 50
Total size: 9,305,750 bytes (8.87 MB)

  reachy-001/camera/2026-02-18/20260218T093616735Z_000008.jpg  (187,721 bytes)
  reachy-001/camera/2026-02-18/20260218T093616882Z_000009.jpg  (187,307 bytes)
  bracketbot-001/camera/2026-02-18/20260218T093617030Z_000010.jpg  (185,412 bytes)
  ...
```

### Download and view a frame

```bash
# Download a single frame via AWS CLI
aws --endpoint-url http://localhost:9000 s3 cp \
  s3://camera-frames/reachy-001/camera/2026-02-18/20260218T093616735Z_000008.jpg \
  ./sample.jpg

open ./sample.jpg        # macOS
# xdg-open ./sample.jpg  # Linux
```

### Download all frames for a robot

```bash
# All camera frames for one robot
aws --endpoint-url http://localhost:9000 s3 sync \
  s3://camera-frames/reachy-001/camera/ \
  ./downloaded-frames/reachy-001/camera/

# All modalities for one robot
aws --endpoint-url http://localhost:9000 s3 sync \
  s3://camera-frames/reachy-001/ \
  ./downloaded-frames/reachy-001/
```

### Compare two frames (verify the filter)

Use `phash_compare.py` to check the hamming distance between two images:

```bash
python3 phash_compare.py ./frame1.jpg ./frame2.jpg
```

Or grab two frames directly from the live camera:

```bash
python3 phash_compare.py --camera --delay 2
```

Output:
```
Hash size:  16x16 = 256 bits
Hamming distance: 31 / 256  (12.1%)
```

If the distance is above your configured threshold (26), the pipeline would accept both frames as distinct. If below, the second frame would be filtered out as redundant.

## AWS Credentials (for S3 archival)

The pipeline's eviction task uploads old frames to AWS S3 when local disk exceeds the threshold. It uses the standard AWS credential chain — no credentials are stored in `config.toml`.

### Setup

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-west-2
```

`.env` is gitignored. Source it before running the pipeline:

```bash
source .env
RUST_LOG=info ./target/release/frame-bucket-pipeline
```

Or inline:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... ./target/release/frame-bucket-pipeline
```

If no credentials are available, S3 uploads will fail (logged as errors) but the pipeline keeps running — frames stay in RustFS and eviction is skipped.

## Stopping

```bash
# Kill pipeline
pkill -f frame-bucket-pipeline

# Stop infrastructure
docker compose down
```
