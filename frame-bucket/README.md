# frame-bucket

Kafka-driven camera frame pipeline for streaming, storing, searching, and labelling video data. Captures MJPEG frames from a robot's camera daemon, filters out redundant frames using perceptual hashing, and stores unique frames in RustFS (S3-compatible object storage) in a circular buffer. Data is saved locally until 80% full disk, upon which oldest data is backed up to an S3 bucket.

## Architecture

```
[Robot A camera :8000]                   [RustFS :9000]            [AWS S3]
        | MJPEG stream                         ^  |                      ^
        v                                      |  |                      |
+-- Producer (robot_id="reachy-001") --+       |  |                      |
| Parse MJPEG ->                        |       |  |                      |
| key="reachy-001:{timestamp_ms}"       |       |  |                      |
| Produce to Kafka "camera.frames"      |       |  |                      |
+-------+-------------------------------+       |  |                      |
        |                                       |  |                      |
        +---- Kafka "camera.frames" +-----------+  |                      |
        |     partition 0: reachy-001 frames    |  |                      |
        |     partition 1: bracketbot-001 frames|  |                      |
        |                                       |  |                      |
+-- Consumer --------------------------------+-+  |                      |
| Parse robot_id from Kafka key               |    |                      |
| Filter (aHash) -> Store to RustFS           |    |                      |
| Record segments: ACTIVE (MP4) / IDLE (JPEG) |    |                      |
| SQLite: {robot_id}.db per robot             |    |                      |
| Monitor disk -> Evict oldest to AWS         |    |                      |
+---------------------------------------------+   |                      |
                                                   |                      |
+-- API Server :8080 -----------------------------+|                      |
| /robots/{id}/segments   - list/query segments    |                      |
| /robots/{id}/timeline   - scrubber time range    |                      |
| /robots/{id}/collections - CRUD collections      |                      |
| /robots/{id}/collections/{id}/clips - CRUD clips |                      |
| Proxies video URLs -> RustFS                     |                      |
| Writes clip manifests -> labelled-data bucket    |                      |
+--------------------------------------------------+                      |
        |                                                                 |
+-- Stream Viewer :3000 -------+                                          |
| index.html - live grid view  |                                          |
| robot.html - live + playback |                                          |
|   View Mode: scrubber, clips |                                          |
|   collections, labeling      |                                          |
+------------------------------+                                          |
```

**Producer** — connects to a camera's MJPEG stream (or polls single frames), wraps each JPEG in a `TimestampedFrame` (8-byte timestamp + 8-byte seq + JPEG payload), sets the Kafka message key to `{robot_id}:{timestamp_ms}`, and publishes to the shared `camera.frames` topic.

**Kafka partitioning** — the `robot_id` prefix in the message key routes all frames from a given robot to the same partition. This guarantees per-robot ordering (sequence numbers are meaningful) and ensures the perceptual hash filter only compares frames from the same robot — never across robots.

**Consumer** — reads from Kafka, extracts `robot_id` from the message key, runs each frame through an aHash perceptual hash filter (16x16 grid = 256 bits, hamming distance comparison), and stores frames that differ enough from the last accepted frame into RustFS under a per-robot path. A background eviction task monitors disk usage and archives old frames to AWS S3 when disk exceeds 80%.

## Project Structure

```
frame-bucket/
├── config.toml              # runtime configuration
├── docker-compose.yml       # Kafka + RustFS containers
├── producer/                # MJPEG ingestion -> Kafka
├── consumer/                # Kafka -> filter -> RustFS storage
│   └── src/
│       ├── filter/
│       │   ├── phash.rs     # perceptual hash (primary)
│       │   └── histogram.rs # histogram comparison (alt)
│       ├── recorder/
│       │   └── encoder.rs   # FFmpeg MP4 segment encoding
│       ├── db.rs            # SQLite: segments, collections, clips
│       ├── storage.rs       # RustFS S3 client
│       └── eviction.rs      # disk monitor + AWS S3 archival
├── api/                     # HTTP API server (Axum)
│   └── src/main.rs          # REST endpoints for segments, collections, clips
├── common/                  # shared config + frame serialization
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

If ffmpeg is missing or encoders are absent, the consumer will log a warning at startup and fall back to IDLE-only mode (no MP4 encoding).

## Running

### 1. Start infrastructure

```bash
cd frame-bucket
docker compose up -d
```

This starts:
- **Kafka** (KRaft mode) on `localhost:9092`
- **RustFS** on `localhost:9000` (S3 API) / `localhost:9001` (console)

### 2. Create the Kafka topic (first time only)

Create with enough partitions to accommodate your robot fleet (one partition per robot for maximum parallelism):

```bash
docker exec frame-bucket-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic camera.frames --partitions 3
```

### 3. Install Deps and Build

```bash
sudo apt install libcurl4-openssl-dev cmake pkg-config -y
cargo build --release
```

### 4. Run producers and consumer

Each robot runs its own producer pointed at its camera. All producers publish to the same Kafka topic. One consumer handles all robots.

```bash
# Robot A producer — robot_id overrides aws_s3.robot_id in config.toml
RUST_LOG=info ./target/release/frame-bucket-producer reachy-001

# Robot B producer
RUST_LOG=info ./target/release/frame-bucket-producer bracketbot-001

# Consumer (filters + stores to RustFS, handles all robots)
RUST_LOG=info ./target/release/frame-bucket-consumer
```

Both binaries load `config.toml` from the current directory. Both producer args are optional and fall back to config values if omitted:

```
frame-bucket-producer [robot_id] [stream_url]
```

With a single shared `config.toml`, each robot just needs its identity and camera URL at launch:

```bash
./target/release/frame-bucket-producer reachy-001 http://100.107.96.29:8000/api/camera/stream
./target/release/frame-bucket-producer bracketbot-001 http://192.168.1.42:8003/stream
```

### 5. Run the API server

The API server provides REST endpoints for querying segments, managing collections/clips, and proxying video URLs. It reads the same `config.toml` and connects to RustFS + the per-robot SQLite databases created by the consumer.

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

Edit `config.toml` (or a per-robot variant) to tune behavior. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `aws_s3.robot_id` | `"reachy-001"` | Robot identifier. Used as the Kafka partition key prefix and the RustFS path prefix (`frames/{robot_id}/`). Must be unique per robot. |
| `filter.phash_threshold` | 26 | Hamming distance threshold (out of 256 bits). Higher = stricter filtering. 26 ~ 10% difference. |
| `filter.phash_hash_size` | 16 | Hash grid size. 16x16 = 256-bit hash. |
| `stream.url` | — | Camera stream URL. Reachy: `http://<ip>:8000/api/camera/stream`. BracketBot: `http://<ip>:8003/stream`. |
| `stream.mode` | `"mjpeg"` | `"mjpeg"` for streaming, `"polling"` for single-frame polling. |
| `stream.fps` | 10.0 | Target FPS for stream/poll rate. |
| `eviction.threshold_percent` | 80.0 | Disk usage % that triggers eviction to AWS S3. |


## Verifying Stored Images

### Browse the RustFS console

Open **http://localhost:9001** in a browser. Log in with `rustfsadmin` / `rustfsadmin`. Navigate to the `camera-frames` bucket to browse and preview stored JPEGs, organized under `frames/{robot_id}/`.

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

If the distance is above your configured threshold (26), the consumer would accept both frames as distinct. If below, the second frame would be filtered out as redundant.

## AWS Credentials (for S3 archival)

The consumer's eviction task uploads old frames to AWS S3 when local disk exceeds the threshold. It uses the standard AWS credential chain — no credentials are stored in `config.toml`.

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

`.env` is gitignored. Source it before running the consumer:

```bash
source .env
RUST_LOG=info ./target/release/frame-bucket-consumer
```

Or inline:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... ./target/release/frame-bucket-consumer
```

If no credentials are available, S3 uploads will fail (logged as errors) but the consumer keeps running — frames stay in RustFS and eviction is skipped.

## Stopping

```bash
# Kill producer/consumer
pkill -f frame-bucket-producer
pkill -f frame-bucket-consumer

# Stop infrastructure
docker compose down
```
