# frame-bucket

Kafka-driven camera frame pipeline for Reachy Mini. Captures MJPEG frames from the robot's camera daemon, filters out redundant frames using perceptual hashing, and stores unique frames in RustFS (S3-compatible object storage).

## Architecture

```
[Camera Daemon :8000]                    [RustFS :9000]        [AWS S3]
        | MJPEG stream                        ^                    ^
        v                                     |                    |
+-- Producer ------+    +-- Consumer ---------+--------------------+
|                   |    |                                          |
| Parse MJPEG ->    |    | Filter (aHash) -> Store to RustFS       |
| Produce to Kafka  |    |                   Monitor disk -> Evict  |
| "camera.frames"   |    |                   oldest to AWS S3       |
+-------+-----------+    +----------+------------------------------|
        |                           |
        +---- Kafka Topic ----------+
              "camera.frames"
```

**Producer** — connects to the camera's MJPEG stream (or polls single frames), wraps each JPEG in a `TimestampedFrame` (8-byte timestamp + 8-byte seq + JPEG payload), and publishes to Kafka.

**Consumer** — reads from Kafka, runs each frame through an aHash perceptual hash filter (16x16 grid = 256 bits, hamming distance comparison), and stores frames that differ enough from the last accepted frame into RustFS. A background eviction task monitors disk usage and archives old frames to AWS S3 when disk exceeds 80%.

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
│       ├── storage.rs       # RustFS S3 client
│       └── eviction.rs      # disk monitor + AWS S3 archival
├── common/                  # shared config + frame serialization
├── check_bucket.py          # inspect stored frames
└── phash_compare.py         # compare two images with aHash
```

## Where Are Frames Stored?

Frames are stored in RustFS (S3-compatible, running at `localhost:9000`) in the bucket `camera-frames`:

```
s3://camera-frames/frames/{YYYY-MM-DD}/{YYYYMMDD}T{HHMMSS}{ms}Z_{seq:06}.jpg
```

Example:
```
frames/2026-02-18/20260218T093616735Z_000008.jpg
frames/2026-02-18/20260218T093617030Z_000010.jpg
```

You can browse stored frames at the RustFS console: **http://localhost:9001** (login: `rustfsadmin` / `rustfsadmin`).

## Prerequisites

- Docker & Docker Compose
- Rust toolchain (`cargo`)
- Python 3 with `boto3`, `opencv-python`, `numpy` (for helper scripts)
- Camera daemon running on port 8000 (on the Pi or locally)

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

```bash
docker exec frame-bucket-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic camera.frames --partitions 3
```

### 3. Build

```bash
cargo build --release
```

### 4. Run producer and consumer

In separate terminals (or background them):

```bash
# Terminal 1 — producer (connects to camera stream)
RUST_LOG=info ./target/release/frame-bucket-producer

# Terminal 2 — consumer (filters + stores to RustFS)
RUST_LOG=info ./target/release/frame-bucket-consumer
```

Both binaries read `config.toml` from the current directory by default, or pass a path as the first argument:

```bash
./target/release/frame-bucket-consumer /path/to/config.toml
```

## Configuration

Edit `config.toml` to tune behavior. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `filter.phash_threshold` | 26 | Hamming distance threshold (out of 256 bits). Higher = stricter filtering. 26 ~ 10% difference. |
| `filter.phash_hash_size` | 16 | Hash grid size. 16x16 = 256-bit hash. |
| `stream.mode` | `mjpeg` | `"mjpeg"` for streaming, `"polling"` for single-frame polling |
| `stream.fps` | 10.0 | Target FPS for stream/poll rate |
| `eviction.threshold_percent` | 80.0 | Disk usage % that triggers eviction to AWS S3 |

## Verifying Stored Images

### Browse the RustFS console

Open **http://localhost:9001** in a browser. Log in with `rustfsadmin` / `rustfsadmin`. Navigate to the `camera-frames` bucket to browse and preview stored JPEGs.

### List frames with the helper script

```bash
python3 check_bucket.py
```

Output:
```
Buckets: ['camera-frames']
Objects in camera-frames: 50
Total size: 9,305,750 bytes (8.87 MB)

  frames/2026-02-18/20260218T093616735Z_000008.jpg  (187,721 bytes)
  frames/2026-02-18/20260218T093616882Z_000009.jpg  (187,307 bytes)
  ...
```

### Download and view a frame

```bash
# Download a single frame via AWS CLI
aws --endpoint-url http://localhost:9000 s3 cp \
  s3://camera-frames/frames/2026-02-18/20260218T093616735Z_000008.jpg \
  ./sample.jpg

# Open it
open ./sample.jpg        # macOS
# xdg-open ./sample.jpg  # Linux
```

### Download all frames for a date

```bash
aws --endpoint-url http://localhost:9000 s3 sync \
  s3://camera-frames/frames/2026-02-18/ \
  ./downloaded-frames/
```

Then open the folder to scroll through images visually.

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

## Stopping

```bash
# Kill producer/consumer
pkill -f frame-bucket-producer
pkill -f frame-bucket-consumer

# Stop infrastructure
docker compose down
```
