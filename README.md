## robot-datastore

End-to-end pipeline for capturing, storing, browsing, and labeling robot video data.

### Quick Start

```bash
# 1. Start infrastructure (Kafka + RustFS)
cd frame-bucket && docker compose up -d

# 2. Start the consumer (processes frames, writes to RustFS + SQLite)
RUST_LOG=info cargo run --release --package frame-bucket-consumer

# 3. Start a producer (connects to robot camera stream)
RUST_LOG=info cargo run --release --package frame-bucket-producer -- reachy-001

# 4. Start the API server (REST endpoints for segments, collections, clips)
RUST_LOG=info cargo run --release --package frame-bucket-api

# 5. Serve the frontend (in a separate terminal)
cd stream-viewer && python3 -m http.server 3000
```

Then open **http://localhost:3000** for the live grid view, or click a stream to open the single robot view with playback and labeling.

See [frame-bucket/README.md](frame-bucket/README.md) for full documentation.

### Components

**frame-bucket** — Rust workspace with three binaries:
- **producer** — connects to a robot's MJPEG camera stream, publishes frames to Kafka
- **consumer** — reads from Kafka, filters redundant frames (perceptual hashing), encodes active periods as MP4 segments and idle periods as JPEG snapshots, stores in RustFS (S3-compatible), evicts to AWS S3 when disk is full
- **api** (port 8080) — REST API for querying segments, managing collections and labeled clips, proxying video URLs from RustFS

**stream-viewer** — vanilla HTML/JS/CSS frontend:
- **index.html** — live grid view of all robot camera streams with filtering
- **robot.html** — single robot view with Live Feed mode and View Mode (scrubber playback, clip selection, collections, data labeling)

