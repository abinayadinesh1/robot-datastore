# Reachy Mini: HTTP Camera Stream Setup

How to get camera frames from the Reachy Mini over HTTP — no GStreamer required on the client.

## System Info

| Component | Value |
|-----------|-------|
| Board | Raspberry Pi Compute Module 4 Rev 1.1 |
| OS | Debian 13 (trixie), aarch64 |
| Python | 3.13.5 (system), venv at `/venvs/mini_daemon/` |
| reachy-mini | 1.2.5 |
| GStreamer | 1.26.2 |
| GStreamer Rust plugins | `/opt/gst-plugins-rs/lib/aarch64-linux-gnu/` |
| Camera | IMX708 wide (1920x1080 via GStreamer, 4608x2592 native) |
| Daemon port | 8000 (FastAPI) |
| WebRTC signaling port | 8443 |

## Architecture

```
IMX708 camera
    │
    ▼
libcamerasrc → v4l2h264enc → webrtcsink (port 8443)
    │
    ├──► unixfdsink (/tmp/reachymini_camera_socket)
    │         │
    │         ▼
    │    GStreamerCamera (unixfdsrc → v4l2convert → appsink)
    │         │
    │         ▼
    │    FastAPI camera router (port 8000)
    │         │
    │         ├── GET /api/camera/frame   → single JPEG
    │         ├── GET /api/camera/stream  → MJPEG stream
    │         └── GET /api/camera/status  → JSON health check
    │
    └──► WebRTC consumers (requires GStreamer Rust plugins)
```

The WebRTC producer (`GstWebRTC`) runs a GStreamer pipeline that captures from the IMX708 via `libcamerasrc`, encodes to H.264, and sends via `webrtcsink`. It also tees raw frames to a unix socket at `/tmp/reachymini_camera_socket` via `unixfdsink`.

The HTTP camera router reads from that unix socket using `GStreamerCamera` (a separate GStreamer pipeline: `unixfdsrc` → `v4l2convert` → `appsink`), encodes frames to JPEG, and serves them over HTTP. This means **any client with `curl` or a browser can grab frames** — no GStreamer installation needed on the client side.

## Setup (One-Time)

Two files were added/modified on the Pi's installed reachy-mini package:

### 1. New file: camera router

**Path on Pi:** `/venvs/mini_daemon/lib/python3.12/site-packages/reachy_mini/daemon/app/routers/camera.py`

**Source:** [`src/reachy_mini/daemon/app/routers/camera.py`](../src/reachy_mini/daemon/app/routers/camera.py)

This adds three endpoints to the daemon's FastAPI server:

- `GET /api/camera/status` — JSON check: is the camera socket available?
- `GET /api/camera/frame?quality=80` — returns a single JPEG frame
- `GET /api/camera/stream?quality=60&fps=10` — MJPEG stream (viewable in browser)

Key implementation detail: on first request, the `GStreamerCamera` singleton is lazily created. A warmup loop retries `read()` up to 10 times (100ms apart) to let the GStreamer pipeline produce its first frame, avoiding a 503 on cold start.

### 2. Modified file: main.py

**Path on Pi:** `/venvs/mini_daemon/lib/python3.12/site-packages/reachy_mini/daemon/app/main.py`

Two changes made (patched in-place on the v1.2.5 installation):

1. **Import + register camera router** in the `wireless_version` block:
   ```python
   from .routers import camera
   router.include_router(camera.router)
   ```

2. **Cleanup camera reader** in the lifespan `finally` block:
   ```python
   if hasattr(app.state, "_camera_reader") and app.state._camera_reader is not None:
       app.state._camera_reader.close()
   ```

> **Warning:** The local codebase's `main.py` is newer than the Pi's v1.2.5. Do NOT scp the local `main.py` directly to the Pi — it imports `preload_default_datasets` which doesn't exist in v1.2.5 and will break the daemon.

## Reproducing: Start Camera Stream

### Step 1: SSH into the Pi

```bash
ssh pollen@reachy-mini.local
# password: root
```

### Step 2: Restart the daemon service

```bash
sudo systemctl restart reachy-mini-daemon
```

The systemd service runs with `--wireless-version --no-autostart`, meaning the FastAPI server starts but the backend (motors + WebRTC) does NOT auto-start.

### Step 3: Start the daemon via API

Wait ~5 seconds for FastAPI to be ready, then:

```bash
curl -X POST 'http://localhost:8000/api/daemon/start?wake_up=false'
```

- `wake_up=false` — starts the backend and WebRTC producer without waking up motors
- This triggers WebRTC pipeline startup, which creates the camera socket at `/tmp/reachymini_camera_socket`

### Step 4: Verify producer is running

```bash
# Check daemon status
curl -s http://localhost:8000/api/daemon/status | python3 -m json.tool

# Verify camera socket exists
ls -la /tmp/reachymini_camera_socket

# Check camera endpoint status
curl -s http://localhost:8000/api/camera/status
```

Expected: daemon state is `"running"`, socket exists, camera `"available": true`.

## Usage (from Client Mac)

### Grab a single frame

```bash
curl -o frame.jpg 'http://reachy-mini.local:8000/api/camera/frame?quality=80'
open frame.jpg
```

Output: 1920x1080 JPEG, ~70-95KB at quality 80.

### View live stream in browser

Open in any browser:

```
http://reachy-mini.local:8000/api/camera/stream?quality=60&fps=10
```

### Use the Python client script

[`examples/debug/http_frame_grabber.py`](../examples/debug/http_frame_grabber.py) — requires `requests`, `opencv-python`, `numpy`.

```bash
# Check status
python http_frame_grabber.py --host reachy-mini.local --status

# Save a single frame
python http_frame_grabber.py --host reachy-mini.local --save photo.jpg

# Live OpenCV window (press 'q' to quit)
python http_frame_grabber.py --host reachy-mini.local --stream
```

### Alternative: full-resolution capture directly on Pi

For 12MP (4608x2592) images with Exif metadata, use `rpicam-jpeg` directly on the Pi:

```bash
ssh pollen@reachy-mini.local
rpicam-jpeg -o photo.jpg
```

Note: this captures at full sensor resolution, bypassing the GStreamer pipeline entirely.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `503: Camera socket not found` | WebRTC producer hasn't started | Run `curl -X POST 'http://localhost:8000/api/daemon/start?wake_up=false'` and wait ~8s |
| `503: No frame available` | GStreamer pipeline still warming up | Retry after 1-2 seconds (warmup loop handles this on first init) |
| Frame grab works then stops | Dashboard at 192.168.x.x called `/api/daemon/stop` | Restart daemon; consider disabling dashboard auto-stop |
| `Connection refused` on port 8000 | Daemon service not running | `sudo systemctl restart reachy-mini-daemon` |
| Corrupted JPEG from curl | Daemon was stopped mid-stream | Restart daemon and re-start via API |

## Files Reference

| File | Location | Purpose |
|------|----------|---------|
| `camera.py` (router) | `src/reachy_mini/daemon/app/routers/camera.py` | HTTP endpoints for frame/stream |
| `main.py` (daemon) | `src/reachy_mini/daemon/app/main.py` | FastAPI app, registers camera router |
| `http_frame_grabber.py` | `examples/debug/http_frame_grabber.py` | Client-side frame grabber script |
| `camera_gstreamer.py` | `src/reachy_mini/media/camera_gstreamer.py` | GStreamerCamera class (unix socket reader) |
| `webrtc_daemon.py` | `src/reachy_mini/media/webrtc_daemon.py` | WebRTC producer (creates camera socket) |
| `launcher.sh` | `src/reachy_mini/daemon/app/services/wireless/launcher.sh` | Systemd service entry point |
