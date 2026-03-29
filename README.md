# Robot Observatory

The goal of the robot observatory is to provide a single place to monitor, control, and use the memories of different robot embodiments. Official support means there is a stream daemon and control daemon written for the robot that lets you view its sensor streams and control its actuators from the stream-viewer. This is currently implemented for Reachy Mini and Bracket Bot. If you have a robot you want to add support to, write to the maintainers via this google form.

---

## Quick Start

```bash
git clone https://github.com/abinayadinesh1/robot-datastore.git
cd robot-datastore
./install.sh
```

Builds the release target, configures three system services for your user and install path, and starts them. Navigate to **http://localhost:3000** to access the stream.

---

## Stream Viewer

The stream viewer does all the work to let the client easily interact with the robot. Core features are:

- Enables livestream viewing of sensor data. Is the robot up and healthy? Is it stuck on any tasks?
- Ability to control robots via WASD or Teleoperation via a socket to the robot itself. 'Rescue'
- Ability to dynamically configure the robot fleet (add and remove robots from the pipeline).
- Enabling seamless playback of robot sensor data.
- Ability to make 'datasets' or collections of sensor data, with captions. For example, to easily go over human demonstrator rollouts and pick and choose the episodes you want to turn into a dataset.

**index.html** — live grid view of all robot camera streams with filtering

**robot.html** — single robot view with Live Feed mode and View Mode (scrubber playback, clip selection, collections, data labeling)

---

## frame-bucket-api

This is the main API that lets the web client talk to the storage backend. It is the skeleton enabling video-playback, perusing through clips, downloading them, and getting live stats of video streams. The form follows function.

```
api/src/
├── main.rs          # AppState, router, main()
├── types.rs         # definition of request/response structs
├── db.rs            # open_robot_db(), row_to_segment() -> for accessing the SQLite
├── s3.rs            # ensure_bucket(), browse_bucket(), download_segment_file()
├── handlers/
│   ├── mod.rs
│   ├── segments.rs  # list_robots, list/get/patch/delete segments, video_redirect, image_proxy
│   ├── timeline.rs  # get_timeline, get_active_dates
│   ├── collections.rs # list/create/get/delete collections
│   ├── clips.rs     # list/create/delete clips, download_info
│   ├── keyframes.rs # get_sampled_keyframes
│   ├── download.rs  # download_collection + stitch helpers (run_ffmpeg, stitch_clip, etc.)
│   └── health.rs    # get_health, browse_rustfs/s3/streams, get_disk_usage
├── llm_client.rs    # (already exists)
├── qa_tree.rs       # (already exists)
└── qa_handler.rs    # (already exists)
```

---

## frame-bucket-pipeline

This is doing the bulk of the work. The goal of the pipeline is to take images coming from various robots and filter them at the source, storing only useful information locally, then in the cloud. In pursuit of that, it completes various independent functions:

**1. Eviction**

An eviction process that takes data stored locally on the server and ejects it to S3. The user can configure a parameter that modulates how fast data gets evicted. This depends on how long the user wants data to live locally, how much local storage they have, and how frequently they're accessing playback features. I keep this parameter at 0 when I don't want any data saved locally.

- Data is not deleted locally until it is successfully evicted.
- You can set a parameter of when you want to start evicting (say, at 5Gb locally stored) and when you want to end evicting (something lower, like 1Gb). When you evict, do it all at once.

**2. Local Storage (RustFS)**

A RustFS storage container for local backups. All data is saved here as soon as it goes through the filter, so even if network connection is bad or you don't want to save in the cloud, you can store all sensor data completely locally.

- Local storage is a circular buffer, so the native behavior is to keep the most recent data if local storage runs out and eviction isn't configured.
- The stream viewer will tell you local storage capacity. A good feature to add would be sending notifications when local storage is almost full.
- RustFS is a one-to-one mirror of S3 storage. We have one bucket for each robot.
- Implementations for get, put, delete, insert on images and videos are defined in `frame-bucket/pipeline/src/storage.rs`

**3. SQLite DB**

Manages a small SQLite DB for each bucket. This stores S3 object keys, timestamps, and text annotations of video data for easy indexing and retrieval. For example, when the stream viewer playback needs to grab video + images at the position the user clicked, it queries this DB to get the S3 keys.

**4. Video Annotation**

When new video segments are added to RustFS, they are annotated by a video model to return a text description. We spawn a new task for each segment and propagate results to the SQLite DB.

- Read `frame-bucket/pipeline/src/annotator.rs` for implementation.
- This uses the InternVideo VideoChat-Flash model. The model is meant for long context videos, but we only send it segments up to 1 min. The max length of the video is another tunable parameter.

---

## Robot Daemon

To use robot-datastore, all you need is a video stream of images from your robot. You can either use JPEG encoded images in a MJPEG stream or H.264 encoded videos.

- Currently tested and officially supported on Bracket Bot and Reachy Mini.
- Also compatible with any RPI cameras (aka imx708 sensors) and v4l2 devices.
- For Reachy Mini, simply install the associated Reachy-mini app and add the stream to your stream viewer to get started. For Bracket Bot, simply run `uv run camera_stream.py` and plug the stream link into the stream viewer.

Reach out if you have a robot that you want to add integration to!
