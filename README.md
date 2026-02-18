### frame-bucket - a rust crate for consuming and storing video streams. 
- takes a camera stream and turns it into an Apache Kafka producer
- sets up a local server with a S3-compatible filesystem (Rust-Fs)
- creates a consumer on the local server that pushes to the filesystem
- implements basic storage saving features (only saving image frames with new information, pushes data to remote S3 storage after 80% disk full, etc. )

### video-peruser
- loads robot data from object storage (either local or )
- allows you to search through robot history for episodic data
- easy UI for robot operators to clip and label episodes from robot history

  
