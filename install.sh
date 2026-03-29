echo "building. this may take a few minutes"
cargo build --release
mv frame-bucket-api.service frame-bucket-pipeline.service stream-viewer.service /etc/systemd/system/
echo "Moved system services to /etc/systemd/system"
systemctl restart frame-bucket-pipeline.service
echo "Started Pipeline"
systemctl restart frame-bucket-api.service
echo "Started API"
systemctl restart stream-viewer.service
echo "Started Stream Viewer"
