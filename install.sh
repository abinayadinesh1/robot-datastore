#!/bin/bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_USER="$(whoami)"

echo "Building... this may take a few minutes"
cd "$REPO_DIR/frame-bucket"
cargo build --release

echo "Configuring system services..."
for SERVICE in frame-bucket-api frame-bucket-pipeline stream-viewer; do
    sed \
        -e "s|REPO_DIR|$REPO_DIR|g" \
        -e "s|INSTALL_USER|$INSTALL_USER|g" \
        "$REPO_DIR/$SERVICE.service" \
    | sudo tee "/etc/systemd/system/$SERVICE.service" > /dev/null
done

sudo systemctl daemon-reload
sudo systemctl restart frame-bucket-pipeline.service
echo "Started Pipeline"
sudo systemctl restart frame-bucket-api.service
echo "Started API"
sudo systemctl restart stream-viewer.service
echo "Started Stream Viewer"
echo ""
echo "Done! Stream viewer available at http://localhost:3000"
