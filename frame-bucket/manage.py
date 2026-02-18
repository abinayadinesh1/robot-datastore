#!/usr/bin/env python3
"""Management script for the frame-bucket pipeline."""

import os
import signal
import subprocess
import sys
import time
import boto3
from botocore.config import Config

S3_ENDPOINT = "http://100.81.222.59:9000"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"
BUCKET = "camera-frames"
PREFIX = "frames/"

PRODUCER_BIN = "./target/release/frame-bucket-producer"
CONSUMER_BIN = "./target/release/frame-bucket-consumer"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def clear_storage():
    """Delete all frames from the RustFS bucket."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_req = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3.delete_objects(Bucket=BUCKET, Delete=delete_req)
        total += len(objects)
    print(f"Deleted {total} objects from s3://{BUCKET}/{PREFIX}")


def _is_running(name):
    """Check if a process matching `name` is running."""
    result = subprocess.run(
        ["pgrep", "-f", name], capture_output=True, text=True
    )
    return result.returncode == 0


def _kill(name):
    """Kill all processes matching `name`."""
    subprocess.run(["pkill", "-f", name], capture_output=True)
    time.sleep(0.5)
    # Force kill if still alive
    if _is_running(name):
        subprocess.run(["pkill", "-9", "-f", name], capture_output=True)
        time.sleep(0.3)
    print(f"Killed {name}.")


def _start_bin(bin_path, name):
    subprocess.Popen(
        [bin_path],
        env={**os.environ, "RUST_LOG": "info"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"{name} started.")


def start_producer(hard_reset=False):
    """Start the producer. With hard_reset, kill existing first."""
    if hard_reset and _is_running("frame-bucket-producer"):
        _kill("frame-bucket-producer")
    elif _is_running("frame-bucket-producer"):
        print("Producer is already running.")
        return
    _start_bin(PRODUCER_BIN, "Producer")


def start_consumer(hard_reset=False):
    """Start the consumer. With hard_reset, kill existing first."""
    if hard_reset and _is_running("frame-bucket-consumer"):
        _kill("frame-bucket-consumer")
    elif _is_running("frame-bucket-consumer"):
        print("Consumer is already running.")
        return
    _start_bin(CONSUMER_BIN, "Consumer")


def list_timestamps():
    """Print timestamps of all consumed frames in the bucket."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            # Key format: frames/YYYY-MM-DD/YYYYMMDDTHHMMSS{ms}Z_{seq}.jpg
            filename = obj["Key"].split("/")[-1]
            ts_part = filename.split("_")[0]  # e.g. 20260218T093932518Z
            print(f"  {ts_part}  ({obj['Size']:,} bytes)  {obj['Key']}")
            count += 1
    print(f"\nTotal: {count} frames")


if __name__ == "__main__":
    hard_reset = "--hard-reset" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--hard-reset"]

    commands = {
        "clear": lambda: clear_storage(),
        "start-producer": lambda: start_producer(hard_reset),
        "start-consumer": lambda: start_consumer(hard_reset),
        "start": lambda: (start_producer(hard_reset), start_consumer(hard_reset)),
        "timestamps": lambda: list_timestamps(),
    }

    if not args or args[0] not in commands:
        print(f"Usage: python3 manage.py <{'|'.join(commands)}> [--hard-reset]")
        sys.exit(1)

    commands[args[0]]()
