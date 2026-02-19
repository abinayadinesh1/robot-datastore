#!/usr/bin/env python3
"""Management script for the frame-bucket pipeline."""

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
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


def clear_storage(keep=1):
    """Delete all frames from the RustFS bucket, keeping the N most recent."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    all_objects = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        all_objects.extend(page.get("Contents", []))
    # Sort by key (encodes timestamp) so newest are last
    all_objects.sort(key=lambda o: o["Key"])
    to_delete = all_objects[:-keep] if keep > 0 else all_objects
    total = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": o["Key"]} for o in batch]})
        total += len(batch)
    print(f"Deleted {total}, kept {len(all_objects) - total} from s3://{BUCKET}/{PREFIX}")


def _parse_key_timestamp(key):
    """Parse the embedded UTC timestamp from a frame object key.

    Key format: frames/YYYY-MM-DD/YYYYMMDDTHHMMSSxxxZ_seq.jpg
    where xxx is milliseconds.
    """
    filename = key.split("/")[-1]
    ts_part = filename.split("_")[0]  # e.g. 20260218T093932518Z
    ts_clean = ts_part.rstrip("Z")
    date_part, time_part = ts_clean.split("T")
    hh, mm, ss = time_part[0:2], time_part[2:4], time_part[4:6]
    ms = time_part[6:9] if len(time_part) > 6 else "000"
    return datetime.strptime(
        f"{date_part}T{hh}:{mm}:{ss}.{ms}", "%Y%m%dT%H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)


def _parse_user_dt(s):
    """Parse a user-supplied datetime string, treating it as UTC.

    Accepted formats: YYYY-MM-DD  or  YYYY-MM-DDTHH:MM:SS
    """
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(
        f"Unrecognized datetime {s!r}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS (UTC)."
    )


def delete_bucket():
    """Delete every object in the bucket, then delete the bucket itself."""
    confirm = input(f"This will permanently delete bucket '{BUCKET}'. Type the bucket name to confirm: ")
    if confirm.strip() != BUCKET:
        print("Aborted.")
        return
    clear_storage(keep=0)
    s3 = _s3_client()
    s3.delete_bucket(Bucket=BUCKET)
    print(f"Bucket '{BUCKET}' deleted.")


def clear_range(from_dt=None, to_dt=None):
    """Delete frames whose timestamps fall within [from_dt, to_dt] (UTC).

    Either bound may be omitted to leave that end open.
    """
    if from_dt is None and to_dt is None:
        print("Provide at least --from or --to.")
        return
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            try:
                ts = _parse_key_timestamp(obj["Key"])
            except (ValueError, IndexError):
                continue
            if from_dt and ts < from_dt:
                continue
            if to_dt and ts > to_dt:
                continue
            to_delete.append(obj)
    total = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
        )
        total += len(batch)
    lo = from_dt.isoformat() if from_dt else "beginning"
    hi = to_dt.isoformat() if to_dt else "now"
    print(f"Deleted {total} frames in range [{lo}, {hi}].")


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
    # Parse flags: --keep N, --from DT, --to DT
    keep = 1
    from_dt = None
    to_dt = None
    filtered = []
    i = 1
    while i < len(sys.argv):
        a = sys.argv[i]
        if a == "--keep":
            i += 1
            keep = int(sys.argv[i])
        elif a == "--from":
            i += 1
            from_dt = _parse_user_dt(sys.argv[i])
        elif a == "--to":
            i += 1
            to_dt = _parse_user_dt(sys.argv[i])
        elif a == "--hard-reset":
            pass
        else:
            filtered.append(a)
        i += 1
    args = filtered

    commands = {
        "clear": lambda: clear_storage(keep=keep),
        "clear-range": lambda: clear_range(from_dt=from_dt, to_dt=to_dt),
        "delete-bucket": lambda: delete_bucket(),
        "start-producer": lambda: start_producer(hard_reset),
        "start-consumer": lambda: start_consumer(hard_reset),
        "start": lambda: (start_producer(hard_reset), start_consumer(hard_reset)),
        "timestamps": lambda: list_timestamps(),
    }

    if not args or args[0] not in commands:
        print("Usage: python3 manage.py <command> [options]")
        print()
        print("Commands:")
        print("  clear              Delete all frames, keeping N most recent (default 1)")
        print("                       --keep N")
        print("  clear-range        Delete frames in a time range (UTC)")
        print("                       --from YYYY-MM-DD[THH:MM:SS]")
        print("                       --to   YYYY-MM-DD[THH:MM:SS]")
        print("  delete-bucket      Remove ALL objects then delete the bucket (with confirmation)")
        print("  start-producer     Start the frame producer")
        print("  start-consumer     Start the frame consumer")
        print("  start              Start both producer and consumer")
        print("                       --hard-reset  (kill existing before starting)")
        print("  timestamps         List timestamps of all frames in the bucket")
        sys.exit(1)

    commands[args[0]]()
