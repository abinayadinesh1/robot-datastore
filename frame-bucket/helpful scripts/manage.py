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
PREFIX = ""

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


def list_buckets():
    """Print all buckets and their top-level prefixes at the RustFS endpoint."""
    s3 = _s3_client()
    buckets = s3.list_buckets().get("Buckets", [])
    if not buckets:
        print(f"No buckets found at {S3_ENDPOINT}")
        return
    print(f"Buckets at {S3_ENDPOINT}:")
    for b in buckets:
        name = b["Name"]
        print(f"  {name}/")
        resp = s3.list_objects_v2(Bucket=name, Delimiter="/")
        for cp in resp.get("CommonPrefixes", []):
            print(f"    {cp['Prefix']}")
            resp2 = s3.list_objects_v2(Bucket=name, Prefix=cp["Prefix"], Delimiter="/")
            for cp2 in resp2.get("CommonPrefixes", []):
                print(f"      {cp2['Prefix']}")


def clear_storage(keep=0, bucket=BUCKET, prefix=PREFIX):
    """Delete all frames from the given bucket/prefix, keeping the N most recent."""
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    print(f"Clearing s3://{bucket}/{prefix} ...", flush=True)

    if keep == 0:
        # Stream deletions page by page — no need to buffer everything
        total = 0
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                batch = page.get("Contents", [])
                if not batch:
                    continue
                s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in batch]})
                total += len(batch)
                print(f"  Deleted {total}...", flush=True)
        except s3.exceptions.NoSuchBucket:
            print(f"Bucket '{bucket}' does not exist at {S3_ENDPOINT}. Run 'list-buckets' to see available buckets.")
            return
        print(f"Done. Deleted {total} from s3://{bucket}/{prefix}")
        return

    # keep > 0: collect all keys first (already in chronological order from S3)
    all_objects = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            all_objects.extend(page.get("Contents", []))
            print(f"  Listed {len(all_objects)} objects...", flush=True)
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket '{bucket}' does not exist at {S3_ENDPOINT}. Run 'list-buckets' to see available buckets.")
        return
    to_delete = all_objects[:-keep]
    if not to_delete:
        print(f"Nothing to delete (found {len(all_objects)}, keeping all).")
        return
    total = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in batch]})
        total += len(batch)
        print(f"  Deleted {total}/{len(to_delete)}...", flush=True)
    print(f"Done. Deleted {total}, kept {len(all_objects) - total} from s3://{bucket}/{prefix}")


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


def clear_range(from_dt=None, to_dt=None, bucket=BUCKET, prefix=PREFIX):
    """Delete frames whose timestamps fall within [from_dt, to_dt] (UTC).

    Either bound may be omitted to leave that end open.
    """
    if from_dt is None and to_dt is None:
        print("Provide at least --from or --to.")
        return
    s3 = _s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    listed = 0
    print(f"Listing s3://{bucket}/{prefix} ...", flush=True)
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                listed += 1
                try:
                    ts = _parse_key_timestamp(obj["Key"])
                except (ValueError, IndexError):
                    continue
                if from_dt and ts < from_dt:
                    continue
                if to_dt and ts > to_dt:
                    continue
                to_delete.append(obj)
            print(f"  Scanned {listed}, matched {len(to_delete)}...", flush=True)
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket '{bucket}' does not exist at {S3_ENDPOINT}. Run 'list-buckets' to see available buckets.")
        return
    if not to_delete:
        print(f"Nothing to delete in range.")
        return
    total = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
        )
        total += len(batch)
        print(f"  Deleted {total}/{len(to_delete)}...", flush=True)
    lo = from_dt.isoformat() if from_dt else "beginning"
    hi = to_dt.isoformat() if to_dt else "now"
    print(f"Done. Deleted {total} frames from s3://{bucket} in range [{lo}, {hi}].")


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
    # Parse flags: --keep N, --from DT, --to DT, --bucket NAME, --prefix PATH
    keep = 1
    from_dt = None
    to_dt = None
    bucket = BUCKET
    prefix = PREFIX
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
        elif a == "--bucket":
            i += 1
            bucket = sys.argv[i].rstrip("/")
        elif a == "--prefix":
            i += 1
            prefix = sys.argv[i]
        elif a == "--hard-reset":
            pass
        else:
            filtered.append(a)
        i += 1
    args = filtered

    commands = {
        "list-buckets": lambda: list_buckets(),
        "clear": lambda: clear_storage(keep=keep, bucket=bucket, prefix=prefix),
        "clear-range": lambda: clear_range(from_dt=from_dt, to_dt=to_dt, bucket=bucket, prefix=prefix),
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
        print(f"  list-buckets       List all buckets at {S3_ENDPOINT}")
        print("  clear              Delete all frames, keeping N most recent (default 1)")
        print(f"                       --keep N")
        print(f"                       --bucket NAME   (default: {BUCKET})")
        print(f"                       --prefix PATH   (default: {PREFIX})")
        print("  clear-range        Delete frames in a time range (UTC)")
        print("                       --from YYYY-MM-DD[THH:MM:SS]")
        print("                       --to   YYYY-MM-DD[THH:MM:SS]")
        print(f"                       --bucket NAME   (default: {BUCKET})")
        print(f"                       --prefix PATH   (default: {PREFIX})")
        print("  delete-bucket      Remove ALL objects then delete the bucket (with confirmation)")
        print("  start-producer     Start the frame producer")
        print("  start-consumer     Start the frame consumer")
        print("  start              Start both producer and consumer")
        print("                       --hard-reset  (kill existing before starting)")
        print("  timestamps         List timestamps of all frames in the bucket")
        sys.exit(1)

    commands[args[0]]()
