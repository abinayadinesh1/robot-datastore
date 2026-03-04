#!/usr/bin/env python3
"""
Backfill video annotations for all segments of a robot.

Reads the SQLite database for a robot, finds segments missing descriptions,
downloads each from RustFS, sends to the Modal VideoChat API, and writes
the description back.

Usage:
    python backfill_annotations.py                          # defaults: reachy-003
    python backfill_annotations.py --robot-id reachy-003
    python backfill_annotations.py --dry-run                # list what would be annotated
    python backfill_annotations.py --limit 10               # only annotate first 10
    python backfill_annotations.py --re-annotate            # overwrite existing descriptions
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config as BotoConfig

# ── Defaults (match config.toml) ───────────────────────────────────────────────

RUSTFS_ENDPOINT = "http://100.81.222.59:9000"
RUSTFS_ACCESS_KEY = "rustfsadmin"
RUSTFS_SECRET_KEY = "rustfsadmin"
RUSTFS_BUCKET = "camera-frames"

DB_DIR = Path(__file__).parent / "data"
ROBOT_ID = "reachy-003"

ANNOTATION_API_URL = (
    "https://abinayadinesh1--videochat-flash-inference-serve.modal.run/v1/video/chat"
)
ANNOTATION_PROMPT = "Give a detailed description of what goes on in this video."
MAX_NUM_FRAMES = 128


def format_ms(ms: int) -> str:
    """Format millisecond timestamp as a human-readable UTC string."""
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        return str(ms)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4"),
    )


def annotate_segment(
    s3, s3_key: str, start_ms: int, end_ms: int, seg_type: str
) -> str | None:
    """Download segment from RustFS, send to annotation API, return description."""

    # Skip pseudo-keys for H.264 idle periods (no actual object stored).
    if s3_key.startswith("idle:"):
        print(f"  skipping (no stored object): {s3_key}")
        return None

    # Download from RustFS.
    try:
        resp = s3.get_object(Bucket=RUSTFS_BUCKET, Key=s3_key)
        data = resp["Body"].read()
    except Exception as e:
        print(f"  ERROR downloading {s3_key}: {e}")
        return None

    print(f"  downloaded {len(data)} bytes from {s3_key}")

    # Determine MIME type from key.
    if s3_key.endswith(".jpg") or s3_key.endswith(".jpeg"):
        mime = "image/jpeg"
        filename = s3_key.rsplit("/", 1)[-1]
    else:
        mime = "video/mp4"
        filename = s3_key.rsplit("/", 1)[-1]

    # POST to annotation API.
    try:
        api_resp = requests.post(
            ANNOTATION_API_URL,
            files={"file": (filename, data, mime)},
            data={
                "question": ANNOTATION_PROMPT,
                "max_num_frames": str(MAX_NUM_FRAMES),
            },
            timeout=300,  # 5 min timeout for large videos
        )
        api_resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR calling annotation API: {e}")
        return None

    result = api_resp.json()
    model_text = result.get("response", "")
    if not model_text:
        print("  WARNING: API returned empty response")
        return None

    # Prepend timestamp range.
    start_fmt = format_ms(start_ms)
    end_fmt = format_ms(end_ms)
    description = f"From {start_fmt} to {end_fmt}, {model_text}"
    return description


def main():
    parser = argparse.ArgumentParser(description="Backfill segment annotations")
    parser.add_argument("--robot-id", default=ROBOT_ID, help="Robot ID (default: reachy-003)")
    parser.add_argument("--db-dir", default=str(DB_DIR), help="Directory containing {robot_id}.db")
    parser.add_argument("--dry-run", action="store_true", help="List segments without annotating")
    parser.add_argument("--limit", type=int, default=0, help="Max segments to annotate (0 = all)")
    parser.add_argument("--re-annotate", action="store_true", help="Re-annotate segments that already have descriptions")
    parser.add_argument("--type", choices=["active", "idle", "all"], default="all", help="Segment type filter")
    parser.add_argument("--api-url", default=ANNOTATION_API_URL, help="Annotation API URL")
    args = parser.parse_args()

    db_path = Path(args.db_dir) / f"{args.robot_id}.db"
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Check if description column exists; add it if not (migration).
    cols = [row[1] for row in conn.execute("PRAGMA table_info(segments)").fetchall()]
    if "description" not in cols:
        print("Adding 'description' column to segments table...")
        conn.execute("ALTER TABLE segments ADD COLUMN description TEXT")
        conn.commit()

    # Build query.
    where_clauses = []
    params = []

    if not args.re_annotate:
        where_clauses.append("description IS NULL")

    if args.type != "all":
        where_clauses.append("type = ?")
        params.append(args.type)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    limit_sql = f" LIMIT {args.limit}" if args.limit > 0 else ""

    query = f"SELECT id, type, start_ms, end_ms, s3_key, size_bytes FROM segments{where_sql} ORDER BY start_ms ASC{limit_sql}"
    rows = conn.execute(query, params).fetchall()

    print(f"Found {len(rows)} segment(s) to annotate for robot '{args.robot_id}'")
    if not rows:
        return

    if args.dry_run:
        for row in rows:
            start_fmt = format_ms(row["start_ms"])
            end_fmt = format_ms(row["end_ms"])
            size_kb = (row["size_bytes"] or 0) / 1024
            print(f"  [{row['id']}] {row['type']:6s}  {start_fmt} → {end_fmt}  {size_kb:.0f}KB  {row['s3_key']}")
        print(f"\nDry run complete. Use without --dry-run to annotate.")
        return

    s3 = get_s3_client()
    annotated = 0
    failed = 0

    for i, row in enumerate(rows, 1):
        seg_id = row["id"]
        s3_key = row["s3_key"]
        start_ms = row["start_ms"]
        end_ms = row["end_ms"]
        seg_type = row["type"]

        print(f"\n[{i}/{len(rows)}] segment {seg_id} ({seg_type}) {format_ms(start_ms)} → {format_ms(end_ms)}")

        description = annotate_segment(s3, s3_key, start_ms, end_ms, seg_type)
        if description:
            conn.execute(
                "UPDATE segments SET description = ? WHERE id = ?",
                (description, seg_id),
            )
            conn.commit()
            print(f"  ✓ saved ({len(description)} chars)")
            annotated += 1
        else:
            failed += 1

        # Brief pause between API calls to avoid overwhelming the endpoint.
        if i < len(rows):
            time.sleep(0.5)

    print(f"\nDone. Annotated: {annotated}, Failed/Skipped: {failed}")
    conn.close()


if __name__ == "__main__":
    main()
