#!/usr/bin/env python3
"""
Backfill video annotations for all segments of a robot.

Reads the SQLite database for a robot, finds segments missing descriptions,
downloads each from RustFS (falling back to AWS S3 for evicted objects),
sends to the Modal VideoChat API, and writes the description back.

Dependencies: requests (+ stdlib only, no boto3)

AWS S3 fallback requires env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

Usage:
    python backfill_annotations.py                          # defaults: reachy-003
    python backfill_annotations.py --robot-id reachy-003
    python backfill_annotations.py --dry-run                # list what would be annotated
    python backfill_annotations.py --limit 10               # only annotate first 10
    python backfill_annotations.py --re-annotate            # overwrite existing descriptions
"""

import argparse
import hashlib
import hmac
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

# ── Defaults (match config.toml) ───────────────────────────────────────────────

RUSTFS_ENDPOINT = "http://100.81.222.59:9000"
RUSTFS_ACCESS_KEY = "rustfsadmin"
RUSTFS_SECRET_KEY = "rustfsadmin"
RUSTFS_BUCKET = "camera-frames"
RUSTFS_REGION = "us-east-1"

# AWS S3 archive (evicted objects land here with the same key)
AWS_S3_BUCKET = "reachy-mini-frames-archive"
AWS_S3_PREFIX = ""  # matches config.toml [aws_s3] prefix
AWS_S3_REGION = "us-west-2"

DB_DIR = Path(__file__).parent / "data"
ROBOT_ID = "reachy-003"

ANNOTATION_API_URL = (
    "https://abinayadinesh1--videochat-flash-inference-serve.modal.run/v1/video/chat"
)
ANNOTATION_PROMPT = "Give a detailed description of what goes on in this video."
MAX_NUM_FRAMES = 128


# ── Minimal S3v4 signer (stdlib only) ─────────────────────────────────────────

def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(secret: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def _s3v4_get(
    endpoint: str,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    region: str,
    timeout: int = 120,
) -> requests.Response:
    """Perform an S3 GET with AWS Signature V4 signing."""
    parsed = urlparse(endpoint)
    host = parsed.netloc

    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_uri = "/" + bucket + "/" + quote(key, safe="/")
    canonical_querystring = ""
    payload_hash = hashlib.sha256(b"").hexdigest()

    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"

    canonical_request = (
        f"GET\n{canonical_uri}\n{canonical_querystring}\n"
        f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )

    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
        + hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    )

    signing_key = _get_signature_key(secret_key, date_stamp, region, "s3")
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    url = f"{endpoint}/{bucket}/{quote(key, safe='/')}"
    headers = {
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
        "Authorization": authorization,
    }

    return requests.get(url, headers=headers, timeout=timeout)


# ── Download helpers ───────────────────────────────────────────────────────────

def download_from_rustfs(s3_key: str) -> bytes | None:
    """Try downloading from RustFS. Returns None on 404/error."""
    try:
        resp = _s3v4_get(
            RUSTFS_ENDPOINT, RUSTFS_BUCKET, s3_key,
            RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_REGION,
        )
        if resp.status_code == 404 or resp.status_code == 403:
            return None
        resp.raise_for_status()
        return resp.content
    except requests.RequestException:
        return None


def download_from_aws_s3(s3_key: str) -> bytes | None:
    """Try downloading from AWS S3 archive. Returns None if creds missing or error."""
    aws_access = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if not aws_access or not aws_secret:
        return None

    # Eviction stores with: aws_key = f"{aws_config.prefix}{key}"
    aws_key = f"{AWS_S3_PREFIX}{s3_key}"
    endpoint = f"https://s3.{AWS_S3_REGION}.amazonaws.com"

    try:
        resp = _s3v4_get(
            endpoint, AWS_S3_BUCKET, aws_key,
            aws_access, aws_secret, AWS_S3_REGION,
        )
        if resp.status_code == 404 or resp.status_code == 403:
            return None
        resp.raise_for_status()
        return resp.content
    except requests.RequestException:
        return None


def download_object(s3_key: str) -> tuple[bytes | None, str]:
    """Download from RustFS, falling back to AWS S3. Returns (data, source)."""
    data = download_from_rustfs(s3_key)
    if data is not None:
        return data, "rustfs"

    data = download_from_aws_s3(s3_key)
    if data is not None:
        return data, "aws-s3"

    return None, "none"


# ── Core logic ─────────────────────────────────────────────────────────────────

def format_ms(ms: int) -> str:
    """Format millisecond timestamp as a human-readable UTC string."""
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        return str(ms)


def annotate_segment(
    s3_key: str, start_ms: int, end_ms: int, seg_type: str, api_url: str
) -> str | None:
    """Download segment, send to annotation API, return description."""

    # Skip pseudo-keys for H.264 idle periods (no actual object stored).
    if s3_key.startswith("idle:"):
        print(f"  skipping (no stored object): {s3_key}")
        return None

    # Skip idle JPEG frames — the video model can't process still images.
    if seg_type == "idle" or s3_key.endswith(".jpg") or s3_key.endswith(".jpeg"):
        print(f"  skipping (JPEG idle frame, video model requires video): {s3_key}")
        return None

    # Download from RustFS, fall back to AWS S3.
    data, source = download_object(s3_key)
    if data is None:
        print(f"  ERROR: not found in RustFS or AWS S3: {s3_key}")
        return None

    print(f"  downloaded {len(data)} bytes from {source}: {s3_key}")

    # Determine MIME type from key.
    filename = s3_key.rsplit("/", 1)[-1]
    if s3_key.endswith(".jpg") or s3_key.endswith(".jpeg"):
        mime = "image/jpeg"
    else:
        mime = "video/mp4"

    # POST to annotation API.
    try:
        api_resp = requests.post(
            api_url,
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

    # Check AWS S3 fallback availability.
    has_aws = bool(os.environ.get("AWS_ACCESS_KEY_ID")) and bool(os.environ.get("AWS_SECRET_ACCESS_KEY"))
    if has_aws:
        print(f"AWS S3 fallback enabled (bucket: {AWS_S3_BUCKET}, region: {AWS_S3_REGION})")
    else:
        print("AWS S3 fallback disabled (set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to enable)")

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
            print(f"  [{row['id']}] {row['type']:6s}  {start_fmt} -> {end_fmt}  {size_kb:.0f}KB  {row['s3_key']}")
        print(f"\nDry run complete. Use without --dry-run to annotate.")
        return

    annotated = 0
    failed = 0
    max_workers = 4

    def process_row(idx, row):
        seg_id = row["id"]
        s3_key = row["s3_key"]
        start_ms = row["start_ms"]
        end_ms = row["end_ms"]
        seg_type = row["type"]

        print(f"\n[{idx}/{len(rows)}] segment {seg_id} ({seg_type}) {format_ms(start_ms)} -> {format_ms(end_ms)}")

        description = annotate_segment(s3_key, start_ms, end_ms, seg_type, args.api_url)
        return seg_id, description

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_row, i, row): row
            for i, row in enumerate(rows, 1)
        }

        for future in as_completed(futures):
            seg_id, description = future.result()
            if description:
                conn.execute(
                    "UPDATE segments SET description = ? WHERE id = ?",
                    (description, seg_id),
                )
                conn.commit()
                print(f"  seg {seg_id}: saved ({len(description)} chars)")
                annotated += 1
            else:
                failed += 1

    print(f"\nDone. Annotated: {annotated}, Failed/Skipped: {failed}")
    conn.close()


if __name__ == "__main__":
    main()
