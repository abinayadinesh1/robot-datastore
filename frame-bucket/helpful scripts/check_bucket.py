#!/usr/bin/env python3
"""Check what's in the RustFS camera-frames bucket."""

import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="rustfsadmin",
    aws_secret_access_key="rustfsadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# List buckets
buckets = s3.list_buckets()
print("Buckets:", [b["Name"] for b in buckets["Buckets"]])

# List objects
resp = s3.list_objects_v2(Bucket="camera-frames", Prefix="frames/", MaxKeys=50)
objects = resp.get("Contents", [])
total_size = sum(o["Size"] for o in objects)
print(f"\nObjects in camera-frames: {resp.get('KeyCount', 0)}")
print(f"Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
print()
for obj in objects[:15]:
    print(f"  {obj['Key']}  ({obj['Size']:,} bytes)")
if len(objects) > 15:
    print(f"  ... and {len(objects) - 15} more")
