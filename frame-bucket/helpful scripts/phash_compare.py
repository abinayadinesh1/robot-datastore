#!/usr/bin/env python3
"""Compare two images using the same aHash logic as the Rust filter.

Usage:
    python3 phash_compare.py image1.jpg image2.jpg [--hash-size 16]

Or grab two frames from the camera:
    python3 phash_compare.py --camera --delay 2
"""

import argparse
import sys
import numpy as np
import cv2


def compute_ahash(image_path_or_array, hash_size=16):
    """Reproduce the exact Rust filter logic:
    1. Load image
    2. Resize to hash_size x hash_size with NEAREST interpolation
    3. Convert to grayscale
    4. Compute mean pixel value
    5. Binary hash: 1 if pixel > mean, else 0
    """
    if isinstance(image_path_or_array, str):
        img = cv2.imread(image_path_or_array)
        if img is None:
            print(f"ERROR: could not read {image_path_or_array}")
            sys.exit(1)
    else:
        img = image_path_or_array

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    resized = cv2.resize(gray, (hash_size, hash_size), interpolation=cv2.INTER_NEAREST)
    mean = np.mean(resized.astype(np.float64))
    bits = (resized.astype(np.float64) > mean).flatten()
    return bits


def hamming_distance(a, b):
    return int(np.sum(a != b))


def main():
    parser = argparse.ArgumentParser(description="Compare two images with aHash (same as Rust filter)")
    parser.add_argument("images", nargs="*", help="Two image file paths")
    parser.add_argument("--hash-size", type=int, default=16, help="Hash grid size (default: 16 = 256 bits)")
    parser.add_argument("--camera", action="store_true", help="Grab two frames from camera HTTP endpoint")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between camera grabs (with --camera)")
    parser.add_argument("--url", default="http://localhost:8000/api/camera/frame?quality=80", help="Camera frame URL")
    args = parser.parse_args()

    if args.camera:
        import time
        import urllib.request

        print(f"Grabbing frame 1 from {args.url} ...")
        resp1 = urllib.request.urlopen(args.url)
        data1 = np.frombuffer(resp1.read(), dtype=np.uint8)
        img1 = cv2.imdecode(data1, cv2.IMREAD_COLOR)

        print(f"Waiting {args.delay}s ...")
        time.sleep(args.delay)

        print(f"Grabbing frame 2 ...")
        resp2 = urllib.request.urlopen(args.url)
        data2 = np.frombuffer(resp2.read(), dtype=np.uint8)
        img2 = cv2.imdecode(data2, cv2.IMREAD_COLOR)

        h1 = compute_ahash(img1, args.hash_size)
        h2 = compute_ahash(img2, args.hash_size)
    else:
        if len(args.images) != 2:
            parser.error("Provide exactly 2 image paths, or use --camera")
        h1 = compute_ahash(args.images[0], args.hash_size)
        h2 = compute_ahash(args.images[1], args.hash_size)

    dist = hamming_distance(h1, h2)
    total_bits = args.hash_size * args.hash_size
    pct = dist / total_bits * 100

    print(f"\nHash size:  {args.hash_size}x{args.hash_size} = {total_bits} bits")
    print(f"Hamming distance: {dist} / {total_bits}  ({pct:.1f}%)")
    print(f"\nSuggested thresholds:")
    print(f"  distance < {dist}  → would REJECT (same scene)")
    print(f"  distance > {dist}  → would ACCEPT (scene changed)")


if __name__ == "__main__":
    main()
