#!/usr/bin/env bash
#
# Integration test for health endpoints.
# Run on the server where the API is running:
#
#   bash test_health_endpoints.sh [API_BASE]
#
# Default API_BASE: http://localhost:8080
#
set -euo pipefail

API="${1:-http://localhost:8080}"
PASS=0
FAIL=0
WARN=0

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

pass() { PASS=$((PASS+1)); green "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); red   "  FAIL: $1"; }
warn() { WARN=$((WARN+1)); yellow "  WARN: $1"; }

assert_field() {
    local json="$1" field="$2" desc="$3"
    val=$(echo "$json" | jq -r "$field" 2>/dev/null || echo "null")
    if [ "$val" = "null" ] || [ -z "$val" ]; then
        fail "$desc — field $field is missing/null"
        return 1
    fi
    pass "$desc"
    return 0
}

assert_gt_zero() {
    local json="$1" field="$2" desc="$3"
    val=$(echo "$json" | jq "$field" 2>/dev/null || echo "0")
    if [ "$val" = "null" ] || [ "$val" = "0" ]; then
        fail "$desc — $field is 0 or missing"
        return 1
    fi
    pass "$desc ($val)"
    return 0
}

# ────────────────────────────────────────────────────────────────────────
bold "=== Testing Health Endpoints at $API ==="
echo

# ── 1. GET /health ──────────────────────────────────────────────────────
bold "── GET /health ──"
HTTP=$(curl -s -o /tmp/health.json -w "%{http_code}" --connect-timeout 5 "$API/health" 2>/dev/null || echo "000")
if [ "$HTTP" != "200" ]; then
    fail "/health returned HTTP $HTTP (API unreachable or storage_stats.json not written)"
    echo "  Response: $(cat /tmp/health.json 2>/dev/null || echo 'none')"
else
    BODY=$(cat /tmp/health.json)
    pass "/health returned 200"

    assert_field "$BODY" '.rustfs.status'        "/health has rustfs.status"
    assert_field "$BODY" '.storage.total_gb'     "/health has storage.total_gb"
    assert_field "$BODY" '.storage.threshold_gb' "/health has storage.threshold_gb"
    assert_field "$BODY" '.s3.status'            "/health has s3.status"
    assert_field "$BODY" '.eviction.state'       "/health has eviction.state"
    assert_field "$BODY" '.disk.status'          "/health has disk.status"
    assert_gt_zero "$BODY" '.disk.total_gb'      "/health disk.total_gb > 0"
    assert_gt_zero "$BODY" '.disk.used_gb'       "/health disk.used_gb > 0"
fi
echo

# ── 2. GET /health/rustfs ──────────────────────────────────────────────
bold "── GET /health/rustfs ──"
HTTP=$(curl -s -o /tmp/health_rustfs.json -w "%{http_code}" --connect-timeout 8 "$API/health/rustfs" 2>/dev/null || echo "000")
if [ "$HTTP" != "200" ]; then
    fail "/health/rustfs returned HTTP $HTTP"
    echo "  Response: $(cat /tmp/health_rustfs.json 2>/dev/null || echo 'none')"
else
    BODY=$(cat /tmp/health_rustfs.json)
    pass "/health/rustfs returned 200"

    BUCKET=$(echo "$BODY" | jq -r '.bucket')
    if [ "$BUCKET" = "camera-frames" ]; then
        pass "/health/rustfs bucket = camera-frames"
    else
        fail "/health/rustfs bucket = '$BUCKET' (expected camera-frames)"
    fi

    N_CONTAINERS=$(echo "$BODY" | jq '.containers | length')
    if [ "$N_CONTAINERS" -gt 0 ]; then
        pass "/health/rustfs has $N_CONTAINERS container(s)"
    else
        fail "/health/rustfs has 0 containers — RustFS bucket is empty"
    fi

    assert_gt_zero "$BODY" '.total_objects'    "/health/rustfs total_objects > 0"
    assert_gt_zero "$BODY" '.total_size_bytes' "/health/rustfs total_size_bytes > 0"

    # Check first container structure
    if [ "$N_CONTAINERS" -gt 0 ]; then
        C0=$(echo "$BODY" | jq '.containers[0]')
        assert_field "$C0" '.name'         "/health/rustfs container[0] has name"
        assert_gt_zero "$C0" '.date_count' "/health/rustfs container[0] date_count > 0"

        N_RECENT=$(echo "$C0" | jq '.recent_objects | length')
        if [ "$N_RECENT" -gt 0 ]; then
            pass "/health/rustfs container[0] has $N_RECENT recent objects"
            OBJ0=$(echo "$C0" | jq '.recent_objects[0]')
            assert_field "$OBJ0" '.key'           "/health/rustfs recent_objects[0] has key"
            assert_field "$OBJ0" '.size_bytes'    "/health/rustfs recent_objects[0] has size_bytes"
            assert_field "$OBJ0" '.last_modified' "/health/rustfs recent_objects[0] has last_modified"
        else
            fail "/health/rustfs container[0] has 0 recent objects"
        fi
    fi

    IS_LIVE=$(echo "$BODY" | jq '.is_live')
    if [ "$IS_LIVE" = "true" ]; then
        pass "/health/rustfs is LIVE (data modified within 120s)"
    else
        warn "/health/rustfs is_live=false (no recent writes — pipeline may be stopped)"
    fi
fi
echo

# ── 3. GET /health/s3 ──────────────────────────────────────────────────
bold "── GET /health/s3 ──"
HTTP=$(curl -s -o /tmp/health_s3.json -w "%{http_code}" --connect-timeout 8 "$API/health/s3" 2>/dev/null || echo "000")
if [ "$HTTP" != "200" ]; then
    fail "/health/s3 returned HTTP $HTTP (AWS credentials may be missing)"
    echo "  Response: $(cat /tmp/health_s3.json 2>/dev/null || echo 'none')"
else
    BODY=$(cat /tmp/health_s3.json)
    pass "/health/s3 returned 200"

    BUCKET=$(echo "$BODY" | jq -r '.bucket')
    if [ "$BUCKET" = "reachy-mini-frames-archive" ]; then
        pass "/health/s3 bucket = reachy-mini-frames-archive"
    else
        fail "/health/s3 bucket = '$BUCKET' (expected reachy-mini-frames-archive)"
    fi

    assert_field "$BODY" '.containers'      "/health/s3 has containers array"
    assert_field "$BODY" '.total_objects'    "/health/s3 has total_objects"
    assert_field "$BODY" '.total_size_bytes' "/health/s3 has total_size_bytes"

    N_CONTAINERS=$(echo "$BODY" | jq '.containers | length')
    if [ "$N_CONTAINERS" -gt 0 ]; then
        pass "/health/s3 has $N_CONTAINERS container(s)"
        C0=$(echo "$BODY" | jq '.containers[0]')
        assert_field "$C0" '.name' "/health/s3 container[0] has name"
    else
        warn "/health/s3 has 0 containers (eviction may not have run yet)"
    fi
fi
echo

# ── 4. GET /health/streams ─────────────────────────────────────────────
bold "── GET /health/streams ──"
HTTP=$(curl -s -o /tmp/health_streams.json -w "%{http_code}" --connect-timeout 10 "$API/health/streams" 2>/dev/null || echo "000")
if [ "$HTTP" != "200" ]; then
    fail "/health/streams returned HTTP $HTTP"
    echo "  Response: $(cat /tmp/health_streams.json 2>/dev/null || echo 'none')"
else
    BODY=$(cat /tmp/health_streams.json)
    pass "/health/streams returned 200"

    N_STREAMS=$(echo "$BODY" | jq 'length')
    if [ "$N_STREAMS" -gt 0 ]; then
        pass "/health/streams has $N_STREAMS stream(s) configured"
    else
        fail "/health/streams returned empty array — no robots in config.toml"
    fi

    # Check each stream
    for i in $(seq 0 $((N_STREAMS-1))); do
        S=$(echo "$BODY" | jq ".[$i]")
        RID=$(echo "$S" | jq -r '.robot_id')
        URL=$(echo "$S" | jq -r '.stream_url')
        REACH=$(echo "$S" | jq -r '.reachable')

        if [ "$REACH" = "true" ]; then
            pass "  stream $RID ($URL) is reachable"
        else
            warn "  stream $RID ($URL) is NOT reachable (robot may be off)"
        fi

        # Validate fields exist
        if [ -z "$RID" ] || [ "$RID" = "null" ]; then
            fail "  stream[$i] missing robot_id"
        fi
        if [ -z "$URL" ] || [ "$URL" = "null" ]; then
            fail "  stream[$i] missing stream_url"
        fi
    done
fi
echo

# ── Summary ─────────────────────────────────────────────────────────────
bold "=== Summary ==="
green "  Passed:   $PASS"
[ "$WARN" -gt 0 ] && yellow "  Warnings: $WARN"
[ "$FAIL" -gt 0 ] && red    "  Failed:   $FAIL"
echo

if [ "$FAIL" -gt 0 ]; then
    red "TESTS FAILED"
    exit 1
else
    green "ALL TESTS PASSED"
    exit 0
fi
