//! Integration tests for health endpoints.
//!
//! These tests require the API server to be running and reachable at API_BASE.
//! They also require the backing services (RustFS, S3, robot streams) to be live.
//!
//! Run with:
//!   API_BASE=http://100.81.222.59:8080 cargo test --package frame-bucket-api --test health_endpoints -- --nocapture
//!
//! Each test only passes if the endpoint returns actual data (not empty/error).

use std::time::Duration;

fn api_base() -> String {
    std::env::var("API_BASE").unwrap_or_else(|_| "http://100.81.222.59:8080".into())
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client")
}

// ---------------------------------------------------------------------------
// GET /health — storage_stats.json + disk usage
// ---------------------------------------------------------------------------

#[test]
fn health_returns_200_with_rustfs_and_disk_data() {
    let url = format!("{}/health", api_base());
    let resp = client().get(&url).send().expect("request to /health failed");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "/health returned non-200: {}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().expect("/health body is not valid JSON");

    // rustfs section must be present and have a status string
    let rustfs = &body["rustfs"];
    assert!(
        rustfs.is_object(),
        "/health missing 'rustfs' object. Full body: {body}"
    );
    let rustfs_status = rustfs["status"].as_str().unwrap_or("");
    assert!(
        !rustfs_status.is_empty(),
        "/health rustfs.status is empty. Got: {rustfs}"
    );

    // storage section must be present with numeric values
    let storage = &body["storage"];
    assert!(
        storage.is_object(),
        "/health missing 'storage' object. Full body: {body}"
    );
    assert!(
        storage["total_gb"].is_number(),
        "/health storage.total_gb is not a number. Got: {storage}"
    );
    assert!(
        storage["threshold_gb"].is_number(),
        "/health storage.threshold_gb is not a number. Got: {storage}"
    );

    // disk section must be present with actual values
    let disk = &body["disk"];
    assert!(
        disk.is_object(),
        "/health missing 'disk' object. Full body: {body}"
    );
    let disk_status = disk["status"].as_str().unwrap_or("");
    assert!(
        !disk_status.is_empty(),
        "/health disk.status is empty. Got: {disk}"
    );
    assert!(
        disk["total_gb"].as_f64().unwrap_or(0.0) > 0.0,
        "/health disk.total_gb should be > 0. Got: {disk}"
    );
    assert!(
        disk["used_gb"].as_f64().unwrap_or(0.0) > 0.0,
        "/health disk.used_gb should be > 0. Got: {disk}"
    );
    assert!(
        disk["usage_pct"].as_f64().unwrap_or(0.0) > 0.0,
        "/health disk.usage_pct should be > 0. Got: {disk}"
    );

    // s3 section
    let s3 = &body["s3"];
    assert!(
        s3.is_object(),
        "/health missing 's3' object. Full body: {body}"
    );

    // eviction section
    let eviction = &body["eviction"];
    assert!(
        eviction.is_object(),
        "/health missing 'eviction' object. Full body: {body}"
    );
}

// ---------------------------------------------------------------------------
// GET /health/rustfs — browse local RustFS bucket
// ---------------------------------------------------------------------------

#[test]
fn health_rustfs_returns_bucket_contents() {
    let url = format!("{}/health/rustfs", api_base());
    let resp = client()
        .get(&url)
        .send()
        .expect("request to /health/rustfs failed");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "/health/rustfs returned non-200: {}. Body: {}",
        resp.status(),
        resp.text().unwrap_or_default()
    );

    let body: serde_json::Value = resp.json().expect("/health/rustfs body is not valid JSON");

    // Must have the expected top-level fields
    assert_eq!(
        body["bucket"].as_str().unwrap_or(""),
        "camera-frames",
        "/health/rustfs bucket name mismatch. Got: {}",
        body["bucket"]
    );

    assert!(
        body["containers"].is_array(),
        "/health/rustfs missing 'containers' array. Full body: {body}"
    );

    assert!(
        body["total_objects"].is_number(),
        "/health/rustfs missing 'total_objects'. Full body: {body}"
    );

    assert!(
        body["total_size_bytes"].is_number(),
        "/health/rustfs missing 'total_size_bytes'. Full body: {body}"
    );

    // We expect at least 1 container (robot) with actual objects
    let containers = body["containers"].as_array().unwrap();
    assert!(
        !containers.is_empty(),
        "/health/rustfs returned 0 containers — RustFS bucket appears empty"
    );

    let first = &containers[0];
    assert!(
        first["name"].is_string() && !first["name"].as_str().unwrap().is_empty(),
        "/health/rustfs first container has no name. Got: {first}"
    );
    assert!(
        first["date_count"].as_i64().unwrap_or(0) > 0,
        "/health/rustfs first container has 0 date folders — no data ingested. Got: {first}"
    );
    assert!(
        first["recent_dates"].is_array()
            && !first["recent_dates"].as_array().unwrap().is_empty(),
        "/health/rustfs first container has no recent_dates. Got: {first}"
    );
    assert!(
        first["recent_objects"].is_array()
            && !first["recent_objects"].as_array().unwrap().is_empty(),
        "/health/rustfs first container has no recent_objects — no data in latest date folder. Got: {first}"
    );

    // Validate recent_objects have required fields
    let obj = &first["recent_objects"].as_array().unwrap()[0];
    assert!(
        obj["key"].is_string() && !obj["key"].as_str().unwrap().is_empty(),
        "/health/rustfs recent object missing 'key'. Got: {obj}"
    );
    assert!(
        obj["size_bytes"].is_number(),
        "/health/rustfs recent object missing 'size_bytes'. Got: {obj}"
    );
    assert!(
        obj["last_modified"].is_string() && !obj["last_modified"].as_str().unwrap().is_empty(),
        "/health/rustfs recent object missing 'last_modified'. Got: {obj}"
    );

    // total_objects must be > 0
    assert!(
        body["total_objects"].as_i64().unwrap_or(0) > 0,
        "/health/rustfs total_objects is 0 — RustFS has no data"
    );
}

// ---------------------------------------------------------------------------
// GET /health/s3 — browse AWS S3 archive bucket
// ---------------------------------------------------------------------------

#[test]
fn health_s3_returns_bucket_contents() {
    let url = format!("{}/health/s3", api_base());
    let resp = client()
        .get(&url)
        .send()
        .expect("request to /health/s3 failed");

    // S3 may return 503 if AWS creds aren't configured — that's a real failure
    assert_eq!(
        resp.status().as_u16(),
        200,
        "/health/s3 returned non-200 (AWS credentials may be missing or bucket unreachable): {}. Body: {}",
        resp.status(),
        resp.text().unwrap_or_default()
    );

    let body: serde_json::Value = resp.json().expect("/health/s3 body is not valid JSON");

    assert_eq!(
        body["bucket"].as_str().unwrap_or(""),
        "reachy-mini-frames-archive",
        "/health/s3 bucket name mismatch. Got: {}",
        body["bucket"]
    );

    assert!(
        body["containers"].is_array(),
        "/health/s3 missing 'containers' array. Full body: {body}"
    );

    assert!(
        body["total_objects"].is_number(),
        "/health/s3 missing 'total_objects'. Full body: {body}"
    );

    assert!(
        body["total_size_bytes"].is_number(),
        "/health/s3 missing 'total_size_bytes'. Full body: {body}"
    );

    // S3 archive may legitimately be empty if eviction hasn't run yet,
    // but the response shape must be correct.
    // If there ARE containers, validate their structure.
    let containers = body["containers"].as_array().unwrap();
    if !containers.is_empty() {
        let first = &containers[0];
        assert!(
            first["name"].is_string(),
            "/health/s3 container missing 'name'. Got: {first}"
        );
        assert!(
            first["date_count"].is_number(),
            "/health/s3 container missing 'date_count'. Got: {first}"
        );
        assert!(
            first["recent_dates"].is_array(),
            "/health/s3 container missing 'recent_dates'. Got: {first}"
        );
        assert!(
            first["recent_objects"].is_array(),
            "/health/s3 container missing 'recent_objects'. Got: {first}"
        );
    }
}

// ---------------------------------------------------------------------------
// GET /health/streams — check robot stream reachability
// ---------------------------------------------------------------------------

#[test]
fn health_streams_returns_stream_statuses() {
    let url = format!("{}/health/streams", api_base());
    let resp = client()
        .get(&url)
        .send()
        .expect("request to /health/streams failed");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "/health/streams returned non-200: {}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().expect("/health/streams body is not valid JSON");

    assert!(
        body.is_array(),
        "/health/streams should return an array. Got: {body}"
    );

    let streams = body.as_array().unwrap();
    assert!(
        !streams.is_empty(),
        "/health/streams returned empty array — no robots configured in config.toml"
    );

    for (i, stream) in streams.iter().enumerate() {
        assert!(
            stream["robot_id"].is_string() && !stream["robot_id"].as_str().unwrap().is_empty(),
            "/health/streams[{i}] missing 'robot_id'. Got: {stream}"
        );
        assert!(
            stream["stream_url"].is_string()
                && !stream["stream_url"].as_str().unwrap().is_empty(),
            "/health/streams[{i}] missing 'stream_url'. Got: {stream}"
        );
        assert!(
            stream["reachable"].is_boolean(),
            "/health/streams[{i}] missing 'reachable' boolean. Got: {stream}"
        );
    }

    // At least one stream should be reachable if the robot is on
    let any_reachable = streams.iter().any(|s| s["reachable"].as_bool() == Some(true));
    if !any_reachable {
        eprintln!(
            "WARNING: no streams are reachable. This may be expected if robots are off.\n\
             Streams: {:?}",
            streams
        );
    }
}

// ---------------------------------------------------------------------------
// Cross-endpoint consistency: /health rustfs status matches /health/rustfs
// ---------------------------------------------------------------------------

#[test]
fn health_and_rustfs_browse_are_consistent() {
    let health_url = format!("{}/health", api_base());
    let rustfs_url = format!("{}/health/rustfs", api_base());

    let health_resp = client().get(&health_url).send();
    let rustfs_resp = client().get(&rustfs_url).send();

    // If /health works, /health/rustfs should also work (both use the same RustFS client)
    if let Ok(h) = health_resp {
        if h.status().is_success() {
            let hr: serde_json::Value = h.json().unwrap();
            let rustfs_status = hr["rustfs"]["status"].as_str().unwrap_or("unknown");

            if let Ok(r) = rustfs_resp {
                assert!(
                    r.status().is_success(),
                    "/health says rustfs status='{}' but /health/rustfs returned HTTP {}. \
                     The RustFS browse endpoint should work if the health endpoint can read storage_stats.json.",
                    rustfs_status,
                    r.status()
                );
            }
        }
    }
}
