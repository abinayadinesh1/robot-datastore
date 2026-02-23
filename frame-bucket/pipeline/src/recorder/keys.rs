use chrono::{DateTime, TimeZone, Utc};

fn fmt_ts(ms: i64) -> String {
    let dt: DateTime<Utc> = Utc
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now);
    dt.format("%Y%m%dT%H%M%S%3fZ").to_string()
}

fn date_str(ms: i64) -> String {
    let dt: DateTime<Utc> = Utc
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now);
    dt.format("%Y-%m-%d").to_string()
}

/// Key for the idle period's representative JPEG frame.
/// e.g. "frames/reachy-001/camera/2026-02-18/20260218T093000000Z_20260218T094000000Z.jpg"
pub fn idle_jpeg_key(prefix: &str, robot_id: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "{prefix}{robot_id}/camera/{date}/{start}_{end}.jpg",
        date = date_str(start_ms),
        start = fmt_ts(start_ms),
        end = fmt_ts(end_ms),
    )
}

/// Key for an active video segment MP4.
/// e.g. "frames/reachy-001/camera/2026-02-18/20260218T094000000Z_20260218T095000000Z.mp4"
pub fn active_segment_key(prefix: &str, robot_id: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "{prefix}{robot_id}/camera/{date}/{start}_{end}.mp4",
        date = date_str(start_ms),
        start = fmt_ts(start_ms),
        end = fmt_ts(end_ms),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_format() {
        // 2026-02-18T09:30:00.000Z = 1739871000000 ms
        let start = 1739871000000i64;
        let end = start + 60_000; // +60s

        let k = idle_jpeg_key("frames/", "reachy-001", start, end);
        assert!(k.ends_with(".jpg"), "idle jpeg key should end with .jpg");
        assert!(k.contains("reachy-001/camera/"), "should have robot/camera path");

        let k2 = active_segment_key("frames/", "reachy-001", start, end);
        assert!(k2.ends_with(".mp4"), "active key should end with .mp4");

        // Both share the same date directory
        let date_part = &k[..k.rfind('/').unwrap()];
        let date_part2 = &k2[..k2.rfind('/').unwrap()];
        assert_eq!(date_part, date_part2, "idle and active share the same date dir");
    }
}
