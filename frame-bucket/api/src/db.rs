use std::path::Path;

use rusqlite::Connection;

use crate::types::Segment;

pub fn open_robot_db(db_dir: &Path, robot_id: &str) -> rusqlite::Result<Connection> {
    let path = db_dir.join(format!("{robot_id}.db"));
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
    )?;
    Ok(conn)
}

pub fn row_to_segment(row: &rusqlite::Row<'_>) -> rusqlite::Result<Segment> {
    let labels_raw: String = row.get(7)?;
    let labels: Vec<String> = serde_json::from_str(&labels_raw).unwrap_or_default();
    Ok(Segment {
        id: row.get(0)?,
        robot_id: row.get(1)?,
        segment_type: row.get(2)?,
        start_ms: row.get(3)?,
        end_ms: row.get(4)?,
        s3_key: row.get(5)?,
        size_bytes: row.get(6)?,
        frame_count: row.get(8).ok(),
        labels,
        description: row.get(9).ok(),
    })
}
