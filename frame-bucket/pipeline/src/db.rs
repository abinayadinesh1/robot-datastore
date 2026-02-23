use rusqlite::{Connection, Result as SqlResult, params};
use std::path::Path;
use std::sync::Mutex;
use tracing::{debug, info};

/// Per-robot SQLite database for segment metadata.
///
/// One file per robot: `{db_dir}/{robot_id}.db`
/// Schema: a single `segments` table indexed by (robot_id, start_ms, end_ms).
///
/// WAL mode is enabled so the consumer (writer) and API server (reader) can
/// operate concurrently without blocking each other.
pub struct SegmentDb {
    conn: Mutex<Connection>,
    robot_id: String,
}

impl SegmentDb {
    /// Open (or create) the SQLite database for a given robot.
    /// Creates `db_dir` if it does not exist.
    pub fn open(db_dir: &Path, robot_id: &str) -> SqlResult<Self> {
        std::fs::create_dir_all(db_dir)
            .map_err(|_e| rusqlite::Error::InvalidPath(db_dir.into()))?;

        let db_path = db_dir.join(format!("{robot_id}.db"));
        let conn = Connection::open(&db_path)?;

        // Enable WAL for concurrent reader (API) + writer (consumer)
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS segments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                robot_id    TEXT    NOT NULL,
                type        TEXT    NOT NULL CHECK(type IN ('active','idle')),
                start_ms    INTEGER NOT NULL,
                end_ms      INTEGER NOT NULL,
                s3_key      TEXT    NOT NULL,
                size_bytes  INTEGER,
                frame_count INTEGER,
                labels      TEXT    DEFAULT '[]'
            );
            CREATE INDEX IF NOT EXISTS idx_time
                ON segments(robot_id, start_ms, end_ms);

            CREATE TABLE IF NOT EXISTS collections (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                robot_id    TEXT    NOT NULL,
                name        TEXT    NOT NULL,
                description TEXT    DEFAULT '',
                created_at  INTEGER NOT NULL,
                updated_at  INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_collections_name
                ON collections(robot_id, name);

            CREATE TABLE IF NOT EXISTS collection_clips (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id   INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
                robot_id        TEXT    NOT NULL,
                modality        TEXT    NOT NULL DEFAULT 'camera',
                clip_start_ms   INTEGER NOT NULL,
                clip_end_ms     INTEGER NOT NULL,
                segment_ids     TEXT    NOT NULL DEFAULT '[]',
                manifest_s3_key TEXT,
                created_at      INTEGER NOT NULL,
                UNIQUE(collection_id, clip_start_ms, clip_end_ms)
            );
            CREATE INDEX IF NOT EXISTS idx_clips_collection
                ON collection_clips(collection_id);

            PRAGMA foreign_keys = ON;",
        )?;

        info!(path = db_path.display().to_string(), robot_id, "SQLite database opened");

        Ok(Self {
            conn: Mutex::new(conn),
            robot_id: robot_id.to_string(),
        })
    }

    /// Insert a completed active (MP4) segment. Returns the new row id.
    pub fn insert_active(
        &self,
        start_ms: i64,
        end_ms: i64,
        s3_key: &str,
        size_bytes: u64,
        frame_count: u32,
    ) -> SqlResult<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO segments (robot_id, type, start_ms, end_ms, s3_key, size_bytes, frame_count)
             VALUES (?1, 'active', ?2, ?3, ?4, ?5, ?6)",
            params![self.robot_id, start_ms, end_ms, s3_key, size_bytes as i64, frame_count as i64],
        )?;
        let id = conn.last_insert_rowid();
        debug!(id, start_ms, end_ms, s3_key, "inserted active segment");
        Ok(id)
    }

    /// Insert an idle period snapshot. Returns the new row id.
    pub fn insert_idle(
        &self,
        start_ms: i64,
        end_ms: i64,
        s3_key: &str,
        size_bytes: u64,
    ) -> SqlResult<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO segments (robot_id, type, start_ms, end_ms, s3_key, size_bytes)
             VALUES (?1, 'idle', ?2, ?3, ?4, ?5)",
            params![self.robot_id, start_ms, end_ms, s3_key, size_bytes as i64],
        )?;
        let id = conn.last_insert_rowid();
        debug!(id, start_ms, end_ms, s3_key, "inserted idle segment");
        Ok(id)
    }

}
