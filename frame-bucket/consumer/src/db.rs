use rusqlite::{Connection, Result as SqlResult, params};
use serde::{Deserialize, Serialize};
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

    // ── Collection methods ───────────────────────────────────────────────

    /// Create a new collection. Returns the new row id.
    pub fn insert_collection(&self, name: &str, description: &str) -> SqlResult<i64> {
        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO collections (robot_id, name, description, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![self.robot_id, name, description, now, now],
        )?;
        let id = conn.last_insert_rowid();
        debug!(id, name, "inserted collection");
        Ok(id)
    }

    /// List all collections for this robot.
    pub fn list_collections(&self) -> SqlResult<Vec<Collection>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, robot_id, name, description, created_at, updated_at
             FROM collections WHERE robot_id = ?1 ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map(params![self.robot_id], |row| {
            Ok(Collection {
                id: row.get(0)?,
                robot_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;
        rows.collect()
    }

    /// Get a single collection by id.
    pub fn get_collection(&self, id: i64) -> SqlResult<Option<Collection>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, robot_id, name, description, created_at, updated_at
             FROM collections WHERE id = ?1 AND robot_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![id, self.robot_id], |row| {
            Ok(Collection {
                id: row.get(0)?,
                robot_id: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    /// Delete a collection and its clips (via CASCADE).
    pub fn delete_collection(&self, id: i64) -> SqlResult<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "DELETE FROM collections WHERE id = ?1 AND robot_id = ?2",
            params![id, self.robot_id],
        )?;
        Ok(changed > 0)
    }

    // ── Clip methods ─────────────────────────────────────────────────────

    /// Insert a clip into a collection. Returns the new row id.
    pub fn insert_clip(
        &self,
        collection_id: i64,
        modality: &str,
        clip_start_ms: i64,
        clip_end_ms: i64,
        segment_ids_json: &str,
        manifest_s3_key: &str,
    ) -> SqlResult<i64> {
        let now = chrono::Utc::now().timestamp_millis();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO collection_clips
             (collection_id, robot_id, modality, clip_start_ms, clip_end_ms, segment_ids, manifest_s3_key, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![collection_id, self.robot_id, modality, clip_start_ms, clip_end_ms, segment_ids_json, manifest_s3_key, now],
        )?;
        // Touch the collection's updated_at
        conn.execute(
            "UPDATE collections SET updated_at = ?1 WHERE id = ?2",
            params![now, collection_id],
        )?;
        let id = conn.last_insert_rowid();
        debug!(id, collection_id, clip_start_ms, clip_end_ms, "inserted clip");
        Ok(id)
    }

    /// List all clips in a collection.
    pub fn list_clips(&self, collection_id: i64) -> SqlResult<Vec<CollectionClip>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, collection_id, robot_id, modality, clip_start_ms, clip_end_ms,
                    segment_ids, manifest_s3_key, created_at
             FROM collection_clips
             WHERE collection_id = ?1 AND robot_id = ?2
             ORDER BY clip_start_ms ASC",
        )?;
        let rows = stmt.query_map(params![collection_id, self.robot_id], |row| {
            Ok(CollectionClip {
                id: row.get(0)?,
                collection_id: row.get(1)?,
                robot_id: row.get(2)?,
                modality: row.get(3)?,
                clip_start_ms: row.get(4)?,
                clip_end_ms: row.get(5)?,
                segment_ids: row.get(6)?,
                manifest_s3_key: row.get(7)?,
                created_at: row.get(8)?,
            })
        })?;
        rows.collect()
    }

    /// Delete a single clip.
    pub fn delete_clip(&self, clip_id: i64) -> SqlResult<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "DELETE FROM collection_clips WHERE id = ?1 AND robot_id = ?2",
            params![clip_id, self.robot_id],
        )?;
        Ok(changed > 0)
    }
}

// ── Data types ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct Collection {
    pub id: i64,
    pub robot_id: String,
    pub name: String,
    pub description: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionClip {
    pub id: i64,
    pub collection_id: i64,
    pub robot_id: String,
    pub modality: String,
    pub clip_start_ms: i64,
    pub clip_end_ms: i64,
    pub segment_ids: String,
    pub manifest_s3_key: Option<String>,
    pub created_at: i64,
}
