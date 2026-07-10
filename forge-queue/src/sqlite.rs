//! `rusqlite`-WAL lease queue — the durable single-writer backbone.
//!
//! All [`Queue`] ops run the (synchronous) SQLite work on
//! [`tokio::task::spawn_blocking`] so the async runtime is never blocked; the one
//! connection lives behind an `Arc<Mutex<_>>`, which IS the single-writer
//! discipline that makes the lease transaction safe without row locks.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use forge_core::{now_ms, ForgeError, Item, ItemState, Queue, QueueCounts, Shard};
use rusqlite::{params, Connection, OptionalExtension};

/// SQLite-backed durable queue. Open it once and hand it to a `BatchRun`.
pub struct SqliteQueue {
    conn: Arc<Mutex<Connection>>,
    /// Advisory `leased_by` tag so a checkpoint shows which process holds a lease.
    coordinator_id: String,
}

/// The lease-state split behind [`SqliteQueue::resume_audit`] — the resume-readiness
/// view of a checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResumeAudit {
    /// Leases a worker still holds (unexpired) — actively in flight right now.
    pub live_leases: u64,
    /// Expired leases — items a dead / spot-killed worker dropped; a `resume` or
    /// `sweep` reclaims these. `> 0` is the fingerprint of an interruption.
    pub orphaned_leases: u64,
    /// `(leased_by, count)` for the live leases, most first — "who is still running".
    pub live_holders: Vec<(String, u64)>,
}

fn q(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Queue(e.to_string())
}

fn join_err(e: tokio::task::JoinError) -> ForgeError {
    ForgeError::Queue(format!("blocking task: {e}"))
}

/// Status breakdown, given an already-locked connection. Shared by
/// [`SqliteQueue::counts`] and [`SqliteQueue::audit_snapshot`] so the latter can
/// read counts and the resume-audit lease split under one lock acquisition.
fn counts_locked(conn: &Connection) -> Result<QueueCounts, ForgeError> {
    let mut stmt = conn
        .prepare("SELECT status, COUNT(*) FROM items GROUP BY status")
        .map_err(q)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(q)?;
    let mut c = QueueCounts::default();
    for r in rows {
        let (status, n) = r.map_err(q)?;
        let n = n as u64;
        match status.as_str() {
            "pending" => c.pending = n,
            "leased" => c.leased = n,
            "done" => c.done = n,
            "dead_letter" => c.dead = n,
            other => tracing::warn!(status = other, "unexpected item status in DB"),
        }
    }
    Ok(c)
}

/// Live/orphaned lease split, given an already-locked connection. Shared by
/// [`SqliteQueue::resume_audit`] and [`SqliteQueue::audit_snapshot`].
fn resume_audit_locked(conn: &Connection, now_ms: i64) -> Result<ResumeAudit, ForgeError> {
    // Leases split by expiry. A NULL leased_until on a leased row shouldn't happen
    // (the lease transaction always stamps it), but treat it as orphaned — the safe
    // side (a resume reclaims it) rather than counting it as live and stranding it.
    let (live, orphaned): (u64, u64) = conn
        .query_row(
            "SELECT
               COALESCE(SUM(CASE WHEN leased_until > ?1 THEN 1 ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN leased_until IS NULL OR leased_until <= ?1 THEN 1 ELSE 0 END), 0)
             FROM items WHERE status = 'leased'",
            [now_ms],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )
        .map_err(q)?;

    // Who holds live leases (for "is another worker still running?").
    let mut stmt = conn
        .prepare(
            "SELECT COALESCE(leased_by, '(unknown)'), COUNT(*) FROM items
              WHERE status = 'leased' AND leased_until > ?1
              GROUP BY leased_by ORDER BY COUNT(*) DESC, leased_by",
        )
        .map_err(q)?;
    let live_holders = stmt
        .query_map([now_ms], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })
        .map_err(q)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(q)?;

    Ok(ResumeAudit {
        live_leases: live,
        orphaned_leases: orphaned,
        live_holders,
    })
}

impl SqliteQueue {
    /// Open (creating if needed) the checkpoint DB at `path`, apply the
    /// crash-safety pragmas, and ensure the schema exists. This is the durable
    /// "checkpoint": the queue rows ARE the resume state.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ForgeError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let conn = Connection::open(path).map_err(q)?;
        // WAL: concurrent readers (status/sweep) alongside the single writer.
        // busy_timeout: ride out brief contention rather than erroring (a known
        // failure mode of batch tools sharing a SQLite file). synchronous=NORMAL: durable under WAL with
        // far fewer fsyncs than FULL.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA busy_timeout = 60000;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )
        .map_err(q)?;
        conn.execute_batch(SCHEMA).map_err(q)?;
        // Migration for checkpoints created before the job table carried the fleet
        // config: adding a column that already exists is the only failure mode, so
        // the error is deliberately ignored.
        let _ = conn.execute("ALTER TABLE job ADD COLUMN concurrency INTEGER", []);
        Ok(Self::wrap(conn))
    }

    /// Test/embedding helper: an in-memory queue (no durability).
    pub fn open_in_memory() -> Result<Self, ForgeError> {
        let conn = Connection::open_in_memory().map_err(q)?;
        conn.execute_batch(SCHEMA).map_err(q)?;
        // Migration for checkpoints created before the job table carried the fleet
        // config: adding a column that already exists is the only failure mode, so
        // the error is deliberately ignored.
        let _ = conn.execute("ALTER TABLE job ADD COLUMN concurrency INTEGER", []);
        Ok(Self::wrap(conn))
    }

    fn wrap(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            coordinator_id: format!("forge-{}", std::process::id()),
        }
    }

    /// Record this run's job metadata so a later `resume` can reuse the original
    /// output path AND fleet config (the checkpoint holds one job; re-running
    /// replaces it). Recording the per-worker concurrency closes a real footgun: a
    /// bare `resume` used to silently fall back to the flag default, which against
    /// a small engine meant starting AIMD at a far-too-high ceiling — a 429 storm
    /// that dead-letters valid items. Called once at startup — synchronous since
    /// it's off the dispatch hot path.
    pub fn record_job(
        &self,
        input_uri: &str,
        output_uri: &str,
        concurrency: usize,
    ) -> Result<(), ForgeError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO job (job_id, input_uri, output_uri, concurrency, created_at, status)
                 VALUES ('default', ?1, ?2, ?3, ?4, 'running')
             ON CONFLICT(job_id) DO UPDATE SET
                 input_uri = excluded.input_uri,
                 output_uri = excluded.output_uri,
                 concurrency = excluded.concurrency,
                 created_at = excluded.created_at",
            params![input_uri, output_uri, concurrency as i64, now_ms()],
        )
        .map_err(q)?;
        Ok(())
    }

    /// The per-worker concurrency recorded by the original `run`, if this
    /// checkpoint has one (`None` for checkpoints from older builds).
    pub fn job_concurrency(&self) -> Result<Option<usize>, ForgeError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT concurrency FROM job WHERE job_id='default'",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .map_err(q)
        .map(|v| v.flatten().map(|c| c.max(1) as usize))
    }

    /// The output path recorded by the original `run`, if this checkpoint has one.
    pub fn job_output_uri(&self) -> Result<Option<String>, ForgeError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT output_uri FROM job WHERE job_id='default'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(q)
        .map(Option::flatten)
    }

    /// Distribution of attempt counts over **terminal** items (done + dead-letter),
    /// returned as `(attempts, count)` ascending — the retry-cost signal surfaced by
    /// `forge status`. Pending/leased items are excluded because their attempt count
    /// isn't final yet. An inherent method, not part of the [`Queue`] trait, so a
    /// leaner backend (e.g. `redb`) needn't implement it.
    pub async fn attempt_histogram(&self) -> Result<Vec<(u32, u64)>, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<Vec<(u32, u64)>, ForgeError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT attempts, COUNT(*) FROM items
                      WHERE status IN ('done', 'dead_letter')
                      GROUP BY attempts
                      ORDER BY attempts",
                )
                .map_err(q)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)? as u32, row.get::<_, i64>(1)? as u64))
                })
                .map_err(q)?;
            rows.collect::<rusqlite::Result<Vec<_>>>().map_err(q)
        })
        .await
        .map_err(join_err)?
    }

    /// Resume-readiness audit: split the `leased` count into **live** leases (a
    /// worker still holds an unexpired lease — actively in flight) versus
    /// **orphaned** ones (`leased_until` is in the past — a dead or spot-killed
    /// worker), and list which `leased_by` coordinators hold live leases.
    ///
    /// This is the join-free answer to "what would `resume` pick up, and what did
    /// dead workers drop" — the question Ray/vLLM batch users hand-roll by joining
    /// their output file back against their input on keys. forge's lease queue knows
    /// it directly: `reclaimable = pending + orphaned` (nothing lost, nothing
    /// double-run), and `orphaned > 0` is the fingerprint of an interruption that a
    /// `resume`/`sweep` will heal. `now_ms` is passed in so the split is taken
    /// against a single consistent clock reading.
    pub async fn resume_audit(&self, now_ms: i64) -> Result<ResumeAudit, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<ResumeAudit, ForgeError> {
            let conn = conn.lock().unwrap();
            resume_audit_locked(&conn, now_ms)
        })
        .await
        .map_err(join_err)?
    }

    /// [`SqliteQueue::counts`] and [`SqliteQueue::resume_audit`] combined under one
    /// lock acquisition, so `forge audit`'s `reclaimable`/`interrupted` figures come
    /// from a single consistent DB snapshot — two independent locked reads could
    /// otherwise straddle a concurrent lease/ack and disagree with each other.
    pub async fn audit_snapshot(
        &self,
        now_ms: i64,
    ) -> Result<(QueueCounts, ResumeAudit), ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<(QueueCounts, ResumeAudit), ForgeError> {
            let conn = conn.lock().unwrap();
            let counts = counts_locked(&conn)?;
            let audit = resume_audit_locked(&conn, now_ms)?;
            Ok((counts, audit))
        })
        .await
        .map_err(join_err)?
    }

    /// Breakdown of dead-lettered items by classified failure reason
    /// ([`classify_failure`](forge_core::classify_failure)), as `(reason, count)`
    /// most-common first — the "what's failing" signal for `forge status`. Scans only
    /// dead rows (bounded by the dead count, not the job). Inherent, not on the trait.
    pub async fn failure_breakdown(&self) -> Result<Vec<(String, u64)>, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<Vec<(String, u64)>, ForgeError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT last_error FROM items WHERE status = 'dead_letter'")
                .map_err(q)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, Option<String>>(0))
                .map_err(q)?;
            let mut counts: std::collections::HashMap<&'static str, u64> =
                std::collections::HashMap::new();
            for r in rows {
                let err = r.map_err(q)?.unwrap_or_default();
                *counts
                    .entry(forge_core::classify_failure(&err).as_str())
                    .or_insert(0) += 1;
            }
            // Most-common first; ties broken by reason name for deterministic output.
            let mut out: Vec<(String, u64)> = counts
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            Ok(out)
        })
        .await
        .map_err(join_err)?
    }

    /// Borrow the underlying connection — **test-only**. Production callers must go
    /// through the [`Queue`] trait so they can't bypass the lease/mutation contract.
    #[cfg(test)]
    pub(crate) fn connection(&self) -> &Mutex<Connection> {
        &self.conn
    }
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS items (
    custom_id    TEXT PRIMARY KEY,
    method       TEXT NOT NULL DEFAULT 'POST',
    url          TEXT NOT NULL DEFAULT '',
    body         TEXT NOT NULL,                 -- JSON, passed to the engine verbatim
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    leased_until INTEGER,                        -- epoch ms; NULL when pending
    leased_by    TEXT,
    last_error   TEXT
);
-- The lease/reaper hot path scans by (status, leased_until).
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status, leased_until);

CREATE TABLE IF NOT EXISTS job (
    job_id      TEXT PRIMARY KEY,
    input_uri   TEXT,
    output_uri  TEXT,
    model       TEXT,
    concurrency INTEGER,
    created_at  INTEGER,
    status      TEXT NOT NULL DEFAULT 'running'
);

-- Line-range bookkeeping for seek-resume ONLY (never a dependency unit).
CREATE TABLE IF NOT EXISTS shards (
    shard_id          INTEGER PRIMARY KEY,
    byte_offset_start INTEGER NOT NULL,
    byte_offset_end   INTEGER NOT NULL,
    line_start        INTEGER NOT NULL,
    line_end          INTEGER NOT NULL,
    lines_hydrated    INTEGER NOT NULL DEFAULT 0
);
";

// Single-writer lease, emulating SELECT ... FOR UPDATE SKIP LOCKED. The subquery
// is fully materialised before the UPDATE applies (SQLite semantics), so picking
// and claiming the rows is atomic under the one writer. Picks fresh `pending`
// rows AND `leased` rows whose lease has expired (a dead/spot-killed worker).
const LEASE_SQL: &str = "
UPDATE items
   SET status       = 'leased',
       leased_until = ?1,
       leased_by    = ?2,
       attempts     = attempts + 1
 WHERE custom_id IN (
       SELECT custom_id FROM items
        WHERE status = 'pending'
           OR (status = 'leased' AND leased_until IS NOT NULL AND leased_until < ?3)
        ORDER BY rowid
        LIMIT ?4
 )
RETURNING custom_id, method, url, body, attempts, leased_until, leased_by, last_error
";

type RawRow = (
    String,         // custom_id
    String,         // method
    String,         // url
    String,         // body (JSON text)
    i64,            // attempts
    Option<i64>,    // leased_until
    Option<String>, // leased_by
    Option<String>, // last_error
);

impl Queue for SqliteQueue {
    async fn enqueue(&self, items: &[Item]) -> Result<u64, ForgeError> {
        if items.is_empty() {
            return Ok(0);
        }
        let conn = Arc::clone(&self.conn);
        let items = items.to_vec();
        tokio::task::spawn_blocking(move || -> Result<u64, ForgeError> {
            let mut guard = conn.lock().unwrap();
            let tx = guard.transaction().map_err(q)?;
            let mut inserted = 0u64;
            {
                // ON CONFLICT DO NOTHING → re-ingest on resume is a no-op.
                let mut stmt = tx
                    .prepare(
                        "INSERT INTO items (custom_id, method, url, body)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(custom_id) DO NOTHING",
                    )
                    .map_err(q)?;
                for it in &items {
                    let body = serde_json::to_string(&it.body)?;
                    inserted += stmt
                        .execute(params![it.custom_id, it.method, it.url, body])
                        .map_err(q)? as u64;
                }
            }
            tx.commit().map_err(q)?;
            Ok(inserted)
        })
        .await
        .map_err(join_err)?
    }

    async fn lease(&self, limit: usize, lease_for: Duration) -> Result<Vec<Item>, ForgeError> {
        let conn = Arc::clone(&self.conn);
        let coordinator = self.coordinator_id.clone();
        let now = now_ms();
        let leased_until = now + lease_for.as_millis() as i64;
        let limit = limit as i64;
        tokio::task::spawn_blocking(move || -> Result<Vec<Item>, ForgeError> {
            let conn = conn.lock().unwrap();
            let mut stmt = conn.prepare(LEASE_SQL).map_err(q)?;
            // RETURNING makes this a query: stepping every row is what applies the
            // UPDATE, so we must consume the whole iterator.
            let raw: Vec<RawRow> = stmt
                .query_map(params![leased_until, coordinator, now, limit], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                })
                .map_err(q)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(q)?;

            let mut items = Vec::with_capacity(raw.len());
            for (custom_id, method, url, body, attempts, leased_until, leased_by, last_error) in raw
            {
                items.push(Item {
                    custom_id,
                    method,
                    url,
                    body: serde_json::from_str(&body)?,
                    status: ItemState::Leased,
                    attempts: attempts as u32,
                    leased_until,
                    leased_by,
                    last_error,
                });
            }
            Ok(items)
        })
        .await
        .map_err(join_err)?
    }

    async fn ack(&self, custom_id: &str, attempt: u32) -> Result<bool, ForgeError> {
        let conn = Arc::clone(&self.conn);
        let custom_id = custom_id.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool, ForgeError> {
            let conn = conn.lock().unwrap();
            // Lease-fenced: the `attempts=?2` predicate is the lease generation, so a
            // stale worker whose lease expired and was re-leased to a newer attempt
            // can't close the row. A 0-row update is a tolerated no-op (the store
            // write is idempotent, so the effect is still exactly-once).
            let n = conn
                .execute(
                    "UPDATE items SET status='done', leased_until=NULL, leased_by=NULL
                     WHERE custom_id=?1 AND status='leased' AND attempts=?2",
                    params![custom_id, attempt],
                )
                .map_err(q)?;
            Ok(n > 0)
        })
        .await
        .map_err(join_err)?
    }

    async fn reap(&self) -> Result<u64, ForgeError> {
        let conn = Arc::clone(&self.conn);
        let now = now_ms();
        tokio::task::spawn_blocking(move || -> Result<u64, ForgeError> {
            let conn = conn.lock().unwrap();
            let n = conn
                .execute(
                    "UPDATE items SET status='pending', leased_until=NULL, leased_by=NULL
                     WHERE status='leased' AND leased_until IS NOT NULL AND leased_until < ?1",
                    params![now],
                )
                .map_err(q)?;
            Ok(n as u64)
        })
        .await
        .map_err(join_err)?
    }

    async fn dead_letter(
        &self,
        custom_id: &str,
        attempt: u32,
        error: &str,
    ) -> Result<bool, ForgeError> {
        let conn = Arc::clone(&self.conn);
        let custom_id = custom_id.to_string();
        let error = error.to_string();
        tokio::task::spawn_blocking(move || -> Result<bool, ForgeError> {
            let conn = conn.lock().unwrap();
            // Lease-fenced on the `attempts` generation, like `ack`.
            let n = conn
                .execute(
                    "UPDATE items SET status='dead_letter', last_error=?3,
                            leased_until=NULL, leased_by=NULL
                     WHERE custom_id=?1 AND status='leased' AND attempts=?2",
                    params![custom_id, attempt, error],
                )
                .map_err(q)?;
            Ok(n > 0)
        })
        .await
        .map_err(join_err)?
    }

    async fn counts(&self) -> Result<QueueCounts, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<QueueCounts, ForgeError> {
            let conn = conn.lock().unwrap();
            counts_locked(&conn)
        })
        .await
        .map_err(join_err)?
    }

    async fn record_shard(&self, shard: &Shard) -> Result<(), ForgeError> {
        let conn = Arc::clone(&self.conn);
        let shard = *shard;
        tokio::task::spawn_blocking(move || -> Result<(), ForgeError> {
            let conn = conn.lock().unwrap();
            // The DB auto-assigns the shard_id (rowid): each recorded shard is just an
            // append-only ingest checkpoint, and `hydrated_through` reads only the max
            // byte offset, so a re-ingest's new shards simply extend the table.
            conn.execute(
                "INSERT INTO shards
                     (byte_offset_start, byte_offset_end, line_start, line_end, lines_hydrated)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    shard.byte_offset_start as i64,
                    shard.byte_offset_end as i64,
                    shard.line_start as i64,
                    shard.line_end as i64,
                    shard.lines_hydrated as i64,
                ],
            )
            .map_err(q)?;
            Ok(())
        })
        .await
        .map_err(join_err)?
    }

    async fn hydrated_through(&self) -> Result<Option<(u64, u64)>, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || -> Result<Option<(u64, u64)>, ForgeError> {
            let conn = conn.lock().unwrap();
            // The furthest-hydrated shard is the ingest high-water mark.
            conn.query_row(
                "SELECT byte_offset_end, line_end FROM shards
                  ORDER BY byte_offset_end DESC LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(q)
        })
        .await
        .map_err(join_err)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(id: &str) -> Item {
        Item {
            custom_id: id.to_string(),
            method: "POST".into(),
            url: "/v1/chat/completions".into(),
            body: json!({"model": "m", "messages": []}),
            status: ItemState::Pending,
            attempts: 0,
            leased_until: None,
            leased_by: None,
            last_error: None,
        }
    }

    #[test]
    fn open_in_memory_creates_schema() {
        let q = SqliteQueue::open_in_memory().expect("open");
        let conn = q.connection().lock().unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='items'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "items table should exist");
    }

    #[tokio::test]
    async fn enqueue_lease_ack_roundtrip() {
        let q = SqliteQueue::open_in_memory().unwrap();
        let n = q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(q.counts().await.unwrap().pending, 3);

        let leased = q.lease(2, Duration::from_secs(300)).await.unwrap();
        assert_eq!(leased.len(), 2);
        assert!(leased
            .iter()
            .all(|i| i.status == ItemState::Leased && i.attempts == 1));
        // body round-trips through JSON text
        assert_eq!(leased[0].body["model"], "m");

        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased, c.done), (1, 2, 0));

        assert!(q
            .ack(&leased[0].custom_id, leased[0].attempts)
            .await
            .unwrap());
        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased, c.done), (1, 1, 1));
    }

    #[tokio::test]
    async fn ack_is_lease_fenced() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a")]).await.unwrap();
        let leased = q.lease(1, Duration::from_secs(300)).await.unwrap();
        let gen = leased[0].attempts; // lease generation (== 1)

        // A stale attempt (wrong generation) must NOT close the row.
        assert!(!q.ack("a", gen + 1).await.unwrap());
        assert_eq!(q.counts().await.unwrap().leased, 1);

        // The real lease holder acks successfully.
        assert!(q.ack("a", gen).await.unwrap());
        assert_eq!(q.counts().await.unwrap().done, 1);
    }

    #[test]
    fn job_metadata_roundtrip() {
        let q = SqliteQueue::open_in_memory().unwrap();
        assert_eq!(q.job_output_uri().unwrap(), None);
        assert_eq!(q.job_concurrency().unwrap(), None);
        q.record_job("in.jsonl", "out.jsonl", 8).unwrap();
        assert_eq!(q.job_output_uri().unwrap().as_deref(), Some("out.jsonl"));
        assert_eq!(q.job_concurrency().unwrap(), Some(8));
        // Re-running replaces both.
        q.record_job("in.jsonl", "other.jsonl", 16).unwrap();
        assert_eq!(q.job_output_uri().unwrap().as_deref(), Some("other.jsonl"));
        assert_eq!(q.job_concurrency().unwrap(), Some(16));
    }

    #[tokio::test]
    async fn enqueue_is_idempotent() {
        let q = SqliteQueue::open_in_memory().unwrap();
        assert_eq!(q.enqueue(&[item("a"), item("b")]).await.unwrap(), 2);
        // Re-ingesting the same ids (e.g. on resume) inserts nothing new.
        assert_eq!(q.enqueue(&[item("a"), item("b")]).await.unwrap(), 0);
        assert_eq!(q.counts().await.unwrap().total(), 2);
    }

    #[tokio::test]
    async fn reaper_requeues_expired_leases() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b")]).await.unwrap();
        // Zero-length lease: leased_until == now, so it's expired a moment later.
        let leased = q.lease(2, Duration::from_millis(0)).await.unwrap();
        assert_eq!(leased.len(), 2);
        tokio::time::sleep(Duration::from_millis(5)).await;

        let requeued = q.reap().await.unwrap();
        assert_eq!(requeued, 2);
        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased), (2, 0));
        // attempts was incremented by the (now-undone) lease — the re-lease will
        // bump it again, which is how poison items eventually exhaust retries.
        let released = q.lease(2, Duration::from_secs(300)).await.unwrap();
        assert!(released.iter().all(|i| i.attempts == 2));
    }

    #[tokio::test]
    async fn resume_audit_splits_live_from_orphaned_leases() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c"), item("d")])
            .await
            .unwrap();

        // Two items leased with a real (live) lease; ack one → done.
        let live = q.lease(2, Duration::from_secs(300)).await.unwrap();
        q.ack(&live[0].custom_id, live[0].attempts).await.unwrap();

        // One item leased with a zero-length lease → immediately orphaned (a dead /
        // spot-killed worker). It is NOT reaped yet — audit must see it as orphaned.
        q.lease(1, Duration::from_millis(0)).await.unwrap();

        // State now: 1 pending (d), 1 live lease, 1 orphaned lease, 1 done.
        let now = now_ms();
        let audit = q.resume_audit(now).await.unwrap();
        assert_eq!(audit.live_leases, 1, "one unexpired lease is in flight");
        assert_eq!(audit.orphaned_leases, 1, "one expired lease is reclaimable");
        assert_eq!(audit.live_holders.iter().map(|(_, n)| n).sum::<u64>(), 1);

        let c = q.counts().await.unwrap();
        assert_eq!(
            c.pending + audit.orphaned_leases,
            2,
            "reclaimable = pending + orphaned"
        );

        // A clean, drained queue reports no interruption.
        let q2 = SqliteQueue::open_in_memory().unwrap();
        q2.enqueue(&[item("x")]).await.unwrap();
        let leased = q2.lease(1, Duration::from_secs(300)).await.unwrap();
        q2.ack(&leased[0].custom_id, leased[0].attempts)
            .await
            .unwrap();
        let clean = q2.resume_audit(now_ms()).await.unwrap();
        assert_eq!((clean.live_leases, clean.orphaned_leases), (0, 0));
    }

    #[tokio::test]
    async fn audit_snapshot_matches_separate_counts_and_resume_audit_calls() {
        // `audit_snapshot` must return the same numbers `counts()` +
        // `resume_audit()` would (it's the single-lock combination of both), so
        // `forge audit`'s reclaimable/interrupted figures stay a drop-in
        // replacement for the old two-call sequence.
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap();
        q.lease(1, Duration::from_secs(300)).await.unwrap();
        q.lease(1, Duration::from_millis(0)).await.unwrap();

        let now = now_ms();
        let (counts, audit) = q.audit_snapshot(now).await.unwrap();
        assert_eq!(counts, q.counts().await.unwrap());
        assert_eq!(audit, q.resume_audit(now).await.unwrap());
        assert_eq!(audit.live_leases, 1);
        assert_eq!(audit.orphaned_leases, 1);
        assert_eq!(counts.pending, 1);
    }

    #[tokio::test]
    async fn dead_letter_quarantines() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("poison")]).await.unwrap();
        let leased = q.lease(1, Duration::from_secs(300)).await.unwrap();
        assert_eq!(leased.len(), 1);
        assert!(q
            .dead_letter("poison", leased[0].attempts, "always 500")
            .await
            .unwrap());
        let c = q.counts().await.unwrap();
        assert_eq!((c.leased, c.dead), (0, 1));
    }

    #[tokio::test]
    async fn attempt_histogram_counts_terminal_items_by_attempt() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("poison")])
            .await
            .unwrap();

        // a, b complete on the first attempt.
        let first = q.lease(2, Duration::from_secs(300)).await.unwrap();
        for it in &first {
            q.ack(&it.custom_id, it.attempts).await.unwrap();
        }
        // poison: lease (attempt 1), expire+reap, re-lease (attempt 2), dead-letter.
        q.lease(1, Duration::from_millis(0)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        q.reap().await.unwrap();
        let again = q.lease(1, Duration::from_secs(300)).await.unwrap();
        assert_eq!(again[0].attempts, 2);
        q.dead_letter("poison", again[0].attempts, "boom")
            .await
            .unwrap();

        // Terminal items only: 2 done at attempt 1, 1 dead at attempt 2.
        let hist = q.attempt_histogram().await.unwrap();
        assert_eq!(hist, vec![(1, 2), (2, 1)]);
    }

    #[tokio::test]
    async fn failure_breakdown_classifies_dead_items() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap();
        let leased = q.lease(3, Duration::from_secs(300)).await.unwrap();
        let att = |id: &str| leased.iter().find(|i| i.custom_id == id).unwrap().attempts;
        q.dead_letter(
            "a",
            att("a"),
            "worker: retries exhausted after 5 attempt(s): HTTP 500 Internal Server Error",
        )
        .await
        .unwrap();
        q.dead_letter(
            "b",
            att("b"),
            "worker: retries exhausted after 5 attempt(s): HTTP 500 Internal Server Error",
        )
        .await
        .unwrap();
        q.dead_letter(
            "c",
            att("c"),
            "worker: retries exhausted after 3 attempt(s): validation failed: not valid JSON",
        )
        .await
        .unwrap();

        // Most-common first.
        let breakdown = q.failure_breakdown().await.unwrap();
        assert_eq!(
            breakdown,
            vec![
                ("server_error".to_string(), 2),
                ("validation".to_string(), 1)
            ]
        );
    }

    #[tokio::test]
    async fn lease_skips_terminal_and_respects_limit() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c"), item("d")])
            .await
            .unwrap();
        // Lease all, ack two → done; the other two stay leased (not expired).
        let first = q.lease(2, Duration::from_secs(300)).await.unwrap();
        q.ack(&first[0].custom_id, first[0].attempts).await.unwrap();
        q.ack(&first[1].custom_id, first[1].attempts).await.unwrap();
        // Only the 2 still-pending rows are leasable; done rows are never re-leased.
        let second = q.lease(10, Duration::from_secs(300)).await.unwrap();
        assert_eq!(second.len(), 2);
        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased, c.done), (0, 2, 2));
    }
}
