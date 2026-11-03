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

/// SQLite says only "unable to open database file" when it cannot create the checkpoint,
/// or the `-wal`/`-shm` files WAL keeps beside it. The usual cause is a directory this
/// process cannot write — in a container, a mounted directory owned by another uid than
/// the image's — so when that is the case, say so.
fn open_err(path: &Path, e: rusqlite::Error) -> ForgeError {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let probe = dir.join(format!(".forge-write-probe-{}", std::process::id()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            q(e)
        }
        // EACCES, or EROFS (30) for a read-only mount.
        Err(io)
            if io.kind() == std::io::ErrorKind::PermissionDenied
                || io.raw_os_error() == Some(30) =>
        {
            ForgeError::Queue(format!(
                "{e}: {} is not writable by this process ({io}), and the checkpoint and its \
                 -wal/-shm files live there. In a container, run as the directory's owner: \
                 --user \"$(id -u):$(id -g)\"",
                std::fs::canonicalize(dir)
                    .unwrap_or_else(|_| dir.to_path_buf())
                    .display()
            ))
        }
        Err(_) => q(e),
    }
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

/// Take a **consistent** copy of a checkpoint DB that another process may be writing.
///
/// A backup of this file cannot be `cp`. The queue runs in WAL mode, so the committed
/// state at any instant is spread across `state.db`, `state.db-wal` and `state.db-shm`;
/// copying them with three separate reads, in any order, can capture a torn set —
/// committed items missing, or a WAL that refers to pages the copied `.db` does not
/// have. The failure is silent: the copy opens fine and is simply missing work.
///
/// `VACUUM INTO` runs inside a read transaction, so it writes a single self-contained
/// file holding exactly the state committed when it started. Writers are not blocked,
/// which is what makes an online backup of a live intake root possible at all.
///
/// The destination must not exist — SQLite refuses to overwrite, and so do we: a backup
/// that silently replaces yesterday's is not a backup.
pub fn snapshot_db(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> Result<(), ForgeError> {
    let (src, dest) = (src.as_ref(), dest.as_ref());
    if dest.exists() {
        return Err(ForgeError::Config(format!(
            "refusing to overwrite an existing snapshot at {}",
            dest.display()
        )));
    }
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    // Read-only: a backup must never be able to modify what it is backing up, and
    // opening read-write would create an empty DB if `src` were mistyped.
    let conn =
        Connection::open_with_flags(src, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(q)?;
    conn.busy_timeout(std::time::Duration::from_secs(60))
        .map_err(q)?;
    // The path goes in as a bound parameter, not formatted into the SQL — a job
    // directory is operator-controlled, but quoting a path into a statement is a habit
    // worth not having.
    conn.execute("VACUUM INTO ?1", [dest.to_string_lossy().as_ref()])
        .map_err(q)?;
    Ok(())
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
        let conn = Connection::open(path).map_err(|e| open_err(path, e))?;
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
        .map_err(|e| open_err(path, e))?;
        conn.execute_batch(SCHEMA).map_err(q)?;
        // Migration for checkpoints created before the job table carried the fleet
        // config: adding a column that already exists is the only failure mode, so
        // the error is deliberately ignored.
        let _ = conn.execute("ALTER TABLE job ADD COLUMN concurrency INTEGER", []);
        check_and_stamp_version(&conn)?;
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
        check_and_stamp_version(&conn)?;
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

    /// Every dead-lettered item, as `(custom_id, last_error)`, in insertion order.
    ///
    /// Inherent rather than on the [`Queue`] trait, matching
    /// [`failure_breakdown`](Self::failure_breakdown) and
    /// [`attempt_histogram`](Self::attempt_histogram): a leaner backend need not carry
    /// a requeue path to be a correct queue.
    pub async fn dead_letters(&self) -> Result<Vec<(String, Option<String>)>, ForgeError> {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(
            move || -> Result<Vec<(String, Option<String>)>, ForgeError> {
                let conn = conn.lock().unwrap();
                let mut stmt = conn
                    .prepare(
                        "SELECT custom_id, last_error FROM items
                      WHERE status = 'dead_letter' ORDER BY rowid",
                    )
                    .map_err(q)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                    })
                    .map_err(q)?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(q)
            },
        )
        .await
        .map_err(join_err)?
    }

    /// `DeadLetter → Pending` for `ids`, returning the ids actually transitioned.
    ///
    /// **The retry budget is reset with the state.** `attempts` is what
    /// [`Queue::lease`] increments and what the run loop compares against
    /// `--max-attempts`, so a requeued item that kept its exhausted count would be
    /// leased once and dead-letter again immediately — a requeue that looks like it
    /// worked and changes nothing. `last_error` is cleared for the same reason: it
    /// describes a run that is no longer this item's latest.
    ///
    /// **Only `dead_letter` rows move.** A `done` item is never resurrected (its
    /// result is already emitted and billed), and a `pending`/`leased` item is not
    /// touched by an operator's stale id list. The returned set is the honest answer
    /// to "what did this actually do", which the caller reconciles against the store.
    ///
    /// Idempotent: a second call for the same ids transitions nothing and returns
    /// empty.
    pub async fn requeue_dead(&self, ids: &[String]) -> Result<Vec<String>, ForgeError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = Arc::clone(&self.conn);
        let ids = ids.to_vec();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>, ForgeError> {
            let mut guard = conn.lock().unwrap();
            // One transaction: either every listed item is runnable again or none is,
            // so a crash cannot leave an operator reconciling a half-applied requeue
            // against a dead-letter file that was already pruned.
            let tx = guard.transaction().map_err(q)?;
            let mut moved = Vec::new();
            {
                let mut stmt = tx
                    .prepare(
                        "UPDATE items
                            SET status='pending', attempts=0, last_error=NULL,
                                leased_until=NULL, leased_by=NULL
                          WHERE custom_id=?1 AND status='dead_letter'
                      RETURNING custom_id",
                    )
                    .map_err(q)?;
                for id in &ids {
                    let mut rows = stmt.query(params![id]).map_err(q)?;
                    if let Some(row) = rows.next().map_err(q)? {
                        moved.push(row.get::<_, String>(0).map_err(q)?);
                    }
                }
            }
            tx.commit().map_err(q)?;
            Ok(moved)
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

/// The checkpoint layout this binary reads and writes.
///
/// **Version 1 is the layout that already existed.** Checkpoints written before this
/// field existed carry no version row, and are stamped `1` on first open rather than
/// migrated — because they *are* version 1; nothing about their tables changed. The
/// point of recording it is forward: a future layout change can now be detected
/// instead of silently misread, and this binary refuses a checkpoint written by a
/// newer forge rather than interpreting its tables under the old meaning.
pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

/// The `meta` key carrying [`CHECKPOINT_SCHEMA_VERSION`].
const META_SCHEMA_VERSION: &str = "schema_version";
/// Provenance: which forge wrote (or last opened) this checkpoint. Never read by the
/// code — it is here for the operator reading a support ticket.
const META_FORGE_VERSION: &str = "forge_version";

/// Read the recorded schema version, refuse a newer one, and stamp an unversioned
/// checkpoint with the current version.
///
/// Refusing is the entire feature. A checkpoint whose `items` table means something
/// different is one this binary would happily lease from, ack into, and bill — the
/// failure mode a version check exists to convert into a message.
fn check_and_stamp_version(conn: &Connection) -> Result<(), ForgeError> {
    let recorded: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![META_SCHEMA_VERSION],
            |row| row.get(0),
        )
        .optional()
        .map_err(q)?;

    match recorded.as_deref().map(str::parse::<u32>) {
        Some(Ok(v)) if v > CHECKPOINT_SCHEMA_VERSION => {
            return Err(ForgeError::Store(format!(
                "checkpoint was written by a newer forge (checkpoint schema v{v}, this \
                 binary reads v{}). Reading it under the old layout could lease, ack or \
                 bill items that no longer mean what this binary thinks they mean. \
                 Upgrade forge, or start a fresh checkpoint.",
                CHECKPOINT_SCHEMA_VERSION
            )));
        }
        Some(Ok(v)) if v < CHECKPOINT_SCHEMA_VERSION => {
            // No older version exists yet, so this arm cannot fire today. It is here so
            // that when one does, the seam is a named refusal with a remedy rather than
            // a silent misread — the same reason the newer-version arm exists.
            return Err(ForgeError::Store(format!(
                "checkpoint uses schema v{v}, this binary reads v{}, and no migration \
                 from v{v} exists. Finish the job with the forge that wrote it, or start \
                 a fresh checkpoint.",
                CHECKPOINT_SCHEMA_VERSION
            )));
        }
        Some(Ok(_)) => {}
        Some(Err(_)) => {
            let raw = recorded.unwrap_or_default();
            return Err(ForgeError::Store(format!(
                "checkpoint records an unreadable schema version {raw:?}. This file was \
                 not written by forge, or was edited by hand; refusing to interpret its \
                 tables."
            )));
        }
        // Unversioned: a checkpoint from before the version was recorded. It IS v1 —
        // no table changed — so stamp it rather than refuse a working job.
        None => {}
    }

    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![META_SCHEMA_VERSION, CHECKPOINT_SCHEMA_VERSION.to_string()],
    )
    .map_err(q)?;
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![META_FORGE_VERSION, env!("CARGO_PKG_VERSION")],
    )
    .map_err(q)?;
    Ok(())
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
-- The lease/reaper hot path scans by (status, leased_until). Its implicit trailing
-- rowid is load-bearing: it is what lets the lease read insertion order straight out
-- of the index instead of sorting the whole pending set. See LEASE_SQL.
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

-- Checkpoint format identity. See `CHECKPOINT_SCHEMA_VERSION`.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
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
// rows AND `leased` rows whose lease has expired (a dead/spot-killed worker),
// oldest-first by insertion order across both sets — with one stated exception, at
// the end of this comment.
//
// **Why the two branches are separate, and why neither says `ORDER BY rowid`.**
// This began as one predicate — `status = 'pending' OR (status = 'leased' AND
// leased_until < ?)` — with `ORDER BY rowid LIMIT ?`. Correct, and quadratic: no
// index can serve both that `OR` and an ordering on `rowid`, so SQLite answered
// `MULTI-INDEX OR` + `USE TEMP B-TREE FOR ORDER BY` and sorted the **entire**
// pending set on every call before the `LIMIT` bit. Since `BatchRun` leases once
// per completion, that is a full sort per item: at 1M queued it cost 211 ms to
// return ONE row, and the fan-out bench managed 7.7 items/s against 269 at 50k
// (BENCHMARKS.md).
//
// The fix is to ask for the order the index already has. `idx_items_status` is
// `(status, leased_until)`, so under an equality on `status` its entries are
// ordered by `leased_until` and then — via the index's implicit trailing rowid —
// by `rowid`. `ORDER BY leased_until, rowid` is therefore satisfied by a scan
// with early exit, while `ORDER BY rowid` alone is not (SQLite cannot prove
// `leased_until` is constant, so it sorts). Every pending row has
// `leased_until IS NULL`, so within that branch the two orderings are the same
// rows in the same order — insertion order, which `--prefix-bucket` relies on.
//
// Splitting the `OR` into two `LIMIT`-ed branches keeps each one index-served;
// the outer `ORDER BY rid` then interleaves at most `2 × limit` rows, which is
// the sort SQLite still reports and which costs nothing at that size — 0.115 ms
// instead of 211 ms, and flat in queue size rather than linear.
//
// **Where selection differs from the single-predicate form, and why that is
// acceptable.** Each branch applies its own `LIMIT`, and the expired branch is
// ordered by `leased_until` rather than by `rowid`. So when MORE than `limit`
// leases have expired AND they carry DISTINCT `leased_until` values, that branch
// offers the oldest leases rather than the lowest rowids, and the outer interleave
// can therefore return a different set than `ORDER BY rowid` over the whole `OR`
// would have.
//
// Both conditions are needed. At or below the cap the branch drops nothing, so
// the two forms agree by construction; one row over is enough to part them. That
// is why the 1M cross-check recorded in BENCHMARKS.md matched row-for-row and
// could not have done otherwise: it offered 500 expired rows against a limit of
// 1024, leaving the branch 524 short of its cap. The second condition is what
// makes the first bite — expirations from several different lease calls, most
// sharply a retried row whose renewed lease is younger than its neighbours'
// despite its low rowid.
//
// Nothing is lost when it happens: every row the branch passed over is still
// expired, still eligible, and reclaimed by a later call once the backlog ahead of
// it drains (re-leasing moves a row's `leased_until` into the future, so the
// expired set strictly shrinks). What changes is reclaim ORDER — oldest-lease-first
// instead of lowest-rowid-first — and the only thing riding on that is the
// cache-locality `--prefix-bucket` buys, which a reclaimed row has already left
// behind by being reclaimed at all. The pending branch, which is the hot path and
// the one `--prefix-bucket` is about, is exactly insertion-ordered: every write
// that sets `status = 'pending'` also sets `leased_until = NULL` (the reaper and
// the dead-letter requeue both do), and `enqueue` never writes the column.
//
// `rowid` is not indexable here (the table's primary key is `custom_id TEXT`, so
// `CREATE INDEX ... ON items(rowid)` is `no such column: rowid`); the index's
// implicit rowid is the only handle on insertion order, which is why the ORDER BY
// has to be phrased to reach it.
const LEASE_SQL: &str = "
UPDATE items
   SET status       = 'leased',
       leased_until = ?1,
       leased_by    = ?2,
       attempts     = attempts + 1
 WHERE custom_id IN (
       SELECT custom_id FROM (
           SELECT * FROM (
               SELECT custom_id, rowid AS rid FROM items
                WHERE status = 'pending'
                ORDER BY leased_until, rowid
                LIMIT ?4
           )
           UNION ALL
           SELECT * FROM (
               SELECT custom_id, rowid AS rid FROM items
                WHERE status = 'leased'
                  AND leased_until IS NOT NULL
                  AND leased_until < ?3
                ORDER BY leased_until, rowid
                LIMIT ?4
           )
       )
        ORDER BY rid
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

    /// The guard on the quadratic lease. `ORDER BY rowid` over the `OR` predicate made
    /// SQLite sort the whole pending set on every lease — 211 ms to return one row at
    /// 1M queued, and the fan-out bench fell from 269 items/s to 7.7 (BENCHMARKS.md).
    /// The plan, not a stopwatch, is what pins it: each branch must be an index SEARCH,
    /// and the only sort allowed is the outer interleave of at most `2 × limit` rows.
    #[tokio::test]
    async fn the_lease_reads_insertion_order_out_of_the_index_and_does_not_sort_the_queue() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap();

        let plan: Vec<String> = {
            let conn = q.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {LEASE_SQL}"))
                .expect("explain");
            let rows = stmt
                .query_map(params![0i64, "c", 0i64, 1i64], |r| r.get::<_, String>(3))
                .expect("query plan")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("plan rows");
            rows
        };
        let plan_text = plan.join("\n");

        let searches = plan
            .iter()
            .filter(|l| l.contains("USING INDEX idx_items_status"))
            .count();
        assert_eq!(
            searches, 2,
            "both lease branches must be served by idx_items_status; plan was:\n{plan_text}"
        );
        assert!(
            !plan_text.contains("SCAN items"),
            "the lease must never table-scan; plan was:\n{plan_text}"
        );
        // The shape this replaced: one predicate whose two index searches feed a
        // single UNCAPPED row set, which the `ORDER BY` then sorts whole. Without
        // this line the test passes against that statement — the sort below is one
        // either way, and only its input size differs.
        assert!(
            !plan_text.contains("MULTI-INDEX OR"),
            "the two lookups must stay separately LIMIT-ed, not be OR-unioned into one \
             unbounded set; plan was:\n{plan_text}"
        );
        // Exactly one sort, and — given the assertion above — it is the outer
        // interleave of the two capped branches, so at most `2 × limit` rows. A
        // second one means a branch went back to sorting its whole result set.
        let sorts = plan
            .iter()
            .filter(|l| l.contains("USE TEMP B-TREE FOR ORDER BY"))
            .count();
        assert!(
            sorts <= 1,
            "a lease branch is sorting again (expect at most the outer interleave); plan was:\n{plan_text}"
        );
    }

    /// Insertion order is the contract `--prefix-bucket` buys: it reorders the input so
    /// prefix-similar items are adjacent, which only pays off if the lease hands them
    /// out in that order.
    #[tokio::test]
    async fn leases_follow_insertion_order_not_custom_id_order() {
        let q = SqliteQueue::open_in_memory().unwrap();
        // Ids deliberately descending, so id order and insertion order disagree.
        q.enqueue(&[item("z"), item("y"), item("x"), item("w")])
            .await
            .unwrap();

        let first = q.lease(2, Duration::from_secs(300)).await.unwrap();
        let ids: Vec<&str> = first.iter().map(|i| i.custom_id.as_str()).collect();
        assert_eq!(ids, vec!["z", "y"], "insertion order, not sorted by id");

        let second = q.lease(2, Duration::from_secs(300)).await.unwrap();
        let ids: Vec<&str> = second.iter().map(|i| i.custom_id.as_str()).collect();
        assert_eq!(ids, vec!["x", "w"]);
    }

    /// One lease call must serve both sets — fresh work and a dead worker's expired
    /// lease — interleaved in insertion order, which is what the two-branch UNION has
    /// to preserve.
    #[tokio::test]
    async fn one_lease_takes_expired_leases_and_pending_work_in_insertion_order() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c"), item("d")])
            .await
            .unwrap();

        // "a" and "b" go to a worker that then dies: a zero-length lease is expired
        // the moment it is taken.
        let dead = q.lease(2, Duration::from_millis(0)).await.unwrap();
        assert_eq!(dead.len(), 2);
        tokio::time::sleep(Duration::from_millis(5)).await;

        // A fresh lease for everything: the two expired rows come first (they were
        // inserted first), then the never-leased ones.
        let all = q.lease(4, Duration::from_secs(300)).await.unwrap();
        let ids: Vec<&str> = all.iter().map(|i| i.custom_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);
        // The reclaimed pair carries its second attempt; the fresh pair its first.
        assert_eq!(all[0].attempts, 2);
        assert_eq!(all[1].attempts, 2);
        assert_eq!(all[2].attempts, 1);
        assert_eq!(all[3].attempts, 1);
        assert_eq!(q.counts().await.unwrap().leased, 4);
    }

    /// The one case where selection differs from the single-predicate `ORDER BY rowid`
    /// form the lease replaced: when more expired leases are on offer than the limit,
    /// the expired branch takes the OLDEST leases rather than the lowest rowids. Pinned
    /// because it is a real behaviour change, and because the argument for accepting it
    /// is that nothing is lost — which is the second half of this test.
    #[tokio::test]
    async fn a_saturated_expired_branch_takes_the_oldest_leases_not_the_lowest_rowids() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c"), item("d")])
            .await
            .unwrap();

        // All four are expired leases held by a worker that died, but "a" — the lowest
        // rowid — was retried last, so its lease is the youngest of the four. Written
        // straight into the row: leasing and sleeping to build this would be pinning
        // the scheduler rather than the statement.
        {
            let conn = q.conn.lock().unwrap();
            for (id, until) in [("a", 40i64), ("b", 10), ("c", 20), ("d", 30)] {
                let n = conn
                    .execute(
                        "UPDATE items
                            SET status = 'leased', leased_until = ?2, leased_by = 'dead'
                          WHERE custom_id = ?1",
                        params![id, until],
                    )
                    .expect("seed expired lease");
                assert_eq!(n, 1, "seeding {id} matched no row");
            }
        }

        // Every one of those `leased_until` values is long past, so all four are
        // eligible and only two fit. Sorted because what is under test is which rows
        // the statement picks, not the order `RETURNING` hands them back in.
        let mut ids: Vec<String> = q
            .lease(2, Duration::from_secs(300))
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.custom_id)
            .collect();
        ids.sort();
        // `ORDER BY rowid` over the whole `OR` would have said ["a", "b"].
        assert_eq!(ids, vec!["b", "c"]);

        // Nothing is lost by passing "a" over: it is still expired, still eligible, and
        // comes back on the very next call. Re-leasing only ever moves `leased_until`
        // forward, so a row cannot be starved by rows that keep overtaking it.
        let mut ids: Vec<String> = q
            .lease(2, Duration::from_secs(300))
            .await
            .unwrap()
            .into_iter()
            .map(|i| i.custom_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "d"]);
        assert_eq!(q.counts().await.unwrap().leased, 4);
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

    #[test]
    #[cfg(unix)]
    fn an_unwritable_checkpoint_directory_is_named() {
        // What the image's quick start hit on any mounted directory it did not own:
        // "queue: unable to open database file: state.db", and nothing about why.
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("forge-ckpt-ro-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        // root writes anywhere, so there is nothing to see as root.
        let as_root = std::fs::write(dir.join("probe"), b"").is_ok();
        let res = SqliteQueue::open(dir.join("state.db"));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        if as_root {
            return;
        }
        let err = res
            .err()
            .expect("a checkpoint in a read-only directory must not open");
        let msg = err.to_string();
        assert!(msg.contains("is not writable by this process"), "{msg}");
        assert!(msg.contains("--user"), "{msg}");
    }

    #[test]
    fn a_pre_versioning_checkpoint_is_stamped_not_refused() {
        // Version 1 IS the layout those files already have — nothing about their tables
        // changed — so a working job must not be broken by the introduction of a version
        // field. Recording it protects the NEXT layout change.
        let dir = std::env::temp_dir().join(format!("forge-ckpt-ver-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");

        // A checkpoint as an older forge left it: real tables, real rows, no meta table.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE items (custom_id TEXT PRIMARY KEY, method TEXT NOT NULL DEFAULT 'POST',
                     url TEXT NOT NULL DEFAULT '', body TEXT NOT NULL,
                     status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                     leased_until INTEGER, leased_by TEXT, last_error TEXT);
                 INSERT INTO items (custom_id, body) VALUES ('a', '{}');",
            )
            .unwrap();
        }
        let q = SqliteQueue::open(&path).expect("a pre-versioning checkpoint must still open");
        {
            let conn = q.connection().lock().unwrap();
            let v: String = conn
                .query_row(
                    "SELECT value FROM meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(v, CHECKPOINT_SCHEMA_VERSION.to_string());
            let n: i64 = conn
                .query_row("SELECT count(*) FROM items", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 1, "the operator's in-flight work is untouched");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_checkpoint_from_a_newer_forge_is_refused_by_name() {
        // The failure this exists to prevent: leasing, acking and BILLING items out of
        // a table whose columns no longer mean what this binary thinks they mean.
        let dir = std::env::temp_dir().join(format!("forge-ckpt-newer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");
        SqliteQueue::open(&path).unwrap();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE meta SET value = ?1 WHERE key='schema_version'",
                params![(CHECKPOINT_SCHEMA_VERSION + 1).to_string()],
            )
            .unwrap();
        }
        let err = match SqliteQueue::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("a newer checkpoint must not open"),
        };
        let msg = err.to_string();
        assert!(msg.contains("newer forge"), "unhelpful: {msg}");
        assert!(
            msg.contains("Upgrade forge"),
            "must state the remedy: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unreadable_schema_version_is_refused_rather_than_assumed() {
        let dir = std::env::temp_dir().join(format!("forge-ckpt-junk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");
        SqliteQueue::open(&path).unwrap();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE meta SET value='banana' WHERE key='schema_version'",
                [],
            )
            .unwrap();
        }
        let err = match SqliteQueue::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("junk must not be read as a version"),
        };
        assert!(
            err.to_string().contains("unreadable schema version"),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopening_a_versioned_checkpoint_is_a_no_op() {
        let dir = std::env::temp_dir().join(format!("forge-ckpt-reopen-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");
        for _ in 0..3 {
            SqliteQueue::open(&path).expect("reopening must stay clean");
        }
        let conn = Connection::open(&path).unwrap();
        let rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM meta WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "the version row is upserted, never duplicated");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn requeue_dead_makes_items_runnable_again_with_a_fresh_retry_budget() {
        // A requeued item that kept its exhausted `attempts` would be leased once and
        // dead-letter again immediately — a requeue that looks like it worked and
        // changes nothing.
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap();
        let leased = q.lease(3, Duration::from_secs(60)).await.unwrap();
        for it in &leased {
            q.dead_letter(&it.custom_id, it.attempts, "retries exhausted: HTTP 500")
                .await
                .unwrap();
        }
        assert_eq!(q.counts().await.unwrap().dead, 3);

        let moved = q
            .requeue_dead(&["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        assert_eq!(moved.len(), 2);
        let c = q.counts().await.unwrap();
        assert_eq!(c.pending, 2);
        assert_eq!(c.dead, 1, "the unlisted item stays quarantined");

        // The retry budget really is fresh, and the stale error is gone.
        let again = q.lease(2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(again.len(), 2);
        for it in &again {
            assert_eq!(it.attempts, 1, "first attempt of a new budget, not the 4th");
            assert!(it.last_error.is_none(), "the stale error must not survive");
        }
    }

    #[tokio::test]
    async fn requeue_dead_moves_only_dead_rows_and_is_idempotent() {
        // An operator's id list is often stale — copied from an older dead file, or a
        // superset. Resurrecting a `done` item would re-run work already emitted and
        // billed; disturbing a `leased` one would fight the coordinator.
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("done1"), item("dead1"), item("pending1")])
            .await
            .unwrap();
        let leased = q.lease(2, Duration::from_secs(60)).await.unwrap();
        for it in &leased {
            if it.custom_id == "done1" {
                q.ack(&it.custom_id, it.attempts).await.unwrap();
            } else {
                q.dead_letter(&it.custom_id, it.attempts, "boom")
                    .await
                    .unwrap();
            }
        }

        let moved = q
            .requeue_dead(&[
                "done1".to_string(),
                "dead1".to_string(),
                "pending1".to_string(),
                "ghost".to_string(),
            ])
            .await
            .unwrap();
        assert_eq!(moved, vec!["dead1".to_string()], "only the dead row moves");
        let c = q.counts().await.unwrap();
        assert_eq!(c.done, 1, "a done item is never resurrected");
        assert_eq!(c.dead, 0);
        assert_eq!(c.pending, 2, "pending1 untouched + dead1 requeued");

        // Idempotent.
        let twice = q.requeue_dead(&["dead1".to_string()]).await.unwrap();
        assert!(twice.is_empty(), "a second requeue transitions nothing");
    }

    #[tokio::test]
    async fn dead_letters_lists_ids_with_their_recorded_error() {
        let q = SqliteQueue::open_in_memory().unwrap();
        q.enqueue(&[item("x"), item("y")]).await.unwrap();
        let leased = q.lease(2, Duration::from_secs(60)).await.unwrap();
        for it in &leased {
            q.dead_letter(&it.custom_id, it.attempts, "retries exhausted: timeout")
                .await
                .unwrap();
        }
        let listed = q.dead_letters().await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed
            .iter()
            .all(|(_, e)| e.as_deref() == Some("retries exhausted: timeout")));
        let ids: Vec<&str> = listed.iter().map(|(i, _)| i.as_str()).collect();
        assert_eq!(ids, vec!["x", "y"]);
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
