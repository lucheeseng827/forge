//! Pure-Rust `redb` lease queue (feature `redb`) — a zero-C alternative to
//! [`SqliteQueue`](crate::SqliteQueue) for the cleanest static cross-compile. Same
//! [`Queue`] contract and the same single-writer discipline (redb serializes write
//! transactions internally, so concurrent `ack`s queue behind one writer).
//!
//! Layout — each item is stored once, keyed by `custom_id`; small **index** tables
//! keep `lease`/`reap` off the full-job scan path:
//!
//! | table | key → value | role |
//! |---|---|---|
//! | `items`   | `custom_id → Item (JSON bytes)` | source of truth |
//! | `pending` | `seq (u64) → custom_id`         | FIFO leasable queue (ordered by insert seq) |
//! | `leased`  | `custom_id → leased_until (i64)`| in-flight index for the reaper (size ≈ in-flight width, not the job) |
//! | `counts`  | `status_code (u8) → count`      | maintained cardinalities (no scan for `counts`) |
//! | `meta`    | `"pending_seq"`/`"requeue_seq" → next seq` | FIFO generators: fresh enqueues use the high range, re-queued expired leases the low range (leased first) |
//!
//! `lease` resurfaces expired leases first (so its semantics match SQLite's
//! "pending OR expired" pick regardless of whether the caller reaped), then pops the
//! lowest-seq pending rows. All ops run on `spawn_blocking`, like the SQLite backend.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use forge_core::{now_ms, ForgeError, Item, ItemState, Queue, QueueCounts};
use redb::{Database, ReadableTable, TableDefinition};

const ITEMS: TableDefinition<&str, &[u8]> = TableDefinition::new("items");
const PENDING: TableDefinition<u64, &str> = TableDefinition::new("pending");
const LEASED: TableDefinition<&str, i64> = TableDefinition::new("leased");
const COUNTS: TableDefinition<u8, u64> = TableDefinition::new("counts");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const C_PENDING: u8 = 0;
const C_LEASED: u8 = 1;
const C_DONE: u8 = 2;
const C_DEAD: u8 = 3;

const SEQ_KEY: &str = "pending_seq";
const REQUEUE_SEQ_KEY: &str = "requeue_seq";

/// Fresh enqueues take `seq` from `[FRESH_BASE, u64::MAX)`; re-queued expired leases
/// take `seq` from `[0, FRESH_BASE)` (a separate counter). Since `lease` pops the
/// **lowest** seq first, an expired lease always resurfaces **before** any never-tried
/// pending item — matching SQLite's "resurface expired first" order and preventing
/// retry starvation under backlog. The `1 << 62` split leaves ~4.6e18 slots on each
/// side, far beyond any real job or retry count.
const FRESH_BASE: u64 = 1 << 62;

/// redb-backed durable queue. See module docs.
pub struct RedbQueue {
    db: Arc<Database>,
    /// Advisory `leased_by` tag so a checkpoint shows which process holds a lease.
    coordinator_id: String,
}

fn q(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Queue(e.to_string())
}

fn join_err(e: tokio::task::JoinError) -> ForgeError {
    ForgeError::Queue(format!("blocking task: {e}"))
}

fn status_code(s: ItemState) -> u8 {
    match s {
        ItemState::Pending => C_PENDING,
        ItemState::Leased => C_LEASED,
        ItemState::Done => C_DONE,
        ItemState::DeadLetter => C_DEAD,
    }
}

impl RedbQueue {
    /// Open (creating if needed) the redb checkpoint file at `path` and ensure every
    /// table exists (so later read transactions never hit a missing table).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ForgeError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let db = Database::create(path).map_err(q)?;
        // Create all tables up front in one write txn.
        let wtx = db.begin_write().map_err(q)?;
        {
            wtx.open_table(ITEMS).map_err(q)?;
            wtx.open_table(PENDING).map_err(q)?;
            wtx.open_table(LEASED).map_err(q)?;
            wtx.open_table(COUNTS).map_err(q)?;
            wtx.open_table(META).map_err(q)?;
        }
        wtx.commit().map_err(q)?;
        Ok(Self {
            db: Arc::new(db),
            coordinator_id: format!("forge-{}", std::process::id()),
        })
    }
}

/// Read-modify-write a counter by `delta`, clamped at 0.
fn adjust(counts: &mut redb::Table<u8, u64>, code: u8, delta: i64) -> Result<(), ForgeError> {
    let cur = counts.get(code).map_err(q)?.map(|g| g.value()).unwrap_or(0) as i64;
    let next = (cur + delta).max(0) as u64;
    counts.insert(code, next).map_err(q)?;
    Ok(())
}

fn encode(item: &Item) -> Result<Vec<u8>, ForgeError> {
    serde_json::to_vec(item).map_err(Into::into)
}

fn decode(bytes: &[u8]) -> Result<Item, ForgeError> {
    serde_json::from_slice(bytes).map_err(Into::into)
}

/// Move every expired lease (`leased_until < now`) back to `Pending`. Scans only the
/// `leased` index (in-flight width), never the whole job. Returns rows moved.
fn requeue_expired(wtx: &redb::WriteTransaction, now: i64) -> Result<u64, ForgeError> {
    let mut items = wtx.open_table(ITEMS).map_err(q)?;
    let mut pending = wtx.open_table(PENDING).map_err(q)?;
    let mut leased = wtx.open_table(LEASED).map_err(q)?;
    let mut counts = wtx.open_table(COUNTS).map_err(q)?;
    let mut meta = wtx.open_table(META).map_err(q)?;

    // Snapshot the expired custom_ids before mutating the index.
    let expired: Vec<String> = leased
        .iter()
        .map_err(q)?
        .filter_map(|r| match r {
            Ok((cid, until)) => (until.value() < now).then(|| Ok(cid.value().to_string())),
            Err(e) => Some(Err(q(e))),
        })
        .collect::<Result<_, _>>()?;

    // Re-queued expired leases take seqs from the LOW range (`[0, FRESH_BASE)`) via a
    // dedicated counter, so they sort ahead of every fresh (`>= FRESH_BASE`) pending
    // row and are leased first — the "expired first" order, not appended to the tail.
    let mut rseq = meta
        .get(REQUEUE_SEQ_KEY)
        .map_err(q)?
        .map(|g| g.value())
        .unwrap_or(0);
    let mut moved = 0u64;
    for cid in expired {
        let bytes = items
            .get(cid.as_str())
            .map_err(q)?
            .ok_or_else(|| q(format!("leased index points at missing item {cid}")))?
            .value()
            .to_vec();
        let mut item = decode(&bytes)?;
        item.status = ItemState::Pending;
        item.leased_until = None;
        item.leased_by = None;
        // attempts stays incremented — that is how a poison item exhausts retries.
        items
            .insert(cid.as_str(), encode(&item)?.as_slice())
            .map_err(q)?;
        leased.remove(cid.as_str()).map_err(q)?;
        pending.insert(rseq, cid.as_str()).map_err(q)?;
        rseq += 1;
        adjust(&mut counts, C_LEASED, -1)?;
        adjust(&mut counts, C_PENDING, 1)?;
        moved += 1;
    }
    meta.insert(REQUEUE_SEQ_KEY, rseq).map_err(q)?;
    Ok(moved)
}

fn enqueue_blocking(db: &Database, items_in: &[Item]) -> Result<u64, ForgeError> {
    let wtx = db.begin_write().map_err(q)?;
    let mut inserted = 0u64;
    {
        let mut items = wtx.open_table(ITEMS).map_err(q)?;
        let mut pending = wtx.open_table(PENDING).map_err(q)?;
        let mut counts = wtx.open_table(COUNTS).map_err(q)?;
        let mut meta = wtx.open_table(META).map_err(q)?;
        let mut seq = meta
            .get(SEQ_KEY)
            .map_err(q)?
            .map(|g| g.value())
            .unwrap_or(FRESH_BASE);
        for it in items_in {
            // Idempotent: re-ingest on resume is a no-op.
            if items.get(it.custom_id.as_str()).map_err(q)?.is_some() {
                continue;
            }
            let mut stored = it.clone();
            stored.status = ItemState::Pending;
            stored.attempts = 0;
            stored.leased_until = None;
            stored.leased_by = None;
            stored.last_error = None;
            items
                .insert(it.custom_id.as_str(), encode(&stored)?.as_slice())
                .map_err(q)?;
            pending.insert(seq, it.custom_id.as_str()).map_err(q)?;
            seq += 1;
            adjust(&mut counts, C_PENDING, 1)?;
            inserted += 1;
        }
        meta.insert(SEQ_KEY, seq).map_err(q)?;
    }
    wtx.commit().map_err(q)?;
    Ok(inserted)
}

fn lease_blocking(
    db: &Database,
    coordinator: &str,
    limit: usize,
    lease_for: Duration,
) -> Result<Vec<Item>, ForgeError> {
    let now = now_ms();
    let leased_until = now + lease_for.as_millis() as i64;
    let wtx = db.begin_write().map_err(q)?;
    let mut out = Vec::new();
    {
        // Match SQLite's "pending OR expired" pick: resurface expired leases first.
        requeue_expired(&wtx, now)?;

        let mut items = wtx.open_table(ITEMS).map_err(q)?;
        let mut pending = wtx.open_table(PENDING).map_err(q)?;
        let mut leased = wtx.open_table(LEASED).map_err(q)?;
        let mut counts = wtx.open_table(COUNTS).map_err(q)?;

        // Lowest-seq pending rows first (FIFO). Collect before mutating `pending`.
        let claim: Vec<(u64, String)> = pending
            .iter()
            .map_err(q)?
            .take(limit)
            .map(|r| {
                r.map(|(k, v)| (k.value(), v.value().to_string()))
                    .map_err(q)
            })
            .collect::<Result<_, _>>()?;

        for (seq, cid) in claim {
            pending.remove(seq).map_err(q)?;
            let bytes = items
                .get(cid.as_str())
                .map_err(q)?
                .ok_or_else(|| q(format!("pending index points at missing item {cid}")))?
                .value()
                .to_vec();
            let mut item = decode(&bytes)?;
            item.status = ItemState::Leased;
            item.attempts += 1;
            item.leased_until = Some(leased_until);
            item.leased_by = Some(coordinator.to_string());
            items
                .insert(cid.as_str(), encode(&item)?.as_slice())
                .map_err(q)?;
            leased.insert(cid.as_str(), leased_until).map_err(q)?;
            adjust(&mut counts, C_PENDING, -1)?;
            adjust(&mut counts, C_LEASED, 1)?;
            out.push(item);
        }
    }
    wtx.commit().map_err(q)?;
    Ok(out)
}

/// Shared body for `ack` / `dead_letter`: a lease-fenced `Leased → terminal`
/// transition. Returns whether the (still-current) lease was transitioned.
fn close_blocking(
    db: &Database,
    custom_id: &str,
    attempt: u32,
    to: ItemState,
    error: Option<&str>,
) -> Result<bool, ForgeError> {
    let wtx = db.begin_write().map_err(q)?;
    let changed;
    {
        let mut items = wtx.open_table(ITEMS).map_err(q)?;
        let mut leased = wtx.open_table(LEASED).map_err(q)?;
        let mut counts = wtx.open_table(COUNTS).map_err(q)?;

        let existing = items.get(custom_id).map_err(q)?.map(|g| g.value().to_vec());
        match existing {
            Some(bytes) => {
                let mut item = decode(&bytes)?;
                // Lease-fenced on the attempts generation, exactly like SQLite.
                if item.status == ItemState::Leased && item.attempts == attempt {
                    item.status = to;
                    item.leased_until = None;
                    item.leased_by = None;
                    if let Some(e) = error {
                        item.last_error = Some(e.to_string());
                    }
                    items
                        .insert(custom_id, encode(&item)?.as_slice())
                        .map_err(q)?;
                    leased.remove(custom_id).map_err(q)?;
                    adjust(&mut counts, C_LEASED, -1)?;
                    adjust(&mut counts, status_code(to), 1)?;
                    changed = true;
                } else {
                    changed = false;
                }
            }
            None => changed = false,
        }
    }
    wtx.commit().map_err(q)?;
    Ok(changed)
}

fn counts_blocking(db: &Database) -> Result<QueueCounts, ForgeError> {
    let rtx = db.begin_read().map_err(q)?;
    let counts = rtx.open_table(COUNTS).map_err(q)?;
    let get = |code: u8| -> Result<u64, ForgeError> {
        Ok(counts.get(code).map_err(q)?.map(|g| g.value()).unwrap_or(0))
    };
    Ok(QueueCounts {
        pending: get(C_PENDING)?,
        leased: get(C_LEASED)?,
        done: get(C_DONE)?,
        dead: get(C_DEAD)?,
    })
}

impl Queue for RedbQueue {
    async fn enqueue(&self, items: &[Item]) -> Result<u64, ForgeError> {
        if items.is_empty() {
            return Ok(0);
        }
        let db = Arc::clone(&self.db);
        let items = items.to_vec();
        tokio::task::spawn_blocking(move || enqueue_blocking(&db, &items))
            .await
            .map_err(join_err)?
    }

    async fn lease(&self, limit: usize, lease_for: Duration) -> Result<Vec<Item>, ForgeError> {
        let db = Arc::clone(&self.db);
        let coordinator = self.coordinator_id.clone();
        tokio::task::spawn_blocking(move || lease_blocking(&db, &coordinator, limit, lease_for))
            .await
            .map_err(join_err)?
    }

    async fn ack(&self, custom_id: &str, attempt: u32) -> Result<bool, ForgeError> {
        let db = Arc::clone(&self.db);
        let custom_id = custom_id.to_string();
        tokio::task::spawn_blocking(move || {
            close_blocking(&db, &custom_id, attempt, ItemState::Done, None)
        })
        .await
        .map_err(join_err)?
    }

    async fn reap(&self) -> Result<u64, ForgeError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let now = now_ms();
            let wtx = db.begin_write().map_err(q)?;
            let moved = requeue_expired(&wtx, now)?;
            wtx.commit().map_err(q)?;
            Ok(moved)
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
        let db = Arc::clone(&self.db);
        let custom_id = custom_id.to_string();
        let error = error.to_string();
        tokio::task::spawn_blocking(move || {
            close_blocking(
                &db,
                &custom_id,
                attempt,
                ItemState::DeadLetter,
                Some(&error),
            )
        })
        .await
        .map_err(join_err)?
    }

    async fn counts(&self) -> Result<QueueCounts, ForgeError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || counts_blocking(&db))
            .await
            .map_err(join_err)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TmpDb {
        path: std::path::PathBuf,
    }
    impl TmpDb {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("forge-redb-{}-{name}.redb", std::process::id()));
            let _ = std::fs::remove_file(&path);
            Self { path }
        }
        fn open(&self) -> RedbQueue {
            RedbQueue::open(&self.path).unwrap()
        }
    }
    impl Drop for TmpDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

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

    #[tokio::test]
    async fn enqueue_lease_ack_roundtrip() {
        let t = TmpDb::new("roundtrip");
        let q = t.open();
        assert_eq!(
            q.enqueue(&[item("a"), item("b"), item("c")]).await.unwrap(),
            3
        );
        assert_eq!(q.counts().await.unwrap().pending, 3);

        let leased = q.lease(2, Duration::from_secs(300)).await.unwrap();
        assert_eq!(leased.len(), 2);
        assert!(leased
            .iter()
            .all(|i| i.status == ItemState::Leased && i.attempts == 1));
        assert_eq!(leased[0].body["model"], "m"); // body round-trips
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
    async fn enqueue_is_idempotent() {
        let t = TmpDb::new("idem");
        let q = t.open();
        assert_eq!(q.enqueue(&[item("a"), item("b")]).await.unwrap(), 2);
        assert_eq!(q.enqueue(&[item("a"), item("b")]).await.unwrap(), 0);
        assert_eq!(q.counts().await.unwrap().total(), 2);
    }

    #[tokio::test]
    async fn ack_is_lease_fenced() {
        let t = TmpDb::new("fenced");
        let q = t.open();
        q.enqueue(&[item("a")]).await.unwrap();
        let leased = q.lease(1, Duration::from_secs(300)).await.unwrap();
        let gen = leased[0].attempts; // == 1

        assert!(!q.ack("a", gen + 1).await.unwrap()); // stale generation: no-op
        assert_eq!(q.counts().await.unwrap().leased, 1);
        assert!(q.ack("a", gen).await.unwrap());
        assert_eq!(q.counts().await.unwrap().done, 1);
    }

    #[tokio::test]
    async fn reaper_requeues_expired_leases() {
        let t = TmpDb::new("reap");
        let q = t.open();
        q.enqueue(&[item("a"), item("b")]).await.unwrap();
        let leased = q.lease(2, Duration::from_millis(0)).await.unwrap();
        assert_eq!(leased.len(), 2);
        tokio::time::sleep(Duration::from_millis(5)).await;

        assert_eq!(q.reap().await.unwrap(), 2);
        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased), (2, 0));
        // attempts persists across the reap → the re-lease bumps it again.
        let again = q.lease(2, Duration::from_secs(300)).await.unwrap();
        assert!(again.iter().all(|i| i.attempts == 2));
    }

    #[tokio::test]
    async fn dead_letter_quarantines() {
        let t = TmpDb::new("dead");
        let q = t.open();
        q.enqueue(&[item("poison")]).await.unwrap();
        let leased = q.lease(1, Duration::from_secs(300)).await.unwrap();
        assert!(q
            .dead_letter("poison", leased[0].attempts, "always 500")
            .await
            .unwrap());
        let c = q.counts().await.unwrap();
        assert_eq!((c.leased, c.dead), (0, 1));
    }

    #[tokio::test]
    async fn lease_resurfaces_expired_without_explicit_reap() {
        // lease() must pick up an expired lease on its own (semantics parity with
        // SQLite's "pending OR expired" pick), even if reap() was never called.
        let t = TmpDb::new("resurface");
        let q = t.open();
        q.enqueue(&[item("a")]).await.unwrap();
        q.lease(1, Duration::from_millis(0)).await.unwrap(); // leased, instantly expired
        tokio::time::sleep(Duration::from_millis(5)).await;
        let again = q.lease(1, Duration::from_secs(300)).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].attempts, 2);
    }

    #[tokio::test]
    async fn expired_leases_resurface_before_fresh_pending() {
        // Regression: an expired lease must be leased AHEAD of never-tried items
        // enqueued after it — not appended to the FIFO tail (retry starvation).
        let t = TmpDb::new("expired-first");
        let q = t.open();
        q.enqueue(&[item("a")]).await.unwrap();
        q.lease(1, Duration::from_millis(0)).await.unwrap(); // a: leased, instantly expired
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Fresh work arrives after a expired.
        q.enqueue(&[item("b"), item("c")]).await.unwrap();

        // The next single-item lease must return the expired `a`, not fresh `b`.
        let next = q.lease(1, Duration::from_secs(300)).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].custom_id, "a", "expired item must resurface first");
        assert_eq!(next[0].attempts, 2, "and keep its incremented attempts");
    }

    #[tokio::test]
    async fn reopen_recovers_state() {
        // The checkpoint IS the state: close the handle and reopen — counts persist.
        let t = TmpDb::new("reopen");
        {
            let q = t.open();
            q.enqueue(&[item("a"), item("b")]).await.unwrap();
            let leased = q.lease(1, Duration::from_secs(300)).await.unwrap();
            q.ack(&leased[0].custom_id, leased[0].attempts)
                .await
                .unwrap();
        }
        let q = t.open();
        let c = q.counts().await.unwrap();
        assert_eq!((c.pending, c.leased, c.done), (1, 0, 1));
    }
}
