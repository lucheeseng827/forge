//! The three seams the brain drives. Concrete backends (SQLite queue, HTTP
//! worker, JSONL store) live in sibling crates and implement these. `forge-core`
//! depends only on the traits, never the impls — so the CLI, the agent, and any
//! other embedder all reuse the same loop.

use std::collections::HashSet;
use std::time::Duration;

use crate::error::ForgeError;
use crate::types::{Item, ItemResult, Shard, WorkerSpec};

/// A snapshot of queue cardinalities, for `forge status` and the loop's
/// drain-complete check.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub pending: u64,
    pub leased: u64,
    pub done: u64,
    pub dead: u64,
}

impl QueueCounts {
    pub fn total(&self) -> u64 {
        self.pending + self.leased + self.done + self.dead
    }

    /// The run is drained when nothing is pending or in flight.
    pub fn is_drained(&self) -> bool {
        self.pending == 0 && self.leased == 0
    }
}

/// A durable, **single-writer** work queue with lease/visibility-timeout
/// semantics. The single-writer rule is what makes the lease transaction safe
/// without row locks — there is exactly one coordinator.
pub trait Queue {
    /// Idempotent hydrate: insert one `Pending` row per `custom_id`, skipping ids
    /// already present (so re-ingest on resume is a no-op). Returns rows inserted.
    async fn enqueue(&self, items: &[Item]) -> Result<u64, ForgeError>;

    /// Atomically lease up to `limit` `Pending` (or expired-`Leased`) items:
    /// set `status = Leased`, `leased_until = now + lease_for`, `attempts += 1`.
    async fn lease(&self, limit: usize, lease_for: Duration) -> Result<Vec<Item>, ForgeError>;

    /// `Leased → Done`. Called **only after** the result is durably in the store.
    ///
    /// **Lease-fenced:** `attempt` is the lease generation returned by [`lease`]
    /// (the post-increment `attempts`). The transition applies only while the row
    /// still carries that generation, so a stale worker whose lease expired and was
    /// re-leased to a newer attempt cannot close it. Returns `true` if the row was
    /// transitioned, `false` if the lease was stale (a harmless no-op — the store
    /// write already de-duplicated the result).
    async fn ack(&self, custom_id: &str, attempt: u32) -> Result<bool, ForgeError>;

    /// Reaper: `Leased → Pending` for every item whose `leased_until < now`.
    /// Returns the count re-queued. This is the entire spot-survival mechanism on
    /// the coordinator side — no heartbeat needed in the OSS core.
    async fn reap(&self) -> Result<u64, ForgeError>;

    /// `Leased → DeadLetter` once retries are exhausted, recording `error`.
    /// Lease-fenced on `attempt` exactly like [`ack`](Queue::ack); returns whether
    /// the (still-current) lease was transitioned.
    async fn dead_letter(
        &self,
        custom_id: &str,
        attempt: u32,
        error: &str,
    ) -> Result<bool, ForgeError>;

    /// Current cardinalities by state.
    async fn counts(&self) -> Result<QueueCounts, ForgeError>;

    /// Record an ingest checkpoint — a contiguous [`Shard`] of the input that has been
    /// durably hydrated. Called by `forge_shard::ingest_jsonl` **after** the batch's
    /// [`enqueue`](Queue::enqueue) commits, so a recorded shard is always ≤ what is
    /// actually hydrated. Default: a no-op (a backend without a seek index simply
    /// re-scans from the start on a re-ingest — correct, just not seek-optimized).
    async fn record_shard(&self, _shard: &Shard) -> Result<(), ForgeError> {
        Ok(())
    }

    /// The ingest high-water mark: `(byte_offset, line_no)` through which the input has
    /// been durably hydrated (the max over recorded [`Shard`]s), or `None` if nothing
    /// has been recorded. A re-ingest **seeks** to `byte_offset` instead of re-reading
    /// the hydrated prefix (`enqueue` is idempotent, so re-reading is only wasted work).
    /// Default `None` keeps every existing backend correct (a full re-scan).
    async fn hydrated_through(&self) -> Result<Option<(u64, u64)>, ForgeError> {
        Ok(None)
    }
}

/// One interchangeable, black-box, OpenAI-compatible engine endpoint. A `Worker`
/// answers *which* endpoint serves an identical request — never *how* a task runs
/// (that distinction is the dagron `Executor` boundary).
pub trait Worker {
    /// The declared capability of this endpoint.
    fn spec(&self) -> &WorkerSpec;

    /// Cached readiness, updated out-of-band by a health probe. Synchronous so the
    /// dispatch loop can pick a worker without awaiting.
    fn is_ready(&self) -> bool;

    /// Cached queue-depth signal for **load-aware dispatch** (ROADMAP B3): lower =
    /// less loaded, `None` = unknown. Synchronous and cached exactly like
    /// [`is_ready`](Worker::is_ready) (refreshed out-of-band by
    /// [`probe`](Worker::probe)), so the dispatch pick stays a plain index — no await
    /// inside the fan-out. This is pure **flow control**: the loop biases toward the
    /// least-loaded *interchangeable* worker; it is never a per-item routing decision
    /// (that would cross the dagron boundary). Default `None` keeps every existing
    /// impl source-compatible and degrades dispatch to round-robin.
    fn load(&self) -> Option<u32> {
        None
    }

    /// Refresh and return readiness (a health probe, for impls that have one). The
    /// run loop calls this at startup and whenever the whole fleet looks down.
    /// Default: a no-op that returns the current [`is_ready`](Worker::is_ready).
    async fn probe(&self) -> bool {
        self.is_ready()
    }

    /// Submit one item to the engine and return its result. The implementation
    /// owns the in-flight semaphore (= `concurrency_limit`) and the
    /// backoff-with-full-jitter retry; it returns `Err` only when retries are
    /// exhausted (→ the caller dead-letters the item).
    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError>;
}

/// The result sink, keyed by `custom_id`, order-independent. Writes must be
/// idempotent (a re-run after an expired lease writes the same id again).
pub trait ResultStore {
    /// Persist a successful result, idempotently keyed by `custom_id`. Returns
    /// after the write is durable — the loop acks the queue only once this
    /// resolves.
    async fn put(&self, result: &ItemResult) -> Result<(), ForgeError>;

    /// Persist a terminal failure to the dead-letter sink.
    async fn dead_letter(&self, result: &ItemResult) -> Result<(), ForgeError>;

    /// The set of `custom_id`s already emitted, for dedup-on-resume. (Local JSONL:
    /// scan the file; object store: a sidecar manifest so this stays O(emitted)
    /// rather than re-reading object storage.)
    async fn emitted_ids(&self) -> Result<HashSet<String>, ForgeError>;
}
