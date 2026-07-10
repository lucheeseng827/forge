//! [`BatchRun`] — the embeddable fan-out loop. The whole point of forge in one
//! type: lease independent items, dispatch each to a ready interchangeable worker,
//! write the result to the store **then** ack the queue, and let the reaper
//! re-queue anything a dead/spot-killed worker left leased. No task graph, no
//! sequencing, no successors — a homogeneous work-distribution loop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};

use crate::error::ForgeError;
use crate::retry::RetryPolicy;
use crate::traits::{Queue, ResultStore, Worker};
use crate::types::{Item, ItemResult, JobTotals, TokenUsage};

/// Multiplier applied to the peak observed per-item latency when growing the lease
/// TTL — enough headroom that a slow-but-healthy generation isn't re-queued under it.
const LEASE_LATENCY_FACTOR: u64 = 2;

/// The effective lease TTL for the next round. Grows the configured `base` toward
/// `max` once observed latency suggests the fixed TTL would expire while a slow
/// generation is still in flight (which would wastefully re-dispatch a healthy
/// item). `peak_ms` is the longest a *successful* item has taken so far — 0 before
/// any completes, so the first rounds just use `base`. Tolerates a `max < base`
/// misconfiguration by treating `base` as the floor.
fn adaptive_lease(base: Duration, max: Duration, peak_ms: u64) -> Duration {
    let ceiling = max.max(base);
    let grown = Duration::from_millis(peak_ms.saturating_mul(LEASE_LATENCY_FACTOR));
    grown.clamp(base, ceiling)
}

/// What a single item's dispatch resolved to (folded into [`JobTotals`]).
enum Dispatched {
    Done(TokenUsage),
    Dead,
}

/// Tunables for a run. Defaults are conservative; the CLI overrides from flags.
#[derive(Debug, Clone, Copy)]
pub struct RunConfig {
    /// Visibility timeout floor: the lease TTL the reaper uses before any latency is
    /// observed. The effective TTL grows from here toward [`lease_max`](Self::lease_max)
    /// as the run learns how long items actually take, so a slow generation isn't
    /// re-queued mid-flight.
    pub lease_for: Duration,
    /// Cap on the (adaptive) lease TTL. Bounds how long a genuinely dead worker's
    /// items stay leased before the reaper reclaims them.
    pub lease_max: Duration,
    /// How many items to lease per round trip to the queue.
    pub lease_batch: usize,
    /// How long to wait before re-polling when nothing is leasable yet.
    pub poll_interval: Duration,
    /// How long to wait for at least one worker to become ready before giving up
    /// (so a transient fleet outage is survived, but a misconfigured endpoint
    /// errors out instead of hanging forever).
    pub ready_grace: Duration,
    /// Advisory retry envelope. The **worker** owns the actual retry loop
    /// ([`Worker::submit`](crate::Worker::submit) returns `Err` only once its own
    /// `RetryPolicy` is exhausted), so set the policy on each worker (e.g.
    /// `HttpWorker::with_retry`) to change behavior — the dispatch loop does not
    /// read this field. Kept for callers that thread one policy through both.
    pub retry: RetryPolicy,
    /// Bias dispatch toward the least-loaded ready worker using each engine's own
    /// queue-depth metric ([`Worker::load`](crate::Worker::load)), instead of flat
    /// round-robin (ROADMAP B3). Default `true`; it is a no-op when no worker exposes
    /// a load (every `load()` is `None` → exact round-robin), so it's safe on by
    /// default. Set `false` to force round-robin regardless.
    pub load_aware: bool,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            lease_for: Duration::from_secs(300),
            lease_max: Duration::from_secs(1800),
            lease_batch: 256,
            poll_interval: Duration::from_millis(250),
            ready_grace: Duration::from_secs(60),
            retry: RetryPolicy::default(),
            load_aware: true,
        }
    }
}

/// Compute the per-item worker assignment for one dispatched batch — the **B3
/// load-aware** pick, factored out as a pure function so it's deterministic and
/// unit-testable without real workers.
///
/// `loads[w]`/`caps[w]` are the cached load (lower = less loaded, `None` = unknown)
/// and in-flight capacity of ready worker `w`; `base` rotates round-robin across
/// rounds. Returns `assign` of length `n_items` where `assign[i]` is the index into
/// the ready slice for item `i`.
///
/// - **Fallback (no-op guarantee):** if `load_aware` is off *or* every worker's load
///   is unknown, this is exactly the prior flat round-robin `(base + i) % n` — so B3
///   changes nothing on a fleet that exposes no metric.
/// - **Biased:** otherwise fill workers **least-loaded first, up to each worker's
///   capacity** (unknown-load workers sort last). Each worker's own AIMD semaphore is
///   still the hard in-flight cap, so this only decides *order/share*, never
///   oversubscribes — and a small batch lands entirely on the idlest engine(s).
fn dispatch_assignment(
    loads: &[Option<u32>],
    caps: &[usize],
    base: usize,
    n_items: usize,
    load_aware: bool,
) -> Vec<usize> {
    let n = loads.len();
    debug_assert!(n >= 1 && caps.len() == n);

    let any_known = load_aware && loads.iter().any(Option::is_some);
    if !any_known {
        // No load signal → rotation-fair round-robin, but **capacity-bounded**: a
        // worker whose `caps[w]` (its currently-free slots, when the caller passes
        // free slots rather than full capacity) is exhausted is skipped, so a busy
        // worker never accumulates a backlog that head-of-line-blocks the window.
        // If the caller leases more than the total capacity (legacy callers passing
        // full caps), fall back to the exact flat `(base + i) % n` rotation.
        let total: usize = caps.iter().sum();
        if n_items > total {
            return (0..n_items).map(|i| (base + i) % n).collect();
        }
        let mut free = caps.to_vec();
        let mut assign = Vec::with_capacity(n_items);
        let mut cursor = base;
        while assign.len() < n_items {
            // Next worker in rotation with a free slot (total ≥ n_items guarantees one).
            let mut w = cursor % n;
            while free[w] == 0 {
                w = (w + 1) % n;
            }
            free[w] -= 1;
            assign.push(w);
            cursor = w + 1;
        }
        return assign;
    }

    // Order ready workers by (load asc, round-robin-rotated tiebreak) so equal-load
    // workers still rotate fairly across rounds. Unknown load sorts last (u32::MAX).
    let rot = base % n;
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&w| (loads[w].unwrap_or(u32::MAX), (w + n - rot) % n));

    // Fill least-loaded first, bounded by each worker's capacity. A worker with no
    // capacity left (the caller passes FREE slots) is skipped outright — assigning
    // it anyway would queue the item behind its semaphore and reintroduce
    // head-of-line blocking inside the window.
    let mut assign = Vec::with_capacity(n_items);
    for &w in &order {
        if assign.len() == n_items {
            break;
        }
        let take = caps[w].min(n_items - assign.len());
        for _ in 0..take {
            assign.push(w);
        }
    }
    // Guard: if total capacity somehow underflows the batch (the caller leases
    // ≤ capacity, so this is dead-code insurance), round-robin the remainder.
    while assign.len() < n_items {
        assign.push(order[assign.len() % n]);
    }
    assign
}

/// Drives a queue, a set of interchangeable workers, and a result store to
/// completion. Generic over the three traits so the same loop runs over SQLite +
/// HTTP + JSONL today and other backends later, with zero `dyn` dispatch.
pub struct BatchRun<Q, W, S> {
    queue: Q,
    workers: Vec<W>,
    store: S,
    cfg: RunConfig,
    /// Longest a *successful* item has taken (ms) — drives the adaptive lease TTL.
    /// Monotonic (conservative: errs toward a longer lease, never a premature
    /// re-queue), bounded by `cfg.lease_max`.
    peak_latency_ms: AtomicU64,
}

impl<Q, W, S> BatchRun<Q, W, S>
where
    Q: Queue,
    W: Worker,
    S: ResultStore,
{
    /// Construct a run. Hydrate the queue first (e.g. via `forge_shard::ingest_jsonl`);
    /// `run` drains whatever is already enqueued.
    pub fn new(queue: Q, workers: Vec<W>, store: S) -> Self {
        Self {
            queue,
            workers,
            store,
            cfg: RunConfig::default(),
            peak_latency_ms: AtomicU64::new(0),
        }
    }

    /// Override the run configuration.
    pub fn with_config(mut self, cfg: RunConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// Borrow the queue (e.g. for a `status`/`sweep` command sharing the handle).
    pub fn queue(&self) -> &Q {
        &self.queue
    }

    /// Whether any worker is currently marked ready.
    fn any_ready(&self) -> bool {
        self.workers.iter().any(|w| w.is_ready())
    }

    /// Probe every worker (best-effort) to refresh cached readiness.
    async fn probe_all(&self) {
        for worker in &self.workers {
            let _ = worker.probe().await;
        }
    }

    /// Submit one item, checkpoint its result, and ack/dead-letter it. The unit of
    /// concurrency — many of these run at once, throttled by each worker's own
    /// in-flight semaphore. Returns `Err` only on a queue/store failure (a fatal,
    /// stop-the-run condition); a worker failure is handled inline as a dead-letter.
    async fn dispatch_one(&self, worker: &W, item: &Item) -> Result<Dispatched, ForgeError> {
        // Time the WHOLE submit (retries + backoff included), not just a successful
        // result's reported latency. `Worker::submit` owns the retry loop and only
        // returns `Err` after exhaustion, so a slow retry storm or a slow terminal
        // failure must still raise the observed peak — otherwise `lease_for` stays too
        // short, the reaper reclaims a still-running item, and the coordinator
        // dispatches a duplicate. Fed on both branches.
        let started = Instant::now();
        let outcome = worker.submit(item).await;
        self.peak_latency_ms
            .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        match outcome {
            Ok(result) => {
                // Checkpoint BEFORE ack — the load-bearing exactly-once-effect ordering.
                self.store.put(&result).await?;
                // Lease-fenced by the item's lease generation. A `false` here means
                // our lease went stale (expired + re-leased); the store write already
                // de-duped, so it's a harmless no-op.
                if !self.queue.ack(&item.custom_id, item.attempts).await? {
                    tracing::debug!(custom_id = %item.custom_id, "ack skipped — lease was stale");
                }
                Ok(Dispatched::Done(result.usage))
            }
            Err(err) => {
                // worker.submit only returns Err once its RetryPolicy is exhausted
                // → this item is poison; quarantine it. (Its latency already fed the
                // adaptive lease above, before this match.)
                tracing::warn!(custom_id = %item.custom_id, error = %err, "dead-lettering item");
                let dead = ItemResult::failed(item, worker.spec().worker_id.clone(), &err);
                self.store.dead_letter(&dead).await?;
                self.queue
                    .dead_letter(&item.custom_id, item.attempts, &err.to_string())
                    .await?;
                Ok(Dispatched::Dead)
            }
        }
    }

    /// Drain the queue to terminal states and return aggregate totals.
    ///
    /// Dispatch is a **sliding window**, not batched waves: up to the fleet's total
    /// capacity (Σ `concurrency_limit` over ready workers) items are in flight at
    /// once, and every completion immediately frees a slot that is topped up with a
    /// fresh lease — so a slow worker only ever occupies *its own* slots and can
    /// never gate the rest of the fleet (no head-of-line blocking at batch
    /// boundaries on heterogeneous fleets). Each worker's own in-flight semaphore
    /// remains the hard throttle, so no engine is oversubscribed.
    ///
    /// Loop invariant — the exactly-once-*effect* ordering: each result is written
    /// to the store **before** its item is acked. A crash in between leaves the
    /// item `Leased`; its lease expires; the reaper re-queues it; the re-run's
    /// idempotent store write is a harmless no-op.
    pub async fn run(&self) -> Result<JobTotals, ForgeError> {
        if self.workers.is_empty() {
            return Err(ForgeError::Config("no workers configured".into()));
        }
        // Refresh readiness once before we start handing out leases.
        self.probe_all().await;

        let mut totals = JobTotals::default();
        let mut rr: usize = 0;
        // When the fleet first went unready; we give up once `ready_grace` of
        // wall-clock has actually elapsed (not a rounded count of poll cycles).
        let mut idle_since: Option<Instant> = None;
        // The sliding window of in-flight dispatches. Futures borrow `&self`, so
        // there is still no spawn / Arc / Send requirement. Each future resolves to
        // `(worker index, outcome)` so the per-worker in-flight ledger below stays
        // exact as completions land.
        let mut in_flight = FuturesUnordered::new();
        // Per-worker in-flight count (global index into `self.workers`). New leases
        // are assigned only against a worker's FREE slots — this is what prevents a
        // slow worker's backlog from silting up the window and re-creating
        // head-of-line blocking one layer down (at its semaphore).
        let mut wf: Vec<usize> = vec![0; self.workers.len()];
        // Reap on a wall-clock cadence instead of once per wave — topping up happens
        // on every completion, and a queue round-trip per completion is fine, but a
        // reap sweep per completion is not.
        let mut last_reap: Option<Instant> = None;

        loop {
            // Re-queue anything a dead worker left leased, on a poll-interval cadence
            // (and always on the first pass / when the window has fully drained).
            if last_reap.map_or(true, |t| t.elapsed() >= self.cfg.poll_interval)
                || in_flight.is_empty()
            {
                let reaped = self.queue.reap().await?;
                if reaped > 0 {
                    tracing::debug!(reaped, "re-queued expired leases");
                }
                last_reap = Some(Instant::now());
            }

            // Don't lease work no worker can serve; re-probe and wait for the fleet.
            if !self.any_ready() {
                self.probe_all().await;
                if !self.any_ready() {
                    // Let anything already in flight land first (its worker accepted
                    // it before going unready; readiness only gates NEW leases).
                    if let Some((widx, outcome)) = in_flight.next().await {
                        wf[widx] -= 1;
                        match outcome? {
                            Dispatched::Done(usage) => totals.record_done(&usage),
                            Dispatched::Dead => totals.items_dead += 1,
                        }
                        continue;
                    }
                    if self.queue.counts().await?.is_drained() {
                        break;
                    }
                    let waited = idle_since.get_or_insert_with(Instant::now).elapsed();
                    if waited >= self.cfg.ready_grace {
                        return Err(ForgeError::Worker(format!(
                            "no worker became ready within {:?}; check --workers / engine health",
                            self.cfg.ready_grace
                        )));
                    }
                    tracing::warn!("no ready worker; waiting for the fleet");
                    tokio::time::sleep(self.cfg.poll_interval).await;
                    continue;
                }
            }
            idle_since = None; // a worker is ready

            // The ready fleet (by global index) and each member's FREE slots right
            // now — declared capacity minus what it already has in flight.
            let ready: Vec<usize> = self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, w)| w.is_ready())
                .map(|(g, _)| g)
                .collect();
            let free: Vec<usize> = ready
                .iter()
                .map(|&g| {
                    self.workers[g]
                        .spec()
                        .concurrency_limit
                        .max(1)
                        .saturating_sub(wf[g])
                })
                .collect();

            // Top the window up. Lease only what has a free slot to start on RIGHT
            // NOW, so no leased item sits idle (with its lease clock running) behind
            // a busy worker's semaphore. `lease_batch` is the per-round-trip ceiling.
            let deficit = free.iter().sum::<usize>().min(self.cfg.lease_batch);
            if deficit > 0 {
                // Adaptive TTL: grow with observed latency so a slow generation isn't
                // re-queued mid-flight, capped at `lease_max`.
                let lease_for = adaptive_lease(
                    self.cfg.lease_for,
                    self.cfg.lease_max,
                    self.peak_latency_ms.load(Ordering::Relaxed),
                );
                if lease_for > self.cfg.lease_for {
                    tracing::debug!(?lease_for, "lease TTL grown to cover observed latency");
                }
                let batch = self.queue.lease(deficit, lease_for).await?;

                if batch.is_empty() && in_flight.is_empty() {
                    if self.queue.counts().await?.is_drained() {
                        break;
                    }
                    // Work is leased elsewhere (e.g. an expired-but-unreaped lease);
                    // wait, then reap + re-poll.
                    tokio::time::sleep(self.cfg.poll_interval).await;
                    continue;
                }

                // The per-item worker pick is the **load-aware** assignment (B3) — a
                // synchronous pre-computed index over each ready worker's FREE slots
                // (never its full capacity), biased toward the least-loaded engine
                // when metrics are available and capacity-bounded round-robin
                // otherwise — so a saturated worker is skipped, not queued behind.
                let loads: Vec<Option<u32>> =
                    ready.iter().map(|&g| self.workers[g].load()).collect();
                let assign =
                    dispatch_assignment(&loads, &free, rr, batch.len(), self.cfg.load_aware);
                rr = rr.wrapping_add(batch.len());
                for (i, item) in batch.into_iter().enumerate() {
                    let g = ready[assign[i]];
                    let worker = &self.workers[g];
                    wf[g] += 1;
                    in_flight.push(async move { (g, self.dispatch_one(worker, &item).await) });
                }
            }

            // Consume exactly ONE completion, then loop to top the freed slot back up
            // — this is what makes the window slide instead of draining in waves.
            if let Some((widx, outcome)) = in_flight.next().await {
                wf[widx] -= 1;
                match outcome? {
                    Dispatched::Done(usage) => totals.record_done(&usage),
                    Dispatched::Dead => totals.items_dead += 1,
                }
            }
        }

        // Authoritative final tally from the queue (covers prior-run progress on a
        // resume); token totals are this run's accumulation.
        let counts = self.queue.counts().await?;
        totals.items_total = counts.total();
        totals.items_done = counts.done;
        totals.items_dead = counts.dead;
        Ok(totals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_lease_grows_with_latency_and_caps() {
        let base = Duration::from_secs(300);
        let max = Duration::from_secs(1800);

        // No data yet → the base floor.
        assert_eq!(adaptive_lease(base, max, 0), base);
        // A fast item (100ms × 2 = 200ms) stays at the base.
        assert_eq!(adaptive_lease(base, max, 100), base);
        // A 250s item → 500s lease (2×), between base and cap.
        assert_eq!(adaptive_lease(base, max, 250_000), Duration::from_secs(500));
        // A very slow item is capped at lease_max, not 2× unbounded.
        assert_eq!(adaptive_lease(base, max, 1_000_000), max);
    }

    #[test]
    fn adaptive_lease_tolerates_max_below_base() {
        // Misconfigured lease_max < lease_for: the base is still honored as the floor.
        let base = Duration::from_secs(300);
        let max = Duration::from_secs(100);
        assert_eq!(adaptive_lease(base, max, 0), base);
        assert_eq!(adaptive_lease(base, max, 10_000_000), base);
    }

    // ── B3 load-aware dispatch ──

    #[test]
    fn all_unknown_load_falls_back_to_round_robin() {
        // The no-op regression guard: with no engine exposing a metric, the
        // assignment is byte-for-byte the prior flat round-robin.
        let loads = [None, None, None];
        let caps = [4, 4, 4];
        for base in 0..6 {
            let got = dispatch_assignment(&loads, &caps, base, 5, true);
            let want: Vec<usize> = (0..5).map(|i| (base + i) % 3).collect();
            assert_eq!(got, want, "base={base}");
        }
    }

    #[test]
    fn load_aware_off_is_round_robin_even_with_known_loads() {
        // Explicit opt-out forces round-robin regardless of known loads.
        let loads = [Some(0), Some(99)];
        let caps = [8, 8];
        let got = dispatch_assignment(&loads, &caps, 0, 4, false);
        assert_eq!(got, vec![0, 1, 0, 1]);
    }

    #[test]
    fn load_biased_fills_least_loaded_first() {
        // loads [10, 0, unknown], caps 4 each, a 4-item batch → all 4 land on the
        // idle (load 0) worker; the busy and the unknown get nothing this round.
        let loads = [Some(10), Some(0), None];
        let caps = [4, 4, 4];
        let got = dispatch_assignment(&loads, &caps, 0, 4, true);
        assert_eq!(got, vec![1, 1, 1, 1]);
    }

    #[test]
    fn load_biased_respects_capacity_then_spills_to_next() {
        // Idle worker (load 0) cap 2, busier worker (load 5) cap 3: a 4-item batch
        // fills the idle one to its cap (2), then spills the rest to the next.
        let loads = [Some(5), Some(0)];
        let caps = [3, 2];
        let got = dispatch_assignment(&loads, &caps, 0, 4, true);
        assert_eq!(got, vec![1, 1, 0, 0]); // worker1 (load0) ×2, then worker0 ×2
    }

    #[test]
    fn unknown_load_sorts_after_known_idle() {
        // A mix: a known-idle worker is preferred over an unknown-load one.
        let loads = [None, Some(0)];
        let caps = [4, 4];
        let got = dispatch_assignment(&loads, &caps, 0, 3, true);
        assert_eq!(got, vec![1, 1, 1]); // all to the known-idle worker
    }

    #[test]
    fn equal_loads_rotate_with_base_for_fairness() {
        // Two equally-loaded workers, one item each round: the tiebreak rotates with
        // `base` so neither is favored across rounds.
        let loads = [Some(2), Some(2)];
        let caps = [1, 1];
        assert_eq!(dispatch_assignment(&loads, &caps, 0, 1, true), vec![0]);
        assert_eq!(dispatch_assignment(&loads, &caps, 1, 1, true), vec![1]);
    }

    #[test]
    fn dispatch_scales_to_a_150_worker_fleet() {
        // The scheduling half of the 100+-worker stress: the pure assignment stays
        // correct at fleet scale — strict least-loaded-first, capacity-bounded, and an
        // exact round-robin fallback.
        const N: usize = 150;
        // Distinct loads (worker i has load i) so the ordering is unambiguous.
        let loads: Vec<Option<u32>> = (0..N).map(|i| Some(i as u32)).collect();

        // cap 1 each, 100-item batch → the 100 idlest workers, each once, in load order.
        let caps1 = vec![1usize; N];
        let got = dispatch_assignment(&loads, &caps1, 0, 100, true);
        assert_eq!(
            got,
            (0..100).collect::<Vec<_>>(),
            "fills the 100 idlest, cap 1"
        );

        // Round-robin fallback at scale is still byte-for-byte `(base + i) % n`.
        let rr = dispatch_assignment(&loads, &caps1, 7, 300, false);
        let want_rr: Vec<usize> = (0..300).map(|i| (7 + i) % N).collect();
        assert_eq!(rr, want_rr, "round-robin fallback exact at 150 workers");

        // cap 4 each, 500-item batch → the 125 idlest fill to cap (125 × 4 = 500), the
        // busiest 25 get nothing, and no worker is ever oversubscribed.
        let caps4 = vec![4usize; N];
        let assign = dispatch_assignment(&loads, &caps4, 3, 500, true);
        assert_eq!(assign.len(), 500);
        let mut counts = vec![0usize; N];
        for &w in &assign {
            counts[w] += 1;
        }
        assert!(
            counts.iter().all(|&c| c <= 4),
            "no worker over its cap of 4"
        );
        assert!(
            counts[..125].iter().all(|&c| c == 4),
            "the 125 idlest each filled to cap"
        );
        assert!(
            counts[125..].iter().all(|&c| c == 0),
            "the busiest 25 got nothing this round"
        );
    }
}
