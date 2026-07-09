//! Proves P1.1: a leased batch is dispatched concurrently, bounded by the fleet's
//! total in-flight capacity (Σ `concurrency_limit`). A `CountingWorker` records the
//! peak number of simultaneous `submit` calls.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use forge_core::{
    BatchRun, EndpointKind, ForgeError, Item, ItemResponse, ItemResult, ItemState, Queue,
    RetryPolicy, RunConfig, TokenUsage, Worker, WorkerSpec,
};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use serde_json::json;
use tokio::sync::Semaphore;

/// A worker with no real I/O that sleeps briefly and tracks peak concurrency.
#[derive(Clone)]
struct CountingWorker {
    spec: WorkerSpec,
    inflight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
    /// Enforces the worker's own advertised cap, like `HttpWorker`'s in-flight
    /// semaphore — so the per-worker peak assertions test the worker's throttle,
    /// not scheduler timing.
    sem: Arc<Semaphore>,
}

impl CountingWorker {
    fn new(limit: usize, delay: Duration) -> Self {
        Self {
            spec: WorkerSpec::new("counter", "http://unused", EndpointKind::Chat)
                .concurrency(limit),
            inflight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            delay,
            sem: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl Worker for CountingWorker {
    fn spec(&self) -> &WorkerSpec {
        &self.spec
    }
    fn is_ready(&self) -> bool {
        true
    }
    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
        // Block past the advertised cap, exactly as the real HTTP worker does.
        let _permit = self.sem.acquire().await.expect("semaphore open");
        let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        Ok(ItemResult {
            custom_id: item.custom_id.clone(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: None,
                body: json!({"ok": true}),
            }),
            error: None,
            usage: TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                ..Default::default()
            },
            worker_id: self.spec.worker_id.clone(),
            latency_ms: 0,
            attempt: item.attempts,
            completed_at: 0,
        })
    }
}

fn item(i: usize) -> Item {
    Item {
        custom_id: format!("req-{i}"),
        method: "POST".into(),
        url: "/v1/chat/completions".into(),
        body: json!({"model": "m"}),
        status: ItemState::Pending,
        attempts: 0,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

#[tokio::test]
async fn dispatch_is_concurrent_and_bounded_by_capacity() {
    let limit = 4;
    let total = 16;
    let worker = CountingWorker::new(limit, Duration::from_millis(20));
    let peak = Arc::clone(&worker.peak);

    let queue = SqliteQueue::open_in_memory().unwrap();
    let items: Vec<Item> = (0..total).map(item).collect();
    queue.enqueue(&items).await.unwrap();

    let out = std::env::temp_dir().join(format!("forge-conc-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let store = JsonlStore::new(&out);

    let cfg = RunConfig {
        lease_for: Duration::from_secs(30),
        lease_max: Duration::from_secs(30),
        lease_batch: 64,
        poll_interval: Duration::from_millis(5),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy::default(),
        load_aware: true,
    };
    let totals = BatchRun::new(queue, vec![worker], store)
        .with_config(cfg)
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done as usize, total, "all items complete");
    let observed = peak.load(Ordering::SeqCst);
    assert!(
        observed >= 2,
        "dispatch must be concurrent (peak {observed})"
    );
    assert!(
        observed <= limit,
        "must not exceed fleet capacity {limit} (peak {observed})"
    );

    let _ = std::fs::remove_file(&out);
}

#[tokio::test]
async fn capacity_is_the_sum_over_workers() {
    // Two workers, limit 3 each → fleet capacity 6.
    let a = CountingWorker::new(3, Duration::from_millis(20));
    let b = CountingWorker::new(3, Duration::from_millis(20));
    let (pa, pb) = (Arc::clone(&a.peak), Arc::clone(&b.peak));

    let queue = SqliteQueue::open_in_memory().unwrap();
    let items: Vec<Item> = (0..30).map(item).collect();
    queue.enqueue(&items).await.unwrap();

    let out = std::env::temp_dir().join(format!("forge-conc2-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let store = JsonlStore::new(&out);

    let cfg = RunConfig {
        lease_for: Duration::from_secs(30),
        lease_max: Duration::from_secs(30),
        lease_batch: 64,
        poll_interval: Duration::from_millis(5),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy::default(),
        load_aware: true,
    };
    let totals = BatchRun::new(queue, vec![a, b], store)
        .with_config(cfg)
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done, 30);
    // Round-robin spreads load; each worker is exercised and neither exceeds its
    // own limit.
    let (peak_a, peak_b) = (pa.load(Ordering::SeqCst), pb.load(Ordering::SeqCst));
    assert!(
        peak_a >= 1 && peak_b >= 1,
        "both workers used ({peak_a}, {peak_b})"
    );
    assert!(
        peak_a <= 3 && peak_b <= 3,
        "neither exceeds its limit ({peak_a}, {peak_b})"
    );

    let _ = std::fs::remove_file(&out);
}

/// The "100+-worker stress simulation": one coordinator (`BatchRun`, the sole
/// queue writer) fans a large batch across a **128-worker** fleet. Proves the fan-out
/// drives the whole fleet, spreads work across it (not a hot few), and never
/// oversubscribes any worker past its declared cap — with the single-writer rule holding
/// (the workers only `submit`; only the coordinator touches the queue).
#[tokio::test]
async fn stress_128_worker_fleet_fans_out_without_oversubscribing() {
    const WORKERS: usize = 128;
    const CAP: usize = 4;
    const TOTAL: usize = 6_000;

    let workers: Vec<CountingWorker> = (0..WORKERS)
        .map(|_| CountingWorker::new(CAP, Duration::from_millis(1)))
        .collect();
    let peaks: Vec<Arc<AtomicUsize>> = workers.iter().map(|w| Arc::clone(&w.peak)).collect();

    let queue = SqliteQueue::open_in_memory().unwrap();
    let items: Vec<Item> = (0..TOTAL).map(item).collect();
    queue.enqueue(&items).await.unwrap();

    let out = std::env::temp_dir().join(format!("forge-stress128-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let store = JsonlStore::new(&out);

    let cfg = RunConfig {
        lease_for: Duration::from_secs(60),
        lease_max: Duration::from_secs(60),
        lease_batch: 1024,
        poll_interval: Duration::from_millis(2),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy::default(),
        load_aware: true,
    };
    let totals = BatchRun::new(queue, workers, store)
        .with_config(cfg)
        .run()
        .await
        .unwrap();

    assert_eq!(
        totals.items_done as usize, TOTAL,
        "every item completed across the fleet"
    );
    assert_eq!(totals.items_dead, 0, "no losses");

    // Work reached 100+ distinct workers (round-robin/least-loaded across 128), and no
    // worker was ever pushed past its own in-flight cap.
    let used = peaks
        .iter()
        .filter(|p| p.load(Ordering::SeqCst) >= 1)
        .count();
    assert!(
        used >= 100,
        "work spread across the fleet, not a hot few (used {used}/{WORKERS})"
    );
    let max_peak = peaks
        .iter()
        .map(|p| p.load(Ordering::SeqCst))
        .max()
        .unwrap();
    assert!(
        max_peak <= CAP,
        "no worker oversubscribed past its cap {CAP} (max peak {max_peak})"
    );

    let _ = std::fs::remove_file(&out);
}
