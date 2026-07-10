//! Sliding-window dispatch: a slow worker must not gate the fleet (no head-of-line
//! blocking at batch boundaries). With the old wave-barrier dispatch (`lease batch →
//! dispatch all → await ALL → lease next`), round-robin gave the fast and the slow
//! worker exactly half the items each, and every wave stalled on the slow worker —
//! a mixed fleet ran *slower* than the fast worker alone. With the sliding window,
//! each completion immediately frees a slot that is topped up, so the fast worker
//! keeps eating items while the slow one chews its own.

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

/// A no-I/O worker with a fixed per-item service time and a processed counter,
/// enforcing its advertised cap with its own semaphore like the real `HttpWorker`.
struct TimedWorker {
    spec: WorkerSpec,
    delay: Duration,
    processed: Arc<AtomicUsize>,
    sem: Arc<Semaphore>,
}

impl TimedWorker {
    fn new(id: &str, limit: usize, delay: Duration) -> Self {
        Self {
            spec: WorkerSpec::new(id, "http://unused", EndpointKind::Chat).concurrency(limit),
            delay,
            processed: Arc::new(AtomicUsize::new(0)),
            sem: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl Worker for TimedWorker {
    fn spec(&self) -> &WorkerSpec {
        &self.spec
    }
    fn is_ready(&self) -> bool {
        true
    }
    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
        let _permit = self.sem.acquire().await.expect("semaphore open");
        tokio::time::sleep(self.delay).await;
        self.processed.fetch_add(1, Ordering::SeqCst);
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
            latency_ms: self.delay.as_millis() as u64,
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
async fn slow_worker_does_not_gate_the_fleet() {
    // Fast worker: ~instant. Slow worker: 150ms per item, same declared cap.
    let fast = TimedWorker::new("fast", 4, Duration::from_millis(1));
    let slow = TimedWorker::new("slow", 4, Duration::from_millis(150));
    let fast_count = Arc::clone(&fast.processed);
    let slow_count = Arc::clone(&slow.processed);

    let total = 60;
    let queue = SqliteQueue::open_in_memory().unwrap();
    let items: Vec<Item> = (0..total).map(item).collect();
    queue.enqueue(&items).await.unwrap();

    let out = std::env::temp_dir().join(format!("forge-pipe-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let store = JsonlStore::new(&out);

    let cfg = RunConfig {
        lease_for: Duration::from_secs(30),
        lease_max: Duration::from_secs(30),
        lease_batch: 64,
        poll_interval: Duration::from_millis(5),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy::default(),
        // No load metric on these doubles → exact round-robin assignment, the
        // worst case for head-of-line blocking.
        load_aware: true,
    };
    let totals = BatchRun::new(queue, vec![fast, slow], store)
        .with_config(cfg)
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done, total as u64, "every item terminal-done");
    let fast_n = fast_count.load(Ordering::SeqCst);
    let slow_n = slow_count.load(Ordering::SeqCst);
    assert_eq!(fast_n + slow_n, total, "no item lost or duplicated");

    // Wave-barrier dispatch gave the fast worker EXACTLY half (round-robin, then a
    // barrier per wave). The sliding window keeps refilling the fast worker while
    // the slow one holds only its own slots, so the fast worker must take the
    // overwhelming majority. 2/3 is far above the barrier's 1/2 yet far below the
    // window's typical share (~90%), so the assertion is stable, not timing-tight.
    assert!(
        fast_n * 3 >= total * 2,
        "fast worker took {fast_n}/{total} — head-of-line blocking is back"
    );
}
