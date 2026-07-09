//! Large-N scale benchmarks — `#[ignore]` so the default `cargo test` skips them. Run:
//!
//! ```sh
//! cargo test -p forge-cli --test scale -- --ignored --nocapture
//! # override the size (default 1,000,000):
//! FORGE_SCALE_N=5000000 cargo test -p forge-cli --test scale -- --ignored --nocapture
//! ```
//!
//! Two checks, both at a configurable N (default **1,000,000**, `FORGE_SCALE_N`):
//!
//! 1. `verify_completeness_at_scale_is_ram_bounded` — the completeness sweep is the
//!    load-bearing **bounded-RAM** claim (RAM independent of N). It runs the exact
//!    external sort-merge over N ids with a deliberately tiny run buffer, forcing many
//!    spills, and asserts resident memory stays under a tight ceiling — provably not
//!    holding the id set. This is the "memory stays bounded across the 50M run"
//!    acceptance, exercised at 1M.
//! 2. `fan_out_at_scale_completes` — a full end-to-end fan-out of N items across a fleet
//!    over an on-disk queue: proves the loop drives N items to completion (no OOM, no
//!    O(N²) stall, exactly-once effect) and reports throughput + peak RSS. It is a
//!    scale/soak test, not a strict O(1) claim: the SQLite queue's working set grows
//!    modestly with N, so the *strict* bounded-RAM guarantee belongs to check (1) and
//!    the object-store result path, not a 50M **local** run.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge_core::{
    BatchRun, EndpointKind, ForgeError, Item, ItemResponse, ItemResult, ItemState, Queue,
    ResultStore, RunConfig, TokenUsage, Worker, WorkerSpec,
};
use forge_queue::SqliteQueue;
use serde_json::json;

fn scale_n() -> usize {
    std::env::var("FORGE_SCALE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000)
}

/// Peak resident set size in MiB, from Linux `/proc/self/status` `VmHWM` (the process
/// high-water mark). `0.0` if unavailable (non-Linux) — assertions below tolerate it.
fn peak_rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmHWM:"))
                .and_then(|v| v.split_whitespace().next().map(str::to_string))
        })
        .and_then(|n| n.parse::<u64>().ok())
        .map(|kb| kb as f64 / 1024.0)
        .unwrap_or(0.0)
}

// ───────────────────────── (1) bounded-RAM completeness sweep ─────────────────────────

#[tokio::test]
#[ignore = "large-N benchmark; run with `--ignored --nocapture`"]
async fn verify_completeness_at_scale_is_ram_bounded() {
    let n = scale_n();
    let dir = std::env::temp_dir().join(format!("forge-scale-verify-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let input = dir.join("in.jsonl");
    let results = dir.join("out.jsonl");

    // Write N input ids and N-1 result ids (one deliberately missing) — streaming, so
    // generating the fixture is itself bounded.
    {
        use std::io::Write;
        let mut wi = std::io::BufWriter::new(std::fs::File::create(&input).unwrap());
        let mut wr = std::io::BufWriter::new(std::fs::File::create(&results).unwrap());
        for i in 0..n {
            writeln!(wi, "{{\"custom_id\":\"id{i:09}\"}}").unwrap();
            if i != n / 2 {
                writeln!(wr, "{{\"custom_id\":\"id{i:09}\"}}").unwrap();
            }
        }
        wi.flush().unwrap();
        wr.flush().unwrap();
    }

    // A tiny run buffer (vs the N ids) forces many sorted spills — the whole point: RAM
    // is O(run_capacity), not O(N).
    let cfg = forge_shard::VerifyConfig {
        run_capacity: 16_384,
        missing_out: None,
    };
    let started = Instant::now();
    let report = forge_shard::verify_completeness(&input, &results, None::<&std::path::Path>, &cfg)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(report.input_ids, n as u64);
    assert_eq!(report.missing, 1, "the one omitted id is caught");
    assert!(
        report.runs_spilled > 8,
        "the tiny buffer must spill many runs (got {})",
        report.runs_spilled
    );

    let peak = peak_rss_mb();
    eprintln!(
        "[scale] verify N={n} → {:.1}s, {} spilled runs; peak RSS {peak:.0} MiB",
        elapsed.as_secs_f64(),
        report.runs_spilled,
    );
    // Bounded: holding the N-id set would be tens of MiB extra at 1M and grow with N;
    // the external sort keeps only the 16k-id buffer, so RSS stays low regardless of N.
    assert!(
        peak == 0.0 || peak < 150.0,
        "completeness sweep RAM must stay bounded — peak {peak:.0} MiB at N={n}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────── (2) full fan-out scale/soak ─────────────────────────

/// An O(1)-memory result sink: counts puts, holds no per-item state.
#[derive(Default)]
struct CountingStore {
    done: Arc<AtomicUsize>,
}

impl ResultStore for CountingStore {
    async fn put(&self, _r: &ItemResult) -> Result<(), ForgeError> {
        self.done.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    async fn dead_letter(&self, _r: &ItemResult) -> Result<(), ForgeError> {
        Ok(())
    }
    async fn emitted_ids(&self) -> Result<HashSet<String>, ForgeError> {
        Ok(HashSet::new())
    }
}

/// A no-I/O worker that returns instantly — the loop, not an engine, is under test.
struct FastWorker {
    spec: WorkerSpec,
}

impl FastWorker {
    fn new(cap: usize) -> Self {
        Self {
            spec: WorkerSpec::new("fast", "http://unused", EndpointKind::Chat).concurrency(cap),
        }
    }
}

impl Worker for FastWorker {
    fn spec(&self) -> &WorkerSpec {
        &self.spec
    }
    fn is_ready(&self) -> bool {
        true
    }
    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
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
            worker_id: "fast".into(),
            latency_ms: 0,
            attempt: item.attempts,
            completed_at: 0,
        })
    }
}

fn item(i: usize) -> Item {
    Item {
        custom_id: format!("req-{i:08}"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "large-N benchmark; run with `--ignored --nocapture`"]
async fn fan_out_at_scale_completes() {
    const WORKERS: usize = 64;
    const CAP: usize = 16;
    const HYDRATE_CHUNK: usize = 10_000;
    let n = scale_n();

    let dir = std::env::temp_dir().join(format!("forge-scale-run-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("state.db");

    // Hydrate N items in bounded chunks — the input never materializes in RAM whole.
    let queue = SqliteQueue::open(&db).unwrap();
    let mut i = 0;
    while i < n {
        let end = (i + HYDRATE_CHUNK).min(n);
        let batch: Vec<Item> = (i..end).map(item).collect();
        queue.enqueue(&batch).await.unwrap();
        i = end;
    }

    let workers: Vec<FastWorker> = (0..WORKERS).map(|_| FastWorker::new(CAP)).collect();
    let store = CountingStore::default();
    let done = Arc::clone(&store.done);

    let cfg = RunConfig {
        lease_for: Duration::from_secs(300),
        lease_max: Duration::from_secs(300),
        lease_batch: 2_000,
        poll_interval: Duration::from_millis(5),
        ready_grace: Duration::from_secs(5),
        retry: Default::default(),
        load_aware: true,
    };

    let started = Instant::now();
    let totals = BatchRun::new(queue, workers, store)
        .with_config(cfg)
        .run()
        .await
        .unwrap();
    let elapsed = started.elapsed();

    // The real large-N guarantee: every item completes exactly once, no losses.
    assert_eq!(totals.items_done as usize, n, "every item completed");
    assert_eq!(totals.items_dead, 0, "no losses");
    assert_eq!(
        done.load(Ordering::Relaxed),
        n,
        "the store saw every result"
    );

    let peak = peak_rss_mb();
    eprintln!(
        "[scale] run N={n} workers={WORKERS} → {:.1}s ({:.0} items/s); peak RSS {peak:.0} MiB",
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64(),
    );
    // A runaway guard (not a strict O(1) claim — see the module docs): the SQLite
    // working set scales ~0.7 MiB per 1k items, so allow generous headroom above that
    // and catch only a true leak / hold-everything blow-up.
    let ceiling = 300.0 + (n as f64 / 1000.0) * 1.5;
    assert!(
        peak == 0.0 || peak < ceiling,
        "resident memory blew past the runaway guard — peak {peak:.0} MiB (ceiling {ceiling:.0}) at N={n}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
