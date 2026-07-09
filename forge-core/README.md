# forge-core — the brain of the batch-inference coordinator

`forge-core` owns forge's domain model and its execution loop, and **nothing that
does I/O**. forge is a *single homogeneous fan-out of independent items across
interchangeable workers* — not a task graph. That boundary is enforced in the type
system: there is no `depends_on`, no successor edge, no fan-in, no topological sort
anywhere in this crate (arbitrary DAGs are [dagron](https://github.com/lucheeseng827/dagron)'s job). The
concrete SQLite queue, HTTP worker, and JSONL store live in sibling crates and are
driven **only** through the traits defined here.

## What it does

- **Domain types** — `Item`, `ItemResult` (`ItemResponse` / `ItemError`),
  `WorkerSpec`, `Shard`, `TokenUsage`, `EndpointKind`, `EngineHint`, and the
  `Job` / `JobStatus` / `JobTotals` run metadata.
- **Item state machine** — `ItemState`: `Pending → Leased → Done | DeadLetter`,
  with legal-transition checks and **zero inter-item edges**.
- **The three seams** — the `Queue`, `Worker`, and `ResultStore` traits (plus
  `QueueCounts`), the only surface `forge-core` depends on for I/O.
- **The fan-out loop** — `BatchRun` + `RunConfig`: the embeddable engine that
  leases from a `Queue`, dispatches across a `Vec<impl Worker>` (round-robin, with
  optional load-aware bias), writes to a `ResultStore`, then acks — lease-fenced,
  crash-resumable.
- **Retry / failure policy** — `RetryPolicy` (backoff) and `classify_failure` /
  `FailureKind` for the dead-letter reason breakdown.
- **Cost arbitrage** — `compute_cost`, `CostInputs`, `CostReport`, `UsageTotals`.
- **Response validation** — `ResponseCheck` (accept any / non-empty / valid JSON).
- `ForgeError` (the crate's error enum) and `now_ms()` (epoch-ms clock for leases).

## Quickstart

`forge-core` is a library; you drive it through the three traits. The loop is
generic over concrete backends, so the CLI, the agent, and any other embedder
all reuse it:

```rust
use forge_core::{BatchRun, RunConfig};

// queue:   impl forge_core::Queue        (e.g. forge_queue::SqliteQueue)
// workers: Vec<impl forge_core::Worker>  (e.g. forge_worker::HttpWorker)
// store:   impl forge_core::ResultStore  (e.g. forge_store::JsonlStore)
let totals = BatchRun::new(queue, workers, store)
    .with_config(RunConfig::default())
    .run()
    .await?;

println!("done={} dead={}", totals.items_done, totals.items_dead);
```

The public async traits are used only through generic bounds (never `dyn`), so
`async fn` in trait is used deliberately (`#![allow(async_fn_in_trait)]`).
