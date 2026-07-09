# forge-worker — the HTTP Worker over a black-box inference engine

`forge-worker` is the **execution side** of the forge batch-inference stack. It drives
a black-box, OpenAI-compatible HTTP engine (vLLM / SGLang / llama.cpp) through the
`forge_core::Worker` trait: one persistent keep-alive `reqwest::Client` per `base_url`,
an adaptive in-flight limiter, and all retry/backoff owned here so the coordinator's
lease loop only ever sees terminal success or failure. TLS is `rustls`, so `https://`
engines work and the static-musl build stays pure-Rust.

## What it does

- **`HttpWorker`** — implements `Worker` (`submit`, `probe`, `spec`, `is_ready`,
  `load`). Built from a `WorkerSpec` via `HttpWorker::new`, with builder overrides
  `with_rate_limit`, `with_retry`, and `with_validation`.
- **AIMD adaptive in-flight limiter** — starts at the declared `concurrency_limit`
  ceiling, multiplicatively decreases on a transient overload signal (429 / 5xx /
  timeout) and additively increases back on a streak of successes, converging on the
  engine's real throughput without hand-tuning and never oversubscribing past the cap.
- **Backoff-with-full-jitter retry** — owned inside `submit`, which returns `Err` only
  once the `RetryPolicy` is exhausted. 429 / request-timeout / any 5xx are transient
  (retry); other 4xx are terminal.
- **Learned cooldown** — reads a response's own rate-limit signals (`Retry-After`, or
  `x-ratelimit-reset-*` when the paired `-remaining-*` hits 0), clamps to a 120s max,
  and arms a shared cooldown every `submit` waits out — so the whole fleet backs off in
  lockstep with what the endpoint told it, instead of each item re-discovering the limit.
- **Health probe + load hint** — `probe` does a health-`GET`, caches readiness, and
  best-effort refreshes the engine's queue depth for load-aware dispatch (`load`).
- **Optional content validation** — a `ResponseCheck` over a 2xx body; a failure is a
  *soft* failure that retries like a 5xx then dead-letters, so bad data is never silently
  emitted.

## Quickstart

```rust
use forge_worker::HttpWorker;
use forge_core::{Worker, WorkerSpec, RetryPolicy};

let worker = HttpWorker::new(WorkerSpec {
    base_url: "http://localhost:8000".into(),
    concurrency_limit: 64,
    ..Default::default()
})?
.with_retry(RetryPolicy::default());

if worker.probe().await {
    // `submit` handles concurrency, cooldown, and retry internally; it returns
    // `Err` only when the RetryPolicy is exhausted.
    let result = worker.submit(&item).await?;
}
```

The coordinator holds workers behind the `Worker` trait and pulls leased items to
`submit`; forge-worker is the piece that turns one `Item` into one `ItemResult`.

## Config

Cargo features:

| Feature | Effect |
|---------|--------|
| `governor` | Off by default (keeps `governor` out of the lean `forge-cli` musl binary). Enables an optional global GCRA request-rate ceiling (`with_rate_limit` / `WorkerSpec::rate_limit_per_second`), orthogonal to the AIMD concurrency limiter. |
