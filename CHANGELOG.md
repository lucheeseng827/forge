# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

## [0.1.1] — 2026-07-10

### Changed
- **Sliding-window dispatch** replaces wave-based batching: completions
  immediately free slots that are topped up against workers' FREE capacity, so
  a slow worker can no longer head-of-line-block a mixed fleet (measured: a
  2-fast + 1-weak fleet went from *slower than one fast box* to 2.8× faster;
  single-box throughput +20% to ~97% of the engine ceiling).
- `forge resume` now reuses the per-worker `--concurrency` the original `run`
  recorded in the checkpoint (explicit flag still wins, warned on divergence)
  — a bare resume can no longer restart AIMD at a ceiling the fleet was not
  configured for.

### Added
- **Kubernetes**: repo-root `Dockerfile` (FROM-scratch, ~5 MB, nonroot) and the
  `deploy/helm/forge` chart — `serve-batch` as a Deployment (PVC, probes,
  Secret-backed bearer key) and one-shot `forge run` as a Job whose PVC-backed
  checkpoint turns evictions into resumes. Official multi-arch images at
  `docker.io/mancube/forge`; the chart ships as an OCI artifact at
  `oci://registry-1.docker.io/mancube/forge-chart`.
- **BENCHMARKS**: fleet-scheduling and overload-safety sections, plus an
  isolated EC2 run (cross-node fleet ~90% of ceiling; coordinator at ~10 MB
  RSS / <1% CPU); `examples/engine_sim.py` ships the reproducible engine sim.

## [0.1.0] — first public release

The batch-inference coordinator, end to end: fan a JSONL of independent
requests across N OpenAI-compatible endpoints, survive kills and spot
interruptions, and prove completeness — as a single static binary.

### Core

- **Fan-out engine** (`forge-core`): `Pending → Leased → Done | DeadLetter`
  item state machine (no inter-item edges, by doctrine), `Queue` / `Worker` /
  `ResultStore` traits, full-jitter `RetryPolicy`, store-before-ack ordering,
  and the concurrent `BatchRun` loop bounded by the fleet's total in-flight
  capacity.
- **Durable queue** (`forge-queue`): WAL SQLite with single-writer leases and
  idempotent hydrate; pure-Rust `redb` backend behind a feature for zero-C
  builds. Lease fencing: a stale worker can never close a re-leased item.
- **Streaming ingest** (`forge-shard`): 50M-line JSONL never enters RAM;
  `custom_id` validation, duplicate flagging, parse-error sidecar. Opt-in
  `--prefix-bucket` reorders input by `(model, system-prompt)` so engine
  prefix caches stay hot.
- **HTTP worker** (`forge-worker`): OpenAI-compatible POST, per-item
  `TokenUsage` capture (including `cached_tokens` / `reasoning_tokens`),
  always-on AIMD adaptive concurrency, adaptive cooldown learned from 429s /
  `Retry-After` / `x-ratelimit-*` headers, load-aware dispatch, and an
  optional `governor` GCRA req/sec ceiling (feature-gated).
- **Results** (`forge-store`): idempotent JSONL keyed by `custom_id`
  (exactly-once *effect*), dead-letter sidecar; optional `object_store`
  backend (S3/GCS/Azure) with write-if-absent objects and an O(emitted)
  manifest for resume; optional Parquet export (feature-gated, bounded RAM).

### CLI

- `forge run / resume / status / sweep / audit / verify / import / cost /
  export / serve-batch`. `status` emits human, `--json`, and `--prometheus`
  output; `audit` reports what a resume would reclaim; `verify` proves every
  input id has a terminal result at 50M scale in bounded RAM; `cost` turns
  captured usage into `$ / Mtok`, tokens-per-dollar, and savings vs a named
  online baseline.

### Spot resilience

- `forge-spot` poll-only interruption watchers (AWS/GCP/Azure), the optional
  co-located `forge-agent` drain agent with the `forge-proto` wire contract,
  and a networked lease-proxy coordinator (store-then-ack over a real socket).
- Measured **kill -9 → resume → zero-loss** proof in [BENCHMARKS.md](./BENCHMARKS.md).

### Batch REST front door

- `forge serve-batch`: the OpenAI Batch REST contract (`/v1/files`,
  `/v1/batches`, result-file retrieval) over the same engine — unmodified
  OpenAI SDK code runs its batch flow against your own endpoints, with real
  per-item progress and mid-run partial results.

[Unreleased]: https://github.com/lucheeseng827/forge/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/lucheeseng827/forge/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lucheeseng827/forge/releases/tag/v0.1.0
