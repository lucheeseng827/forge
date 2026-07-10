# forge

**forge** is a single-binary, self-hosted coordinator that fans a giant JSONL of
prompts/docs across N workers — **any OpenAI-compatible engine** (vLLM, SGLang,
llama.cpp, Ollama, …) on your own (spot) GPUs, **or hosted provider APIs**
(OpenAI-compatible and Anthropic-Messages-compatible endpoints alike) — survives
interruptions, and aggregates results cheaply. It is a **narrow batch-inference
work distributor** — explicitly **NOT** a workflow/DAG scheduler.

## The gap

Offline batch inference is ~5–10× cheaper per token than online serving (higher
GPU utilization), and spot GPUs cut another 60–90% off on-demand — but capturing
those savings means hand-gluing a heavy Python control plane to your engines, then
babysitting spot
checkpointing and retries yourself. Cloud Batch APIs (OpenAI/Anthropic/Bedrock)
hide the pain but only give a flat ~50% discount and no choice of model weights,
data locality, or GPU economics. There is no lightweight, self-hosted, single
binary that does just the orchestration **shell** — work queue, sharding,
retry/checkpoint, spot-interruption handling, backpressure, result aggregation —
while the GPU heavy-lifting stays in the engines where it belongs. forge is that
shell.

## Quickstart

forge is BYO-endpoints: point it at whatever serves your model — one or more
self-hosted OpenAI-compatible engines (vLLM `--max-num-seqs`, SGLang
`--max-running-requests`, llama-server `--parallel`, Ollama, a router…) — and
drop a JSONL. Hosted provider endpoints work too: set `--engine openai-api` or
`--engine anthropic-api` and export `FORGE_WORKER_API_KEY` (env-only, never
logged); items in the Anthropic Messages shape (`url: /v1/messages`, e.g.
straight out of `forge import`) drive an Anthropic-compatible API natively,
with usage metered into the same cost ledger.

```sh
forge run \
  --input prompts.jsonl \
  --workers http://gpu1:8000,http://gpu2:8000 \
  --engine vllm \
  --out results.jsonl \
  --checkpoint .forge/state.db

# kill the coordinator (or a worker) mid-run, then:
forge resume   --checkpoint .forge/state.db          # reuses the original run's --out automatically
forge status   --checkpoint .forge/state.db          # counts + success_rate + retry dist + failure mix
forge status   --checkpoint .forge/state.db --json   # machine-readable report (monitoring/alerting)
forge status   --checkpoint .forge/state.db --prometheus   # Prometheus textfile metrics to scrape
forge sweep    --checkpoint .forge/state.db   # re-queue leased-but-no-result items (results land on the next run/resume)
forge verify   --input prompts.jsonl --results results.jsonl     # prove every custom_id has a terminal result (bounded RAM; exit≠0 on a gap)
```

No GPU handy? [`examples/`](./examples/) ships a zero-dependency mock
OpenAI-compatible engine so you can watch the full fan-out → checkpoint → resume
loop (including the dead-letter and parse-error sidecars) on your laptop. What
success looks like (real output, captured against the mock engine — one line has a
poison request on purpose):

```text
$ forge run --input examples/prompts.jsonl --workers http://127.0.0.1:8000 \
    --out results.jsonl --checkpoint .forge/state.db --max-attempts 3
INFO forge: ingested lines=6 hydrated=6 rejected=0
WARN forge_core::run: dead-lettering item custom_id=poison-1 error=worker: retries exhausted after 3 attempt(s): HTTP 500 Internal Server Error
done=5 dead=1 tokens_this_run=75 (total items 6)

$ forge status --checkpoint .forge/state.db
pending=0 leased=0 done=5 dead=1 total=6 success_rate=83.3% retried=0 failures[server_error=1]

$ forge resume --checkpoint .forge/state.db --workers http://127.0.0.1:8000   # idempotent no-op
done=5 dead=1 tokens_this_run=0 (total items 6)

$ forge verify --input examples/prompts.jsonl --results results.jsonl
input_ids=6 emitted_ids=6 missing=0 extra=0 duplicate_input=0 rejected_input=0
complete: every input id has a terminal result ✅
```

Reference docs: **[`docs/DESIGN.md`](./docs/DESIGN.md)** (architecture, the
end-to-end event flow, delivery semantics, and what this design offers that
alternatives don't) · **[`docs/CONFIG.md`](./docs/CONFIG.md)** (every flag/env/feature in
one table) · **[`docs/API.md`](./docs/API.md)** (the engine wire contract, the
coordinator HTTP protocol, the library entry points) ·
**[`docs/OPERATIONS.md`](./docs/OPERATIONS.md)** (deploy, backup, monitoring,
symptom-first troubleshooting, security posture).

`--engine` is only a per-run **default hint** for engine-specific quirks (the
health-probe path and the concurrency knob to respect). All workers in one run
share it; mixed-engine fleets need a per-worker override (planned). The
wire contract is OpenAI-compatible either way, so forge still treats every worker
as a black-box endpoint.

Input is the de-facto **OpenAI Batch** contract — one independent request per
line, keyed by a caller-supplied `custom_id` (Anthropic `custom_id` and Bedrock
`recordId` accepted as aliases). A file already in the OpenAI shape runs as-is;
for an Anthropic Message-Batches file (`params` body) or a Bedrock file
(`recordId` + `modelInput`), run **`forge import`** once to normalize it into
forge-native JSONL (it also validates ids, flags duplicates, and sidecars bad
lines):

```sh
forge import --input anthropic_or_bedrock_batch.jsonl --out forge.jsonl
forge run --input forge.jsonl --workers http://gpu1:8000 --out results.jsonl --checkpoint .forge/state.db
```

```jsonl
{"custom_id":"req-001","method":"POST","url":"/v1/chat/completions","body":{"model":"Qwen/Qwen2.5-7B-Instruct","messages":[{"role":"user","content":"Summarize: ..."}]}}
{"custom_id":"req-002","method":"POST","url":"/v1/chat/completions","body":{"model":"Qwen/Qwen2.5-7B-Instruct","messages":[{"role":"user","content":"Translate to French: ..."}]}}
{"custom_id":"req-003","method":"POST","url":"/v1/embeddings","body":{"model":"BAAI/bge-m3","input":"..."}}
```

Output is a **matched JSONL keyed by the same `custom_id`** — order is **not**
guaranteed (map by id, never by position); failed items route to a separate
dead-letter file:

```jsonl
{"custom_id":"req-001","response":{"status_code":200,"body":{"choices":[{"message":{"content":"..."}}],"usage":{"prompt_tokens":312,"completion_tokens":48,"total_tokens":360}}},"usage":{"prompt_tokens":312,"completion_tokens":48,"total_tokens":360}}
{"custom_id":"req-002","response":{"status_code":200,"body":{"choices":[{"message":{"content":"..."}}],"usage":{"prompt_tokens":40,"completion_tokens":12,"total_tokens":52}}},"usage":{"prompt_tokens":40,"completion_tokens":12,"total_tokens":52}}
{"custom_id":"req-003","response":{"status_code":200,"body":{"data":[{"embedding":[0.01,-0.02]}],"usage":{"prompt_tokens":7,"completion_tokens":0,"total_tokens":7}}},"usage":{"prompt_tokens":7,"completion_tokens":0,"total_tokens":7}}
```

The single static binary boots and starts dispatching in seconds — no Python, no
DB server, no cluster.

## Prove the savings

`forge cost` turns the **real** `usage` forge captured into the invoiceable
arbitrage — forge `$/Mtok`, tokens-per-dollar, and dollars saved vs paying the
online API for the same tokens. You name the prices; it never guesses.

```sh
# A 50M-item summarization run (~35B input / ~7.5B output tokens) that cost
# ~$2,000 of spot GPU, vs an online API at $0.50/Mtok in + $1.50/Mtok out:
forge cost --results results.jsonl \
  --gpu-cost 2000 --online-per-mtok-input 0.50 --online-per-mtok-output 1.50
# forge:  $2000.0000  ($0.0471/Mtok · 21250000 tokens/$)
# online: $28750.0000  →  saved $26750.0000 (93.0%)
```

It's honest both ways: on a tiny job that doesn't amortize the GPU bill, the
"saved" figure goes negative — telling you the batch wasn't worth a dedicated fleet.

It also captures the nested usage detail engines report — `cached_tokens` (prompt
prefill served from the prefix cache) and `reasoning_tokens` — and prints them in the
breakdown. Pass `--online-per-mtok-cached-input` to price cache hits at the online
API's discounted rate for an apples-to-apples baseline; omit it and cached tokens
price at the full input rate (conservative — never overstates the savings). Pair with
`forge run --prefix-bucket`, which reorders the input by shared `(model, system-prompt)`
so the engine's automatic prefix cache stays hot and those `cached_tokens` actually
materialize.

## Embed it

The CLI is a thin driver over `forge-core`; an orchestrator can embed the same
brain directly:

```rust
use forge_core::{BatchRun, EndpointKind, WorkerSpec};
use forge_queue::SqliteQueue;
use forge_worker::HttpWorker;
use forge_store::JsonlStore;

// BYO OpenAI-compatible endpoints. `WorkerSpec` is the declared capability;
// `HttpWorker` is the live client + per-worker in-flight semaphore over it.
let workers = vec![
    HttpWorker::new(WorkerSpec::new("gpu1", "http://gpu1:8000", EndpointKind::Chat).concurrency(256))?,
    HttpWorker::new(WorkerSpec::new("gpu2", "http://gpu2:8000", EndpointKind::Chat).concurrency(256))?,
];

let queue = SqliteQueue::open(".forge/state.db")?;          // durable, single-writer
forge_shard::ingest_jsonl(&queue, "prompts.jsonl").await?;  // stream + hydrate (50M lines never enter RAM)

let store = JsonlStore::new("results.jsonl");               // idempotent, keyed by custom_id
let totals = BatchRun::new(queue, workers, store)
    .run()                                                  // fan out, lease, retry, checkpoint, aggregate
    .await?;

println!("done={} dead={} tokens={}", totals.items_done, totals.items_dead, totals.tokens_total());
# anyhow::Ok(())
```

The crate split is the dependency story: `forge-core` is the leaf (types, the
`Queue`/`Worker`/`ResultStore` traits, and the fan-out loop) and `forge-queue` /
`forge-worker` / `forge-store` / `forge-shard` are interchangeable drivers behind
those traits — so the same loop runs over SQLite + HTTP + JSONL today and other
backends later, with zero `dyn` dispatch.

Crash-safety is structural: at-least-once delivery via lease/visibility-timeout,
idempotent result writes keyed by `custom_id` (**exactly-once *effect***, not
exactly-once execution), and a "checkpoint" that is simply the queue state +
input byte-offset index + result manifest. A fresh coordinator reopens the file,
expires stale leases, skips already-emitted `custom_id`s, and continues. See
[`docs/DESIGN.md`](./docs/DESIGN.md) for the full component map + event flow, and
[`./BENCHMARKS.md`](./BENCHMARKS.md) for the measured **kill-9 → resume →
zero-loss** proof (300-item batch, killed mid-flight, every id done exactly once,
no input⨝output join). `forge audit --checkpoint <db>` reports what a `resume`
would reclaim — pending + orphaned leases from dead/spot-killed workers — without
that join.

## Build a static binary

The single static binary is the whole pitch — no Python, no shared libs to ship.

```sh
# x86_64 static musl (bundled SQLite compiled against musl):
rustup target add x86_64-unknown-linux-musl
sudo apt-get install -y musl-tools          # provides musl-gcc for the bundled C
CC_x86_64_unknown_linux_musl=musl-gcc \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
  cargo build -p forge-cli --release --target x86_64-unknown-linux-musl
file target/x86_64-unknown-linux-musl/release/forge   # → ELF, static-pie, stripped (~4 MB)
```

The result is a ~4 MB fully static, dependency-free binary that boots and starts
dispatching in well under a second. Tagged releases build and publish it via CI.
A [`Dockerfile`](./Dockerfile) wraps the same binary in a FROM-scratch image, and
[`deploy/helm/forge`](./deploy/helm/forge/README.md) runs it on Kubernetes
(`serve-batch` Deployment + one-shot `run` Job with resume-on-eviction).

**Zero-C alternative.** The `redb` queue backend (`forge-queue` feature `redb`) is
pure Rust, so an embedder that wants the cleanest cross-compile (no C toolchain at
all) can build against it instead of bundled SQLite. The default CLI ships the
mature SQLite-bundled backend; `redb` is the library-level zero-C option.

## Write results to object storage (S3 / GCS / Azure)

The lean default binary writes a local results JSONL. To stream results to object
storage instead, build with the off-by-default `object_store` feature and pass an
`--out` **URL** (the checkpoint queue stays local; only the results go remote):

```sh
cargo build -p forge-cli --release --features object_store
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1 \
  forge run --input prompts.jsonl --workers http://gpu1:8000 \
    --out s3://my-bucket/run-42/ --checkpoint .forge/state.db
forge resume --checkpoint .forge/state.db                          # reuses the recorded s3:// URL
forge cost   --results s3://my-bucket/run-42/ --gpu-cost 2000 --online-per-mtok-input 0.50
forge verify --input prompts.jsonl --results s3://my-bucket/run-42/   # completeness over the objects
```

Each result is its own immutable object under `<url>/results/<job>/done|dead/<custom_id>.json`
(write-if-absent, so a re-run/resume is an idempotent no-op), and resume rebuilds its
dedup set from an O(emitted) `_manifest/` prefix — it never re-reads the result objects.
`forge cost` and `forge verify` take that same URL back: **cost** sums usage straight from
the `done/` objects, and **verify** runs its exact, bounded-RAM completeness sweep over the
`_manifest/` (every terminal id, done *and* dead) — so a 50M-id run never materializes the
id set. Credentials/region come from the standard `AWS_*` / `GOOGLE_*` / `AZURE_*`
environment variables. `gs://`, `az://`, `file://`, and `memory://` work the same way. The
default (feature-off) binary refuses an object-store `--out`/`--results` with a clear
error, so the static musl binary never pulls the cloud SDKs.

## Serve the OpenAI Batch REST API

`forge serve-batch` puts the **real OpenAI Batch REST contract** (`/v1/files`,
`/v1/batches`) in front of the same engine — so unmodified OpenAI-SDK code runs its
batch flow against your own GPUs, no 24-hour cloud SLA, no black box. Verified
end-to-end against the `openai` Python SDK (`batches.create` / `retrieve` /
`files.content`).

```sh
forge serve-batch --listen 0.0.0.0:8080 --data-dir ./batchdata \
  --workers http://gpu1:8000,http://gpu2:8000 --engine vllm --api-key "$KEY"
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://your-host:8080/v1", api_key=KEY)
b = client.batches.create(input_file_id=fid, endpoint="/v1/chat/completions",
                          completion_window="24h")
while client.batches.retrieve(b.id).status != "completed":
    time.sleep(5)
out = client.files.content(client.batches.retrieve(b.id).output_file_id)
```

Real per-item progress (`request_counts` straight from the live queue), mid-run
partials, spot-safe resume underneath — the things the cloud Batch APIs don't give
you. See [`docs/API.md` §4](./docs/API.md#4-batch-rest-api-openai-compatible) for the
full route table, object shapes, and the honest MVP limits (file upload is raw-body,
not multipart, today).

## What forge is NOT

forge is a **single homogeneous fan-out of independent items across
interchangeable workers; arbitrary task graphs with inter-task dependencies are
[dagron](https://github.com/lucheeseng827/dagron)'s job.** The boundary is enforced by the data
model, not by discipline — there is nowhere in `forge-core` to express what
dagron exists to express. forge has **no** task graph, **no** `depends_on` /
fan-in, **no** cron/triggers, **no** YAML workflow topology, **no** fleet
provisioning, and it never batches on the wire (the engine's continuous batching
packs concurrent requests). forge can *hand* a flat plan to dagron — the arrow
points outward only (`forge → dagron`); it never absorbs DAG features inward.

Provisioning ("get me 400 spot GPUs across 12 regions") is **out of scope** —
that is your provisioner's / the operator's job. forge consumes spot
interruption signals and re-queues at item granularity; it never launches, tears
down, or autoscales instances.

## Status

> **The core loop works end-to-end.** `forge run` ingests a JSONL, fans it
> across BYO OpenAI-compatible endpoints (real `reqwest` POST + `usage` capture +
> full-jitter retry), checkpoints each result keyed by `custom_id`, dead-letters
> poison items, and `resume` is an idempotent no-op — verified against a live mock
> engine. A leased batch is dispatched **concurrently**, bounded by the fleet's
> total in-flight capacity (Σ `concurrency_limit`). Apache-2.0, a per-PR CI gate +
> doctrine guard, the pure-Rust `redb` backend, the static `x86_64-…-musl` build
> (~4 MB), and a SIGKILL crash/`resume` proof have all landed. Build it standalone:
>
> ```sh
> cargo test --workspace   # 140 tests pass
> cargo run -p forge-cli -- --help
> ```

### Done

- [x] Architecture chosen: **embeddable engine + thin CLI**, single static
      binary (rusqlite-bundled, redb behind a feature)
- [x] Wire contract fixed: OpenAI-compatible HTTP to black-box workers + the
      OpenAI Batch `custom_id` JSONL schema (Anthropic/Bedrock aliases)
- [x] **`forge-core` implemented:** data model + item state machine
      (`Pending → Leased → Done | DeadLetter`, zero inter-item edges), the
      `Queue`/`Worker`/`ResultStore` traits, `RetryPolicy` (full-jitter backoff),
      and the `BatchRun` fan-out loop with the store-before-ack ordering
- [x] **`forge-shard` implemented:** streaming JSONL ingest + `custom_id`
      validation + batched idempotent hydrate (file never enters RAM)
- [x] **`forge-queue` implemented (P1.2):** `SqliteQueue` over WAL SQLite — the
      single-writer lease `UPDATE … RETURNING` (emulating `SELECT … FOR UPDATE SKIP
      LOCKED`), idempotent `ON CONFLICT DO NOTHING` hydrate, ack/reap/dead-letter,
      and `counts`, all run on `tokio::task::spawn_blocking`
- [x] **`forge-worker` implemented (P1.4):** `HttpWorker` — `reqwest` POST to the
      OpenAI-compatible endpoint, `usage` → `TokenUsage`, per-worker in-flight
      semaphore (= `concurrency_limit`), 429/5xx/timeout retry with full-jitter
      backoff, terminal 4xx no-retry, and a `/health` `probe`
- [x] **`forge-store` implemented (P1.5):** `JsonlStore` — append-only result
      JSONL + sibling dead-letter file, idempotent/dedup-on-resume via an
      emitted-id set seeded from the existing files
- [x] **Concurrent dispatch (P1.1, upgraded to a sliding window):** up to the
      fleet's total capacity (Σ `concurrency_limit`) items are in flight at once,
      and every completion immediately frees a slot that is topped up against the
      workers' FREE slots — so a slow worker only ever occupies its own slots and
      can never head-of-line-block the fleet. Each worker's in-flight semaphore
      stays the hard throttle — no `spawn`/`Arc` (futures borrow `&self`)
- [x] **Lease fencing & resume ergonomics:** `ack`/`dead_letter` are fenced on the
      lease generation (a stale worker can't close a re-leased item); `run` records
      the output path so `resume` reuses it automatically (no split export)
- [x] **`forge-cli` wired:** `run`/`resume`/`status`/`sweep` over the above.
      Compiles, clippy-clean (`-D warnings`), **113 tests pass** (incl. a SIGKILL
      crash/resume test, the `redb` backend suite, the AIMD controller tests, the
      `forge-agent` drain tests, wiremock HTTP tests + full-pipeline & concurrency
      integration tests)
- [x] **Parse-error sidecar:** malformed / invalid-`custom_id` input lines are
      preserved (never abort the job) to `<out>.reject.jsonl` for repair + re-ingest
- [x] **Observability:** `forge status` with `--json` and `--prometheus` outputs +
      a terminal success-rate, an attempt/retry distribution (the spot re-queue cost
      signal), and a **per-reason failure breakdown** (validation / server_error /
      client_error / rate_limited / timeout / connection) so you can alert when the
      failure mix shifts
- [x] **Result-validation hook (`--require nonempty|json`):** a structurally-bad 2xx
      (empty generation / non-JSON when JSON was required) is a *soft failure* —
      retried then dead-lettered, never silently emitted. Per-item endpoint-aware.
      The standout no closed Batch API offers (Bedrock batch even *drops* structured
      output)
- [x] **`forge import`:** the off-ramp from the closed Batch APIs — normalizes
      OpenAI / Anthropic (`params`) / Bedrock (`recordId`+`modelInput`) batch files
      into forge-native JSONL, inferring the URL, validating ids, flagging duplicates,
      sidecarring bad lines
- [x] **`forge cost`:** the invoiceable cost-arbitrage report from **real** captured
      tokens — forge `$/Mtok`, tokens-per-dollar, and `$ saved vs the online API` for
      the same tokens
- [x] **`forge verify` (completeness sweep):** proves **every** input `custom_id` has
      a terminal result (a result line or a dead-letter line) and exits non-zero on a
      gap — the completeness acceptance check. It runs at **50M scale in bounded RAM** via
      an exact external sort-merge set-difference (sorted run buffers → spills →
      streaming k-way merge), not a Bloom filter (which could silently call a missing
      id present). Report-only by doctrine — re-queue stays `forge sweep`
- [x] **Object-store output (`object_store`, optional):** an `ObjStore` `ResultStore`
      over S3 / GCS / Azure / local / in-memory — each terminal result a
      conditional-`PutMode::Create` object (write-if-absent = idempotent exactly-once
      *effect*), with an O(emitted) `_manifest/` so resume rebuilds the dedup set
      without re-reading 50M result objects. **Wired into the CLI:** an `--out` that is a
      `scheme://` URL (`s3://` / `gs://` / `az://` / `file://` / `memory://`) writes to
      the object store, and `forge resume` reuses the recorded URL; cloud creds/region
      come from the `AWS_*`/`GOOGLE_*`/`AZURE_*` env vars. Behind an **off-by-default
      `object_store` feature**, so the static-musl `forge-cli` never compiles the cloud
      SDKs (CI asserts their absence); the default binary refuses an object-store `--out`
      with a clear "needs `--features object_store`" message
- [x] **Parquet columnar output (`parquet`, optional):** `forge export` (and the
      `jsonl_to_parquet` library fn) turns a results JSONL into columnar Parquet — one
      row per `custom_id` — streaming a row group at a time so a 50M-row export stays
      bounded in RAM. Behind an **off-by-default `parquet` feature** (the ~30-crate
      arrow stack never reaches the lean binary, CI-asserted); Snappy-only so even the
      feature-on build is musl-static
- [x] **`--max-attempts` now honored** by the worker (was silently fixed at the
      default of 5 — the flag was dropped before reaching `HttpWorker`)
- [x] **Adaptive lease TTL:** the visibility timeout grows from `--lease-secs`
      toward `--lease-max-secs` as the run observes real per-item latency, so a slow
      long-output generation isn't prematurely re-queued (wasting GPU) — and it's
      still heartbeat-free (the deliberate OSS lease-model choice)
- [x] **AIMD adaptive concurrency:** each worker's in-flight limit now self-tunes —
      additive-increase (+1 permit per 8 stable 2xx) toward the configured ceiling,
      multiplicative-decrease (halve) on any 429/5xx/connection error, with the cut
      paid lazily as in-flight permits return (forget-debt). Always-on, no new deps,
      bounded `[1, concurrency_limit]` — so forge converges on an endpoint's real
      throughput ceiling instead of you tuning a batch script for days
- [x] **Spot-drain agent (`forge-agent` + `forge-proto`):** the optional co-located
      agent runs the `forge-spot` watcher **in the VM**; a spot/preempt notice flips a
      one-way drain latch so the run loop stops pulling new leases, drains the
      in-flight batch *if the window allows*, and leaves the rest leased for
      lease-expiry re-queue (zero already-checkpointed loss). It only *proposes*
      results over the deliberately-small `forge-proto` wire contract (no task-graph
      types) — the coordinator stays the single writer
- [x] **Load-aware dispatch (B3):** dispatch biases toward the least-loaded ready
      worker using each engine's own queue-depth metric (vLLM `vllm:num_requests_waiting`
      from `/metrics`, llama.cpp busy `/slots`, SGLang `/get_server_info`) — read
      best-effort during the health probe into a cached `Worker::load()`, so the pick
      stays a synchronous index (no await in the fan-out, no spawn/Arc). It fills the
      idlest engines first up to their cap and **degrades to exact round-robin** when
      no metric is exposed (a true no-op); `--no-load-aware` forces round-robin. Pure
      flow control — no per-item routing, zero new deps
- [x] **Request-rate ceiling (`governor`, optional):** an opt-in global GCRA req/sec
      cap for a worker (e.g. against a shared engine or a cloud Batch API's RPS limit),
      orthogonal to AIMD — AIMD caps in-flight *concurrency*, this caps the emission
      *rate*. Behind an **off-by-default `governor` feature** on `forge-worker`, so the
      static-musl `forge-cli` binary never compiles it (CI asserts its absence);
      `WorkerSpec::rate_limit(req_per_sec)` carries the setting
- [x] **Adaptive cooldown from response headers:** AIMD reacts to failure *shape*;
      this obeys the endpoint's *stated* backoff. A 429 (or any response) carrying
      `Retry-After` or an exhausted `x-ratelimit-reset-{requests,tokens}` budget arms a
      shared per-worker cooldown; `submit()` waits it out **at entry, before the governor
      gate**, so the whole fleet backs off that worker in lockstep instead of each item
      rediscovering the limit. Clamped to 120s, always-on, no config
- [x] **Prefix-cache-aware dispatch ordering (`--prefix-bucket`, opt-in):** a
      bounded-RAM K-way pass that reorders the input by shared `(model, system-prompt)`
      so prefix-similar items are contiguous and the engine's automatic prefix cache
      (vLLM/SGLang) stays hot across a run of them; the resulting hits surface as
      `cached_tokens` in the captured usage and feed `forge cost`'s discounted-cache
      pricing
- [x] **Networked lease-proxy transport:** `forge-agent` talks to a remote coordinator
      over the `forge-proto` HTTP protocol — an `HttpCoordinator` client + a minimal
      `tiny_http` `serve_coordinator` server that does **store-then-ack** fenced by
      lease generation (so a stale post is a harmless no-op and single-writer holds
      over the wire). `usage`/latency ride the wire so `forge cost` stays faithful;
      proven end-to-end over a real socket. The binary exposes `watch` / `run` /
      `serve` (`tiny_http` is confined to this optional binary — the core `forge`
      musl build is untouched)
- [x] **Apache-2.0 `LICENSE`**, a zero-GPU [`examples/`](./examples/) quickstart, a
      per-PR CI gate (fmt + clippy `-D warnings` + tests + locked build), and the
      no-`petgraph` / no-inter-item-edge **doctrine guard**
- [x] Delivery semantics defined: at-least-once + idempotent writes =
      exactly-once *effect*; single-writer coordinator discipline
