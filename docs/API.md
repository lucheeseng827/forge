# forge — API reference

forge has three API surfaces:

1. [The engine-facing wire contract](#1-engine-facing-wire-contract) — what forge sends *to* your workers (you implement nothing; any OpenAI-compatible server works).
2. [The coordinator HTTP protocol (`forge-proto`)](#2-coordinator-http-protocol-forge-proto) — the three routes `forge-agent serve` exposes and `forge-agent run` consumes.
3. [The library API](#3-library-api-forge-core-and-friends) — the public entry points for embedding forge.

## 1. Engine-facing wire contract

Source: `HttpWorker::submit`/`probe` in [`forge-worker/src/lib.rs`](../forge-worker/src/lib.rs).

For each item, forge issues `POST {worker_base_url}{item.url}` (the item's `url`, or
the run's `--endpoint` default: `/v1/chat/completions`, `/v1/completions`,
`/v1/embeddings`, or `/v1/messages`) with the item's `body` passed through
**verbatim** as JSON. The contract is provider-neutral: anything speaking the
OpenAI-compatible surface works unmodified, and the Anthropic-native Messages
shape is supported alongside it.

| Worker | `--engine` hint | Auth | Notes |
|---|---|---|---|
| Self-hosted OpenAI-compatible engine (vLLM, SGLang, llama.cpp, Ollama, routers, …) | `vllm` / `sglang` / `llamacpp` / `router` | none (network-protected) | health probe + load-aware dispatch per the hint |
| Hosted OpenAI-compatible API | `openai-api` | `FORGE_WORKER_API_KEY` → `Authorization: Bearer` | probe skipped (assumed ready); rate limits learned from response headers |
| Hosted Anthropic-compatible Messages API | `anthropic-api` | `FORGE_WORKER_API_KEY` → `x-api-key` + `anthropic-version` | items carry the Messages body (`url: /v1/messages`); usage (`input_tokens`/`output_tokens`, `cache_read_input_tokens`) maps into the common token accounting |

- **Auth is env-only and off by default.** With no `FORGE_WORKER_API_KEY` set, forge
  sends no `Authorization`/API key to workers — BYO endpoints are assumed
  network-protected (see [OPERATIONS §security](./OPERATIONS.md#security-posture)).
  The key is never a flag and never logged (marked sensitive in the HTTP client).
- **Health probe:** `GET {base_url}/health`; any 2xx = ready. Best-effort load metric
  read alongside it (vLLM `GET /metrics`, SGLang `GET /get_server_info`, llama.cpp
  `GET /slots`, 2 s timeout). Hosted hints skip the probe entirely — real responses
  (429s, errors) drive AIMD and cooldown instead.
- **Response handling:** 2xx → body decoded as JSON, `usage` captured into token
  accounting per the *item's* endpoint shape — OpenAI-style (incl. the nested
  `prompt_tokens_details.cached_tokens` and
  `completion_tokens_details.reasoning_tokens`) or Anthropic Messages
  (`input_tokens`/`output_tokens`; total derived) — `x-request-id` header preserved
  if present. 429/408/5xx/timeouts →
  retried with full-jitter backoff up to `--max-attempts`. Other 4xx → terminal,
  dead-lettered without retry. A 2xx that fails `--require` validation is retried
  like a 5xx.
- **Adaptive rate control (response-learned, no config):** every response's rate-limit
  headers arm a per-worker cooldown that all subsequent items on that worker wait out
  at entry — so the whole fleet backs off in lockstep with what the endpoint told us,
  instead of each item rediscovering the limit with its own 429. Honored signals:
  `Retry-After` (seconds) on a 429/503, and `x-ratelimit-reset-{requests,tokens}` when
  the paired `x-ratelimit-remaining-*` has reached 0. Clamped to 120 s per signal
  (a hostile `Retry-After` can't strand a worker; longer waits re-arm). Orthogonal to
  the AIMD concurrency limiter and the optional `governor` req/s ceiling.
- TLS: `https://` engine URLs work (rustls).

## 2. Coordinator HTTP protocol (forge-proto)

Source: routes in [`forge-agent/src/http.rs`](../forge-agent/src/http.rs) (`route`),
types in [`forge-proto/src/lib.rs`](../forge-proto/src/lib.rs). Served by
`forge-agent serve`, spoken by `forge-agent run`'s `HttpCoordinator`.

Every route is **`POST`, JSON in / JSON out**, and **unauthenticated** — the server
binds loopback by default for exactly that reason. There are deliberately no
task-graph types on this wire (no `depends_on`, no fan-in): it is a flat lease-proxy
protocol.

Common error responses (plain-text body):

| code | when |
|---|---|
| `400` | unreadable or unparseable request body (`bad request body: …`) |
| `404` | unknown path (`no route for …`) |
| `405` | non-POST method |
| `500` | queue/store error (message = the internal error) |

### `POST /v1/lease` — pull a lease batch

Request (`LeasePull`):

```json
{"worker_id": "gpu1", "max": 64, "lease_secs": 120}
```

`lease_secs: 0` means "use the server's `--lease-secs` default". Response
(`LeaseGrant`, HTTP 200) — `items` may be empty ("nothing ready; back off and poll
again"):

```json
{"items": [{
  "custom_id": "req-001",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {"model": "Qwen/Qwen2.5-7B-Instruct", "messages": [{"role": "user", "content": "…"}]},
  "lease_generation": 1,
  "attempts": 0
}]}
```

`lease_generation` is the fencing token: echo it back in the result post. A lease
that expires is re-queued by the coordinator's reaper and re-granted with a higher
generation.

### `POST /v1/result` — propose one finished item

The agent only *proposes*; the **server** writes, in the canonical store-then-ack
order (durable result first, then the lease-fenced queue transition). Request
(`ResultPost`), success form:

```json
{
  "custom_id": "req-001",
  "lease_generation": 1,
  "worker_id": "gpu1",
  "outcome": {"done": {
    "status_code": 200,
    "response": {"choices": [{"message": {"content": "…"}}], "usage": {"total_tokens": 360}},
    "usage": {"prompt_tokens": 312, "completion_tokens": 48, "total_tokens": 360},
    "latency_ms": 812
  }}
}
```

Dead-letter form (retries exhausted / terminal 4xx / failed content check):

```json
{"custom_id": "req-002", "lease_generation": 3, "worker_id": "gpu1",
 "outcome": {"dead_letter": {"error": "HTTP 500 after 5 attempts"}}}
```

Response (`AckReply`, HTTP 200): `{"acked": true}` — or `{"acked": false}` when the
lease was stale (expired and re-leased elsewhere). **`false` is a harmless no-op**:
the store write already de-duplicated, so the agent treats both as success. Posts
are idempotent and safe to retry.

### `POST /v1/interruption` — advisory spot notice

```json
{"worker_id": "gpu1", "cloud": "aws", "kind": "terminate", "deadline": "2026-07-07T17:48:00Z"}
```

Response: `{"ok":true}`. **Purely advisory** — the coordinator logs it and relies on
lease expiry regardless; a dropped notice changes nothing (`deadline` is optional).

## 4. Batch REST API (OpenAI-compatible)

`forge serve-batch` fronts the existing engine with the **OpenAI Batch REST API**, so
unmodified OpenAI-SDK code runs its batch flow against forge (verified end-to-end with
the `openai` Python SDK: `batches.create` / `batches.retrieve` / `files.content`).
Served by the `forge-batch` crate (`tiny_http`); every route except `/v1/health` needs
`Authorization: Bearer <api-key>` when `--api-key` is set.

| method | path | body / params | returns |
|---|---|---|---|
| `GET` | `/v1/health` | — (no auth) | `{status:"ok", batches:N}` |
| `POST` | `/v1/files` | raw JSONL body (`purpose=batch`) | File object `{id:"file-…", object:"file", bytes, created_at, filename, purpose}` |
| `GET` | `/v1/files/{id}` | — | the File object |
| `GET` | `/v1/files/{id}/content` | — | input file → bytes as-is; **output** file → one OpenAI batch-output line per result (see below) |
| `POST` | `/v1/batches` | `{input_file_id, endpoint, completion_window:"24h"}` | Batch object, `status:"in_progress"` — starts the fan-out across the server's fleet |
| `GET` | `/v1/batches/{id}` | — | Batch object; `request_counts` + `status` read live from the queue; `output_file_id`/`error_file_id` set on completion |
| `GET` | `/v1/batches` | — | `{object:"list", data:[Batch…]}` (newest-first, unpaginated) |
| `POST` | `/v1/batches/{id}/cancel` | — | Batch object, `status:"cancelled"` |

**Batch object**: `{id:"batch-…", object:"batch", endpoint, input_file_id,
completion_window, status, output_file_id, error_file_id, created_at, in_progress_at,
completed_at, request_counts:{total, completed, failed}}`. `status` is `in_progress`
until the queue drains, then `completed` (`request_counts` comes straight from queue
counts — never double-tracked).

**Output line** (`/v1/files/{output_id}/content`): `{id:"batch_req_…", custom_id,
response:{status_code, request_id, body}, error:null}` for a done item;
`{…, response:null, error:{…}}` for a dead-lettered one. Same `custom_id` keying as the
CLI results file — map by id, never position.

**Honest MVP limits** (see the `forge-batch` rustdoc): `POST /v1/files` takes the raw
JSONL body, **not** multipart — the SDK's `files.create` (multipart) needs a raw POST
shim; every other SDK call is native. The `cancelling` transient is elided (→ straight
to `cancelled`). Output content is buffered while reshaping (input files stream). No
list pagination / `metadata`. A `serve-batch` **restart re-lists batches** from disk
and re-finalizes any that drained while it was down, but does **not** auto-resume an
in-flight run — the batch's `ckpt.db` is intact for `forge resume`.

## 3. Library API (forge-core and friends)

The CLI is a thin driver; everything is embeddable. Full rustdoc lives in the
crates; these are the entry points with one snippet each.

### The fan-out loop — `forge_core::BatchRun`

```rust
use forge_core::{BatchRun, RunConfig, EndpointKind, WorkerSpec};
use forge_queue::SqliteQueue;
use forge_worker::HttpWorker;
use forge_store::JsonlStore;

let workers = vec![
    HttpWorker::new(WorkerSpec::new("gpu1", "http://gpu1:8000", EndpointKind::Chat).concurrency(256))?,
];
let queue = SqliteQueue::open(".forge/state.db")?;              // durable, single-writer
forge_shard::ingest_jsonl(&queue, "prompts.jsonl").await?;      // streaming hydrate
let store = JsonlStore::new("results.jsonl");                   // idempotent, keyed by custom_id
let totals = BatchRun::new(queue, workers, store)
    .with_config(RunConfig::default())
    .run()                                                      // lease → dispatch → store → ack
    .await?;
println!("done={} dead={}", totals.items_done, totals.items_dead);
# anyhow::Ok(())
```

### The three seams — `Queue` / `Worker` / `ResultStore` (traits in `forge_core::traits`)

Implement any of them to swap a backend; the loop is generic (zero `dyn` dispatch).

| trait | contract | shipped impls |
|---|---|---|
| `Queue` | `enqueue` (idempotent), `lease(max, ttl)` (atomic, stamps a generation), `ack`/`dead_letter` (fenced on the generation — a stale worker can't close a re-leased item), `reap`, `counts` — plus the defaulted ingest seek-index pair `record_shard`/`hydrated_through` (default: no-op/`None`, i.e. a re-ingest rescans; `SqliteQueue` implements both, giving `forge_shard` seek-resume) | `forge_queue::SqliteQueue` (WAL SQLite, `open`/`open_in_memory`), `forge_queue::RedbQueue` (feature `redb`, pure Rust) |
| `Worker` | `spec()`, `is_ready()`, `probe()`, `load()` (cached queue depth or `None`), `submit(&Item) -> ItemResult` — returns `Err` only once its own retry policy is exhausted | `forge_worker::HttpWorker` (`new`, `.with_retry(RetryPolicy)`, `.with_validation(ResponseCheck)`, `.with_rate_limit(rps)` behind feature `governor`) |
| `ResultStore` | `put`/`dead_letter` — idempotent, keyed by `custom_id`; a `custom_id` is terminal exactly once across both sinks | `forge_store::JsonlStore`, `forge_store::ObjStore` (feature `object_store`), `forge_store::ParquetStore` (feature `parquet`) |

### Ingest & verify — `forge_shard`

```rust
// Streaming ingest: 50M lines never enter RAM; rejects go to a sidecar. A re-run of an
// interrupted ingest seeks past the already-hydrated prefix (Queue::hydrated_through);
// the input must be the same append-only file (a shrunk/replaced input falls back to a
// full rescan — see OPERATIONS).
let stats = forge_shard::ingest_jsonl_with_rejects(&queue, "in.jsonl", Some("in.reject.jsonl".as_ref())).await?;

// Exact bounded-RAM completeness sweep (external sort-merge, not a Bloom filter):
let cfg = forge_shard::VerifyConfig { run_capacity: 1_000_000, missing_out: None };
let report = forge_shard::verify_completeness("in.jsonl", "results.jsonl", Some("results.jsonl.dead.jsonl".as_ref()), &cfg).await?;
assert!(report.is_complete());

// Normalize a closed-Batch-API file (OpenAI/Anthropic/Bedrock) to forge-native JSONL:
let s = forge_shard::import_batch("anthropic.jsonl", "forge.jsonl", None, forge_core::EndpointKind::Chat).await?;
# anyhow::Ok(())
```

### Cost accounting — `forge_core::compute_cost` + `forge_store::sum_usage`

```rust
let totals = forge_store::sum_usage("results.jsonl").await?;   // streams; missing file = zero
let report = forge_core::compute_cost(totals, forge_core::CostInputs {
    gpu_cost_usd: Some(2000.0),
    online_per_mtok_input: Some(0.50),
    online_per_mtok_output: Some(1.50),
    // Price prefix-cache hits (totals.cached_tokens) at the online API's discounted
    // rate; omit (None) to charge them at the full input rate — conservative.
    online_per_mtok_cached_input: Some(0.05),
});
// report.cached_tokens / report.reasoning_tokens carry the nested-usage breakdown.
# anyhow::Ok(())
```

### Object-store results — `forge_store` (feature `object_store`)

The entry points the CLI's `--out s3://…` path is built on
(`forge-store/src/object_store_backend.rs`):

```rust
// Open a ResultStore at {out}/results/{job} — s3:// gs:// az:// file:// memory://;
// cloud creds/region come from the AWS_*/GOOGLE_*/AZURE_* env vars:
let store = forge_store::objstore_from_out("s3://bucket/run-42/", "job1")?;

// Read a finished run back (discovers the single {job} under {out}/results/;
// zero or multiple jobs is an error):
let run = forge_store::objstore_open_run("s3://bucket/run-42/").await?;
let totals = run.sum_usage().await?;                 // streams the done/ objects
let n = run.dump_emitted_ids("ids.jsonl".as_ref()).await?; // every terminal id, from _manifest/
# anyhow::Ok(())
```

### Spot + agent — `forge_spot`, `forge_agent`

```rust
// Poll-only interruption watch (never provisions; correctness rides on lease expiry):
let notice = forge_spot::watch(&forge_spot::AwsSpot::new(), std::time::Duration::from_secs(5)).await;

// Serve the coordinator protocol over a local queue+store (must run inside a Tokio runtime):
let server = forge_agent::http::serve_coordinator("127.0.0.1:8080", queue, store,
    std::time::Duration::from_secs(120), 4)?;
// … server.addr(), server.base_url(), server.shutdown()
```

The agent side is `forge_agent::run_agent(&AgentConfig, &impl Coordinator, &impl Worker, &Drain)`;
`HttpCoordinator::new(base_url)` is the remote `Coordinator`, `InProcessCoordinator` the local one.
`HttpCoordinator` retries a failed transport `send()` up to 4 times within a 150 s
budget — safe by effect, because posts are lease-fenced and store-deduplicated and a
lost pull just re-leases (`post_json` in `forge-agent/src/http.rs`).
