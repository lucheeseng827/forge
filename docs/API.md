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

| Worker | `--engine` hint | Auth | Verified | Notes |
|---|---|---|---|---|
| Self-hosted OpenAI-compatible engine (vLLM, SGLang, Ollama, routers, …) | `vllm` / `sglang` / `router` | none (network-protected) | shape | health probe + load-aware dispatch per the hint |
| llama.cpp — **native** `/completion` | `llamacpp` | none (network-protected) | shape | no `usage` object; counts arrive top-level as `tokens_evaluated`/`tokens_predicted` and are mapped into the common accounting (total derived) |
| llama.cpp — OpenAI-compatible route | `llamacpp` | none (network-protected) | shape | stock `usage` object |
| Hosted OpenAI-compatible API | `openai-api` | `FORGE_WORKER_API_KEY` → `Authorization: Bearer` | shape | probe skipped (assumed ready); rate limits learned from response headers |
| Hosted Anthropic-compatible Messages API | `anthropic-api` | `FORGE_WORKER_API_KEY` → `x-api-key` + `anthropic-version` | shape | items carry the Messages body (`url: /v1/messages`); usage (`input_tokens`/`output_tokens`, `cache_read_input_tokens`) maps into the common token accounting; total derived |

**What "Verified" means, exactly.** `shape` = forge is driven against that engine's
documented response shape in
[`forge-worker/tests/engine_compat.rs`](../forge-worker/tests/engine_compat.rs) and
asserted to extract the right content and the right token counts. That is a real
regression guard for the wire contract, and it is all it is.

**No row is yet verified against real hardware.** A recorded shape cannot catch an
engine that changed its response between releases, a gateway that rewrites headers, or
anything about behaviour under load. Publishing a run against real vLLM, real SGLang
and one hosted API — same input, same `custom_id`s, three cost lines — is the
outstanding item; see [`../RUNBOOK_AND_VALUE.md`](../RUNBOOK_AND_VALUE.md) §3 seam 7.
Until that exists, treat this column as "the parser handles this shape", not "this
combination has been run".

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

Every route is **`POST`, JSON in / JSON out**. There are deliberately no task-graph
types on this wire (no `depends_on`, no fan-in): it is a flat lease-proxy protocol.

**Authentication.** When the coordinator has a token (`FORGE_COORDINATOR_TOKEN` for
`forge-agent serve`, `CoordinatorConfig::token` for an embedder), every route requires
`Authorization: Bearer <token>`. It is checked in constant time **before** the body is
read, and any failure is the same `401`. `forge-agent run` sends the same variable.
Without a token the wire is open, so `forge-agent serve` refuses any non-loopback
bind unless it is given `--insecure-no-auth`. The wire hands out raw prompts and
accepts the results that become the record, so an open coordinator lets any host that
can reach it read prompts or forge results.

Common error responses (plain-text body):

| code | when |
|---|---|
| `400` | unreadable or unparseable request body (`bad request body: …`) |
| `401` | a token is configured and the request did not carry it (`WWW-Authenticate: Bearer`) |
| `404` | unknown path (`no route for …`) |
| `405` | non-POST method |
| `413` | request body over 64 MiB |
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
`Authorization: Bearer <api-key>` when a key is set (`FORGE_API_KEY`, or `--api-key`).

| method | path | body / params | returns |
|---|---|---|---|
| `GET` | `/v1/health` | — (no auth) | `{status:"ok", service, build:{version, git_sha, target}, uptime_secs}`. No batch count: that lives behind the key on `/metrics`. |
| `GET` | `/metrics` | bearer | Prometheus text exposition (`text/plain; version=0.0.4`): `forge_batches{status}` / `forge_batch_items{state}` / `forge_files` gauges, plus monotonic `*_total` counters (batches created/completed/cancelled/failed, files uploaded, HTTP requests/errors) so `rate()` works. Behind the key, unlike `/v1/health`: it carries per-status batch and item counts, not just liveness. |
| `POST` | `/v1/files` | raw JSONL body (`purpose=batch`) | File object `{id:"file-…", object:"file", bytes, created_at, filename, purpose}` |
| `GET` | `/v1/files` | `?purpose=batch\|batch_output\|batch_error`, `?after=<file id>`, `?limit=<n>` (all optional) | `{object:"list", data:[File…], first_id, last_id, has_more}`, newest first. See *Lists*. |
| `GET` | `/v1/files/{id}` | — | the File object |
| `GET` | `/v1/files/{id}/content` | — | input file → bytes as-is; **output** file → one OpenAI batch-output line per result (see below) |
| `DELETE` | `/v1/files/{id}` | — | `{id, object:"file", deleted:true}`. Removes the record **and** the bytes. Deleting a batch's output or error file also removes the raw results it was built from, and `/results` stops serving them even if a crash or a failed unlink leaves their bytes behind (the next startup removes those). **`409`** while the file is the input of an `in_progress` batch: a restart re-reads that input to resume the batch. Cancel it or let it finish first. |
| `POST` | `/v1/batches` | `{input_file_id, endpoint, completion_window:"24h"}` | Batch object, `status:"in_progress"` — starts the fan-out across the server's fleet |
| `GET` | `/v1/batches/{id}` | — | Batch object; `request_counts` + `status` read live from the queue; `output_file_id`/`error_file_id` set on completion |
| `GET` | `/v1/batches` | `?after=<batch id>`, `?limit=<n>` (optional) | `{object:"list", data:[Batch…], first_id, last_id, has_more}`, newest first. See *Lists*. |
| `POST` | `/v1/batches/{id}/cancel` | — | Batch object, `status:"cancelling"`. It turns `cancelled` once the run has stopped and its last results have landed, usually within a few hundred ms. A batch that is not `in_progress` is returned unchanged. |
| `GET` | `/v1/batches/{id}/results` | `?kind=output\|error` (default `output`), `?after=<cursor>` | **forge extension**: the result lines written **so far**, mid-run, in the output-line shape below (`application/jsonl`, streamed). See *Reading results mid-run*. |

**Batch object**: `{id:"batch-…", object:"batch", endpoint, input_file_id,
completion_window, status, output_file_id, error_file_id, created_at, in_progress_at,
completed_at, cancelled_at, request_counts:{total, completed, failed}}`. `status` is
`in_progress` until the queue drains, then `completed` (`request_counts` comes straight
from queue counts — never double-tracked). A cancel goes `cancelling` → `cancelled`; a
run that cannot proceed ends `failed`.

**Lists.** `GET /v1/files` and `GET /v1/batches` page the way the OpenAI SDKs iterate
them, so `for f in client.files.list()` ends. `?limit=` sets the page size (none: every
item) and `?after=<id>` starts after that item; `has_more` says whether another page
follows, and `last_id` is the `after` for it. An `after` naming no listed item is a
`400` rather than an empty page, because that item may have been deleted since the
previous page, and an empty page would end the iteration early without saying so.

**Output line** (`/v1/files/{output_id}/content`): `{id:"batch_req_…", custom_id,
response:{status_code, request_id, body}, error:null}` for a done item;
`{…, response:null, error:{…}}` for a dead-lettered one. Same `custom_id` keying as the
CLI results file — map by id, never position.

**Reading results mid-run.** The OpenAI contract only exposes results through
`output_file_id`, which is stamped when the batch **completes**. On a large batch that
means nothing is readable until the last item lands. `GET /v1/batches/{id}/results` is a
forge extension that serves every complete line already written:

- Each response carries **`x-forge-next-after`**. Pass it back as `?after=` to receive
  only lines written since, so a client tails a batch without re-downloading it. The
  results file only ever grows, so a cursor stays valid for the batch's lifetime. A
  value the route did not hand out (one that is not on a line boundary, or is past the
  end) is a `400`.
- **`x-forge-batch-status`** is the batch status read *before* the snapshot. **Stop when
  it is terminal (`completed`, `cancelled` or `failed`) and a call returns no lines.**
  At that point you have every result. `cancelling` is not terminal: the run may still
  be writing.
- One response covers at most **16 MiB** of results. Past that, keep calling with the
  new cursor, as for a running batch.
- Only whole lines are served. The snapshot ends at the last newline, so a result being
  written at that moment is never cut in half.
- Line `id`s (`batch_req_…`) are minted per read. Join on `custom_id`.
- It works in **every** status. A `cancelled` or `failed` batch never gets an
  `output_file_id`, so this is the only route to the results it did finish.

```sh
cursor=0
while :; do
  curl -sS -D h.txt -H "Authorization: Bearer $FORGE_API_KEY" \
    "http://localhost:8080/v1/batches/$BATCH/results?after=$cursor" >> partial.jsonl
  cursor=$(awk 'tolower($1)=="x-forge-next-after:"{print $2}' h.txt | tr -d '\r')
  # …stop once x-forge-batch-status is completed/cancelled/failed and nothing new came back
  sleep 5
done
```

**Honest MVP limits** (see the `forge-batch` rustdoc): `POST /v1/files` takes the raw
JSONL body, **not** multipart — the SDK's `files.create` (multipart) needs a raw POST
shim; every other SDK call is native. No `metadata`. A `serve-batch` **restart re-lists batches** from disk,
re-finalizes any that drained while it was down, **and restarts the fan-out for any left
in flight** — the hydrate, the queue and the store are all idempotent, so a resumed batch
completes every item exactly once. SIGTERM (not just Ctrl-C) shuts the server down
gracefully, cancelling and joining each running batch so its `ckpt.db` is left resumable.

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
