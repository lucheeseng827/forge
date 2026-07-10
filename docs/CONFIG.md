# forge — configuration reference

Every knob, in one place. forge is configured **entirely by CLI flags** — there is no
config file, and the only environment variable the **default** binaries read is
`RUST_LOG` (an `--features object_store` build additionally reads cloud credentials
from the environment — see the table below).
Source of truth: the clap derives in
[`forge-cli/src/main.rs`](../forge-cli/src/main.rs) and
[`forge-agent/src/main.rs`](../forge-agent/src/main.rs); regenerate these tables when
those change.

- [Environment variables](#environment-variables)
- [`forge` CLI](#forge-cli) — `run` / `resume` / `status` / `audit` / `sweep` / `import` / `cost` / `verify` / `export` / `serve-batch`
- [`forge-agent` CLI](#forge-agent-cli) — `watch` / `run` / `serve`
- [Cargo features](#cargo-features)
- [Library defaults](#library-defaults-embedders) (for embedders of `forge-core`)
- [State & data formats](#state--data-formats)

## Environment variables

| var | type | default | what it does | when to change |
|---|---|---|---|---|
| `RUST_LOG` | tracing `EnvFilter` | `info` | Log level/filter for both `forge` and `forge-agent` (e.g. `RUST_LOG=forge_core=debug`). | Debugging dispatch/lease behavior. |
| `AWS_*` / `GOOGLE_*` / `AZURE_*` | strings | — | **`--features object_store` builds only**, and only when `--out`/`--results` is a cloud URL: every env var with one of these three prefixes is forwarded to the object_store builders (its `from_env` conventions — credentials, region, endpoint). `file://`/`memory://` URLs get none forwarded. Source: `env_opts`/`scheme_env_opts` in `forge-store/src/object_store_backend.rs`. | Pointing `--out` at S3/GCS/Azure. |

| `FORGE_WORKER_API_KEY` | string | — | Optional bearer/API key for **hosted** worker endpoints (`--engine openai-api` → `Authorization: Bearer`; `--engine anthropic-api` → `x-api-key` + `anthropic-version`). Env-only by design — never a flag, never logged. Unset (the default) sends no auth headers at all. | Driving a hosted provider API instead of (or alongside) self-hosted engines. |

That is the whole list — the default binary deliberately reads no env var but
`RUST_LOG` and (opt-in) `FORGE_WORKER_API_KEY`; with the key unset it sends **no**
auth headers to workers (see [OPERATIONS §security](./OPERATIONS.md#security-posture)).

## `forge` CLI

Source: `Cli`/`Cmd` + the per-subcommand `*Args` structs in
[`forge-cli/src/main.rs`](../forge-cli/src/main.rs).

### Worker/dispatch flags (shared by `run` and `resume`)

Source: `struct WorkerArgs`.

| flag | type | default | what it does | when to change |
|---|---|---|---|---|
| `--workers` | comma-separated URLs | **required** | The BYO OpenAI-compatible base URLs to fan out across. Workers are named `w0`, `w1`, … in URL order. | Always set. |
| `--engine` | `vllm` \| `sglang` \| `llamacpp` \| `router` \| `openai-api` \| `anthropic-api` | `vllm` | Engine hint — affects the health-probe/load-metric shape and (for the two hosted hints) the auth-header style, never the per-item wire contract. Self-hosted hints read a queue-depth metric (vLLM `/metrics`, SGLang `/get_server_info`, llama.cpp `/slots`; `router` reads none); hosted hints skip the probe (no `/health` route to ask) and authenticate with `FORGE_WORKER_API_KEY`. | Match your engine so load-aware dispatch has a metric; `router` for a self-balancing front; the `*-api` hints for hosted provider endpoints. |
| `--endpoint` | `chat` \| `completions` \| `embeddings` \| `messages` | `chat` | Default endpoint for items whose `url` is empty; also the fallback kind for `--require` validation. Items with their own `url` override it per line (`/v1/messages` items validate + meter in the Anthropic Messages shape). | Embedding, legacy-completions, or Anthropic-Messages batches. |
| `--concurrency` | usize | `256` (`resume`: the recorded value) | Per-worker in-flight cap = the **ceiling** of the AIMD adaptive limiter. Set it to the engine's own knob (vLLM `--max-num-seqs`, SGLang `--max-running-requests`, llama-server `--parallel`). `run` records it in the checkpoint; a bare `resume` reuses the recorded value (an explicit flag overrides, with a warning on divergence), so a resume can never silently restart AIMD at a ceiling the fleet was not configured for. | Always match the engine; too high risks engine OOM (AIMD backs off but starts at the ceiling). |
| `--lease-secs` | u64 (seconds) | `300` | Visibility-timeout **floor**: how long a leased item stays invisible before the reaper re-queues it. | Lower for fast items (quicker crash recovery); raise if items legitimately run long. |
| `--lease-max-secs` | u64 (seconds) | `1800` | **Cap** on the adaptive lease TTL. The effective TTL grows from `--lease-secs` toward this as the run observes real item latency (2× peak observed latency, clamped). Values below `--lease-secs` are treated as `--lease-secs`. | Bound how long a dead worker's items stay stuck leased. |
| `--ready-grace-secs` | u64 (seconds) | `60` | How long to wait for ≥1 worker to pass its health probe before erroring out (`no worker became ready within …`). | Slow-booting engines (model load can take minutes). |
| `--max-attempts` | u32 ≥ 1 | `5` | Attempts per item before dead-lettering (1 = no retry). Applied inside the worker's retry loop; `0` is rejected by clap. | Lower for expensive items; raise for flaky networks. |
| `--require` | `any` \| `nonempty` \| `json` | `any` | Content check on every 2xx body. A failing body is a **soft failure**: retried like a 5xx, then dead-lettered with `validation failed: …` — never silently emitted. `json` requires the model's output text to parse as JSON (embeddings degrade to `nonempty`). Applied per *item* endpoint. | Structured-output jobs. |
| `--no-load-aware` | bool flag | off | Forces flat round-robin dispatch. Load-aware bias (fill the least-loaded engine first, from the cached probe-time queue-depth metric) is on by default and is a true no-op when no engine exposes a metric. | Debugging, or when the engine metric is misleading. |

### `forge run`

| flag | type | default | what it does |
|---|---|---|---|
| `--input` | path | **required** | Input JSONL (OpenAI Batch contract, one request per line keyed by `custom_id`). Malformed lines go to the reject sidecar (`<out>.reject.jsonl`, or `<checkpoint>.reject.jsonl` when `--out` is an object-store URL), never abort the job. |
| `--out` | path **or object-store URL** | **required** | Local path: results JSONL (keyed by `custom_id`, order **not** guaranteed), dead-letters to `<out>.dead.jsonl`. `scheme://` URL (`s3://`/`gs://`/`az://`/`file://`/`memory://`, **needs `--features object_store`** — the default binary fails fast, before ingest): each terminal result is its own object under `<url>/results/<job>/done\|dead/<custom_id>.json`, where `<job>` is the sanitized checkpoint file stem (so distinct runs sharing one `--out` root need distinct checkpoint stems). Either way the value is recorded in the checkpoint so `resume` reuses it. Source: `cmd_run`/`is_object_url`/`object_job_id` in `forge-cli/src/main.rs`. |
| `--checkpoint` | path | **required** | The SQLite queue/state DB (see [State & data formats](#state--data-formats)). Created if absent. |
| `--prefix-bucket` | bool flag | off | Reorder the input by shared prompt prefix `(model, system-prompt)` before hydrating, so prefix-similar items are contiguous and the engine's automatic prefix cache (vLLM/SGLang) stays hot across a run of them. Bounded-RAM preprocessing pass (256 temp buckets beside the checkpoint, concatenated in order; only the bucket writers are open, never the corpus). Opt-in; measured ~50% wall-clock elsewhere on prefix-heavy corpora (Daft; vLLM #24320). Source: `forge_shard::bucket::bucket_jsonl`. |
| *(worker flags)* | | | All of [Worker/dispatch flags](#workerdispatch-flags-shared-by-run-and-resume). |

### `forge resume`

| flag | type | default | what it does |
|---|---|---|---|
| `--checkpoint` | path | **required** | The checkpoint of the interrupted run. |
| `--concurrency` | usize | recorded value | Optional. `resume` reuses the per-worker concurrency the original `run` recorded in the checkpoint; an explicit flag overrides it (warned when diverging). Checkpoints from older builds have no recorded value — pass the flag, or the 256 default is used with a loud warning. |
| `--out` | path | recorded path | Optional. `resume` normally reuses the output path (or object-store URL) the original `run` recorded; a diverging `--out` is **ignored with a warning**. Only needed for old checkpoints that predate recorded job metadata (otherwise resume errors with `checkpoint has no recorded output path`). Resuming a run whose recorded `--out` is an object-store URL needs an `object_store` build, same as `run`. |
| *(worker flags)* | | | Same as `run` — the fleet may differ from the original run's. |

### `forge status`

| flag | type | default | what it does |
|---|---|---|---|
| `--checkpoint` | path | **required** | Which run to report on. |
| `--json` | bool flag | off | One machine-readable JSON line (`pending/leased/done/dead/total/success_rate/retried/attempts_histogram/failure_reasons`). |
| `--prometheus` | bool flag | off | Prometheus text-exposition metrics (`forge_items`, `forge_failures`, `forge_retried`, `forge_success_rate`) for a textfile collector / scrape. Takes precedence over `--json`. |

### `forge audit`

| flag | type | default | what it does |
|---|---|---|---|
| `--checkpoint` | path | **required** | Which run to audit. |
| `--json` | bool flag | off | One machine-readable JSON line (`pending/live_leases/orphaned_leases/done/dead/total/reclaimable/interrupted/live_holders`). |

Resume-readiness view: splits `leased` into **live** (a worker holds an unexpired
lease — actively in flight) vs **orphaned** (expired lease from a dead/spot-killed
worker), reports `reclaimable = pending + orphaned` (exactly what `resume`
re-dispatches, join-free), `interrupted` (`orphaned > 0`), and which workers hold
live leases. The alert condition for "a worker died, run `resume`" is
`interrupted: true`. Source: `forge-cli/src/main.rs` `cmd_audit`,
`forge-queue/src/sqlite.rs` `resume_audit`.

### `forge sweep`

| flag | type | default | what it does |
|---|---|---|---|
| `--checkpoint` | path | **required** | Runs one reap pass: re-queues leased items whose visibility timeout expired, prints `re-queued N expired lease(s)`, exits. (The run loop also reaps continuously; `sweep` is for a run that is not currently executing.) |

### `forge import`

| flag | type | default | what it does |
|---|---|---|---|
| `--input` | path | **required** | A closed-Batch-API JSONL: OpenAI (`body`), Anthropic (`params`), or Bedrock (`recordId`+`modelInput`). |
| `--out` | path | **required** | Normalized forge-native JSONL. Unusable/duplicate lines go to `<out>.reject.jsonl`. |
| `--endpoint` | `chat` \| `completions` \| `embeddings` | `chat` | Default for URL inference when a line has no `url` and the body shape is ambiguous. |

### `forge cost`

| flag | type | default | what it does |
|---|---|---|---|
| `--results` | path or object-store URL | **required** | Results written by `forge run`: a local JSONL, or the URL you passed as `--out` (`s3://…`, **needs `--features object_store`**) — usage is then summed straight from the `done/` result objects (one object at a time; dead-letters carry no usage). Streamed, bounded RAM either way. |
| `--gpu-cost` | f64 USD | — | Total GPU spend for the run. Takes precedence over the per-hour form. |
| `--gpu-usd-per-hour` | f64 | — | Spot $/GPU-hour; used with `--gpu-hours` when `--gpu-cost` is absent. |
| `--gpu-hours` | f64 | — | Wall-clock GPU-hours. |
| `--gpus` | f64 | `1.0` | Multiplies the per-hour cost. |
| `--online-per-mtok-input` | f64 | — | Online-API baseline $/1M input tokens (you name the price; forge never guesses). |
| `--online-per-mtok-output` | f64 | input price | Online-API baseline $/1M output tokens. |
| `--online-per-mtok-cached-input` | f64 | input price | Online-API baseline $/1M **cached input** tokens (providers bill cache hits at a discount, often ~10–25% of the input rate). When set, the baseline prices captured `cached_tokens` at this rate and the rest of the prompt at the full input rate — an honest apples-to-apples comparison against a prefix-caching online API. Omit and cached tokens price at the full input rate (conservative; never overstates savings). Cached/reasoning token counts come from the engine's `usage.prompt_tokens_details`/`completion_tokens_details` and are always printed in the breakdown. |
| `--json` | bool flag | off | Machine-readable report. |

### `forge verify`

| flag | type | default | what it does |
|---|---|---|---|
| `--input` | path | **required** | The input JSONL (the `custom_id` universe). |
| `--results` | path or object-store URL | **required** | The results JSONL — its `<results>.dead.jsonl` sibling is read automatically (a dead-lettered id counts as terminal) — or the URL you passed as `--out` (`s3://…`, **needs `--features object_store`**): every terminal id (done **and** dead) is then read from the run's `_manifest/`, no dead sidecar involved. |
| `--missing-out` | path | `<results>.missing.txt` (`<input>.missing.txt` for object-store results) | Where missing ids are written (only created if something is missing). Always a local file — for object-store results there is no local results file to sit beside, so it defaults next to the input. |
| `--no-missing-out` | bool flag | off | Report counts only, no sidecar. |
| `--run-capacity` | usize | `1000000` | Ids buffered in RAM before spilling a sorted run. The sweep is **exact** regardless (external sort-merge, not a Bloom filter); this only trades RAM for temp files. |
| `--json` | bool flag | off | Machine-readable report. Exit is non-zero exactly when an id is missing (the CI gate). |

### `forge export` *(only in a `--features parquet` build)*

| flag | type | default | what it does |
|---|---|---|---|
| `--results` | path | **required** | Results JSONL to convert. |
| `--out` | path | **required** | Output Parquet file (one row per `custom_id`). |
| `--row-group-rows` | usize | `50000` | Rows buffered per row group (bounds RAM at 50M scale). |

### `forge serve-batch`

Serves the **OpenAI Batch REST API** (`/v1/files`, `/v1/batches`) over the existing
engine, so unmodified OpenAI-SDK code pointed at forge runs its batch flow. Source:
`ServeBatchArgs`/`cmd_serve_batch` in `forge-cli/src/main.rs`, `forge-batch` crate.

| flag | type | default | what it does |
|---|---|---|---|
| `--listen` | `host:port` | `127.0.0.1:8080` | Bind address for the REST server. |
| `--data-dir` | path | **required** | Root for uploaded files (`files/`) + per-batch checkpoints & results (`batches/<id>/`). Enough persists here that a restart re-lists batches (an in-flight run task does **not** survive restart — its `ckpt.db` is intact for `forge resume`). |
| `--api-key` | string | *(none)* | Optional bearer token. When set, every route except `/v1/health` requires `Authorization: Bearer <token>`; unset = open (trust the network). |
| `--threads` | usize | `2` | HTTP accept threads sharing the listener. |
| *(worker flags)* | | | The [Worker/dispatch flags](#workerdispatch-flags-shared-by-run-and-resume) — the fleet every batch fans out across. |

See [API.md §Batch REST API](./API.md#4-batch-rest-api-openai-compatible) for the routes and the OpenAI object shapes.

## `forge-agent` CLI

Source: [`forge-agent/src/main.rs`](../forge-agent/src/main.rs). Optional
binary; the plain `forge` CLI never needs it.

### `forge-agent watch` — in-VM spot-interruption detector

| flag | type | default | what it does |
|---|---|---|---|
| `--cloud` | `aws` \| `gcp` \| `azure` | `aws` | Which link-local metadata service to poll (AWS IMDSv2 `spot/instance-action` + rebalance; GCP/Azure equivalents). The process **exits when a notice arms** — hook your graceful stop on that. |
| `--interval-secs` | u64 | `5` | Poll interval. Design to the smallest window: AWS ~120s, **GCP/Azure ~30s**. |
| `--worker-id` | string | `agent` | Stamped on the advisory notice. |

### `forge-agent run` — lease-proxy agent on the spot box

| flag | type | default | what it does |
|---|---|---|---|
| `--coordinator` | URL | **required** | Remote coordinator base URL (a `forge-agent serve`). |
| `--engine` | URL | **required** | The local OpenAI-compatible endpoint this box fronts. |
| `--worker-id` | string | `agent` | Lease owner / fencing key. |
| `--concurrency` | u32 ≥ 1 | `64` | In-flight cap = the engine's own cap; also sizes each lease pull. `0` is rejected. |
| `--endpoint` | `chat` \| `completions` \| `embeddings` | `chat` | Endpoint kind for items without their own path. |
| `--cloud` | `aws` \| `gcp` \| `azure` \| `none` | `none` | Spot watcher; a notice flips the one-way drain latch (stop pulling leases, finish in-flight if the window allows). `none` disables the in-VM drain. |
| `--lease-secs` | u64 | `120` | Lease visibility timeout requested from the coordinator. |
| `--interval-secs` | u64 | `5` | Spot-watch poll interval. |

### `forge-agent serve` — the coordinator side of the wire protocol

| flag | type | default | what it does |
|---|---|---|---|
| `--queue` | path | **required** | The SQLite queue DB (hydrate it separately, e.g. via `forge run`'s ingest). |
| `--out` | path | **required** | Results JSONL the coordinator writes (store-then-ack; single writer). |
| `--bind` | addr | `127.0.0.1:8080` | Bind address. **Loopback by default and unauthenticated** — a public bind would let any reachable host pull prompts or forge results. Widen only behind your own network controls. |
| `--lease-secs` | u64 | `120` | Default lease TTL when a pull doesn't ask (`lease_secs: 0`). |
| `--threads` | usize | `4` | Accept threads (blocking `tiny_http`; the queue's atomic lease keeps concurrent pulls disjoint). |

## Cargo features

All off by default so the static-musl `forge-cli` binary stays lean — CI asserts the
heavy stacks are absent from it. Source: the `[features]` tables in each crate's
`Cargo.toml`.

| feature | crate | what it adds |
|---|---|---|
| `parquet` | `forge-store` / `forge-cli` | `forge export` + `jsonl_to_parquet` (arrow/parquet stack, Snappy-only so it still builds musl-static). |
| `object_store` | `forge-store` / `forge-cli` | `ObjStore`: a `ResultStore` over S3/GCS/Azure/local with conditional-create idempotent writes + a `_manifest/` for O(emitted) resume. On `forge-cli` it wires the CLI: `forge run --out s3://…` (and `resume` of such a run), plus `forge cost`/`forge verify` reading `--results` from the same URL. |
| `governor` | `forge-worker` | `WorkerSpec::rate_limit(req_per_sec)` / `HttpWorker::with_rate_limit` — a global GCRA req/sec ceiling per worker, orthogonal to AIMD (which caps in-flight concurrency, not rate). |
| `redb` | `forge-queue` | The pure-Rust `redb` queue backend — zero C toolchain for embedders; the CLI ships the bundled-SQLite backend. |

## Library defaults (for embedders of `forge-core`)

Values an embedder inherits unless overridden; the CLI overrides the first block from
its flags. Sources cited per row.

| knob | default | source |
|---|---|---|
| `RunConfig.lease_for` / `lease_max` | 300 s / 1800 s | `forge-core/src/run.rs` `impl Default for RunConfig` |
| `RunConfig.lease_batch` | 256 items per queue round-trip | same |
| `RunConfig.poll_interval` | 250 ms | same |
| `RunConfig.ready_grace` | 60 s | same |
| `RunConfig.load_aware` | `true` | same |
| `RunConfig.retry` | **advisory only** — the worker owns the retry loop; set the policy on each worker (`HttpWorker::with_retry`) for it to take effect | `RunConfig.retry` doc comment |
| `RetryPolicy` | 5 attempts, full-jitter backoff `rand(0, min(30 s, 500 ms · 2^attempt))` | `forge-core/src/retry.rs` |
| Retryable statuses | 429, 408, any 5xx (plus transport errors/timeouts); other 4xx are terminal, no retry | `forge-worker/src/lib.rs` `is_retryable_status` |
| `WorkerSpec` defaults | `concurrency_limit = 256`, `engine_hint = vllm`, `health_path = "/health"`, no rate limit | `forge-core/src/types.rs` `WorkerSpec::new` |
| `HttpWorker` HTTP timeouts | connect 10 s, request 600 s (batch generations can run minutes) | `forge-worker/src/lib.rs` `HttpWorker::new` |
| Load-metric fetch timeout | 2 s (best-effort; failure → load unknown → round-robin) | `forge-worker/src/lib.rs` `fetch_load` |
| AIMD | additive +1 permit per 8 consecutive 2xx, multiplicative halve on 429/5xx/connection error, bounded `[1, concurrency_limit]`, always on | `forge-worker/src/lib.rs` `AimdLimit`, `AIMD_INCREASE_AFTER` |
| Adaptive lease growth | effective TTL = clamp(2 × peak successful-item latency, base, max) | `forge-core/src/run.rs` `adaptive_lease`, `LEASE_LATENCY_FACTOR` |
| `HttpCoordinator` timeouts | connect 10 s, request 120 s | `forge-agent/src/http.rs` |
| `HttpCoordinator` transport retry | a failed `send()` is retried up to 4 times (linear 50 ms × attempt backoff) within a 150 s total budget — safe *by effect*: result posts are lease-fenced + store-deduplicated, a lost pull just re-leases | `forge-agent/src/http.rs` `post_json`, `RETRY_BUDGET` |
| Agent loop (`forge-agent run`) | `poll_idle` 1 s, `max_idle_polls` 0 (daemon: runs until drained) | `forge-agent/src/main.rs` |

## State & data formats

What is on disk and what compatibility is promised — the reference summary.

| artifact | format | writer | compatibility notes |
|---|---|---|---|
| `--checkpoint` (e.g. `.forge/state.db`) | SQLite, WAL mode (`synchronous=NORMAL`, `busy_timeout=60000`) — tables `items` (the queue: `custom_id` PK, state `Pending/Leased/Done/DeadLetter`, attempts, lease), `job` (recorded input/output paths), `shards` (append-only **ingest seek checkpoints**: one `byte_offset_start/end` + `line_start/end` + `lines_hydrated` row per committed hydrate batch; a re-run of an interrupted ingest seeks to the max `byte_offset_end` instead of rescanning, and an offset past the current input's EOF triggers a full rescan) | the single coordinator process | Schema is `CREATE TABLE IF NOT EXISTS` (additive); there is **no `format_version` column yet** — treat a checkpoint as owned by the forge version that created it and finish a run before upgrading. The seek checkpoint assumes the input file is append-only (see [OPERATIONS §troubleshooting](./OPERATIONS.md#troubleshooting-symptom-first)). Source: `forge-queue/src/sqlite.rs` `SCHEMA`, `record_shard`/`hydrated_through`; `forge-shard/src/lib.rs` `ingest_jsonl_with_rejects`. |
| `<out>` results JSONL | one `{custom_id, response:{status_code, body}, usage, worker_id, latency_ms, attempt, completed_at}` per line, append-only, order-independent | coordinator (store-then-ack) | The stable contract is: keyed by `custom_id`, terminal exactly once across results+dead files. Map by id, never by position. |
| `<out>.dead.jsonl` | same shape with `error:{code, message}` instead of `response` | coordinator | Dead-letter sibling; read automatically by `forge verify`. |
| `<out>.reject.jsonl` | `{line_no, error, input}` per rejected input line | `run`/`import` ingest | Repair + re-ingest; never blocks the job. When `--out` is an object-store URL the sidecar is `<checkpoint>.reject.jsonl` instead (there is no local results file to sit next to). |
| `<results>.missing.txt` | one missing `custom_id` per line | `forge verify` | Only created when incomplete. Defaults to `<input>.missing.txt` for object-store results. |
| object-store results (`--out s3://…`, feature `object_store`) | one immutable JSON object per terminal result under `<out>/results/<job>/done\|dead/<custom_id>.json` (same `ItemResult` shape as the JSONL lines), plus a zero-byte `_manifest/<custom_id>` marker per terminal id | coordinator (conditional `PutMode::Create` = write-if-absent, idempotent) | `forge cost`/`forge verify` read back the **same `--out` URL** and expect exactly **one** `<job>` under `<out>/results/` (zero or several is an error). `<job>` = sanitized checkpoint file stem. Source: `forge-store/src/object_store_backend.rs`. |
| forge-proto wire | HTTP/1.1 + JSON, three POST routes (see [API.md](./API.md)) | — | Older peers tolerated: `usage`/`latency_ms`/`attempts` default when omitted (`#[serde(default)]`, `forge-proto/src/lib.rs`). No versioned envelope yet. |
