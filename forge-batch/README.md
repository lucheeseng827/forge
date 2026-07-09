# forge-batch — an OpenAI Batch REST front door over the forge engine

`forge-batch` is a real **OpenAI Batch REST API** (`/v1/files`, `/v1/batches`)
served over the forge engine. Point unmodified OpenAI SDK code
(`client.files...`, `client.batches.create(...)`) at it and a giant JSONL fans out
across the worker fleet forge was configured with, surviving crashes exactly like
`forge run`. It is a thin HTTP skin — the same deliberately-minimal `tiny_http`
server the rest of the system uses — that *composes* the existing crates. It adds
**no** task graph, DAG, or fan-in: a batch is one homogeneous fan-out of
independent items.

## What it does

- **`serve_batch(BatchConfig)` → `BatchServer`** — stands up the front door on a set
  of accept threads; `shutdown` stops + joins them and every per-batch supervisor.
- **Composes the stack per batch** — `forge_shard::ingest_jsonl_with_rejects`
  hydrates a per-batch `forge_queue::SqliteQueue`; `forge_core::BatchRun` fans it
  across the fleet into a per-batch `forge_store::JsonlStore`; the **queue counts
  are the progress state** (it reads them, never tracks a second copy).
- **`WorkerFleet` trait + `HttpFleet`** — a fresh `Vec<HttpWorker>` (own keep-alive
  client + AIMD limiter) is built per batch, carrying the same retry + response
  validation as `forge run`. Tests wire an in-process mock through the same seam.
- **Persistence / restart** — each batch persists `batch.json`, the file registry
  persists `files.json`; both re-load on startup so batches re-list and results stay
  fetchable. A batch left `in_progress` when the process died is finalized to
  `completed` on read once its queue is drained (its `ckpt.db` also allows
  `forge resume`).
- **Bearer auth** — enforced on every route except `/v1/health` **iff** an `api_key`
  was configured (constant-time compared); with no key the server is open.
- **`openai` module** — the wire types (`BatchObject`, `FileObject`,
  `CreateBatchRequest`, `RequestCounts`, `OutputLine`, `ListEnvelope`) and result
  reshaping to OpenAI batch-output lines.

### Routes

| method | path | purpose | auth |
|---|---|---|---|
| `GET`  | `/v1/health` | liveness + batch count | none |
| `POST` | `/v1/files` | upload a batch input JSONL → File object | bearer |
| `GET`  | `/v1/files/{id}` | retrieve a File object | bearer |
| `GET`  | `/v1/files/{id}/content` | stream file bytes (outputs reshaped) | bearer |
| `POST` | `/v1/batches` | create + start a batch | bearer |
| `GET`  | `/v1/batches` | list all batches | bearer |
| `GET`  | `/v1/batches/{id}` | retrieve a batch (status from queue counts) | bearer |
| `POST` | `/v1/batches/{id}/cancel` | cancel a batch | bearer |

## Quickstart

`serve_batch` must be called from within a Tokio runtime (its blocking accept
threads bridge into the async engine via the current runtime handle):

```rust
use forge_batch::{serve_batch, BatchConfig, HttpFleet};

let server = serve_batch(BatchConfig {
    bind: "127.0.0.1:8080".into(),
    data_dir: "./data".into(),
    api_key: None,                 // Some(key) → require `Authorization: Bearer <key>`
    fleet: HttpFleet::new(specs, retry, validation),
    run_config,                    // forge_core::RunConfig (lease TTLs, retry, load-aware)
    threads: 2,
})?;
// point your OpenAI SDK's base_url at server.base_url()
```

In practice this is driven by the CLI: `forge serve-batch --listen … --data-dir …
--workers …` (see [`forge-cli`](../forge-cli)).
