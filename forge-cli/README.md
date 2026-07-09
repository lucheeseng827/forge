# forge-cli — the single static `forge` CLI binary

`forge-cli` builds the `forge` binary: a thin driver over [`forge-core`](../forge-core)
that fans a giant JSONL (OpenAI Batch contract) across N BYO OpenAI-compatible
endpoints — vLLM / SGLang / llama.cpp on (spot) GPUs — and survives crashes via a
durable checkpoint. It is an offline/batch inference coordinator, **not** a
workflow/DAG scheduler: no task graph, no cron, no fleet provisioning. The binary
wires `forge_queue::SqliteQueue`, `forge_worker::HttpWorker`, `forge_store::JsonlStore`,
and `forge_core::BatchRun` behind a `clap` command surface.

## What it does

Subcommands (`forge <cmd>`):

- **`run`** — hydrate the checkpoint queue from `--input` and fan it across
  `--workers` into `--out` (optional `--prefix-bucket` cache-friendly reorder).
- **`resume`** — continue an interrupted run from `--checkpoint` (reuses the output
  path the original `run` recorded).
- **`status`** — queue cardinalities + success rate, retry histogram, and failure
  breakdown; `--json` or `--prometheus` for monitoring.
- **`audit`** — resume-readiness: `reclaimable = pending + orphaned_leases` vs
  genuinely-in-flight leases and who holds them.
- **`sweep`** — re-queue expired (orphaned) leases, then exit.
- **`import`** — normalize an OpenAI / Anthropic / Bedrock batch file into
  forge-native JSONL.
- **`cost`** — the token cost-arbitrage report (forge $/Mtok, tokens/$, $ saved vs
  online) from a results file's real usage.
- **`verify`** — bounded-RAM completeness sweep; exits non-zero if any input
  `custom_id` lacks a terminal result (the CI/acceptance gate).
- **`serve-batch`** — the OpenAI Batch REST front door over the engine (see
  [`forge-batch`](../forge-batch)).
- **`export`** — results JSONL → columnar Parquet (requires `--features parquet`).

The shipped musl binary has all features **off**; `--features parquet` enables
`export`, and `--features object_store` lets `run`/`resume`/`cost`/`verify` use an
`s3://` / `gs://` / `az://` `--out`.

## Quickstart

```sh
# fan an input JSONL across two vLLM endpoints, checkpointing to state.db
forge run --input reqs.jsonl --out out.jsonl --checkpoint state.db \
          --workers http://gpu0:8000,http://gpu1:8000

# inspect progress (human, JSON, or Prometheus)
forge status --checkpoint state.db
forge status --checkpoint state.db --json

# after a crash / spot kill: reclaim orphaned leases and continue
forge sweep  --checkpoint state.db
forge resume --checkpoint state.db --workers http://gpu0:8000

# confirm every input id has a terminal result (non-zero exit if not)
forge verify --input reqs.jsonl --results out.jsonl
```

The core four are `run | resume | status | sweep`; `run` and `resume` share the
`--workers` fleet flags (`--concurrency`, `--lease-secs`, `--max-attempts`,
`--require`, `--engine`, …).

## Config

| Env | Purpose |
|-----|---------|
| `RUST_LOG` | `tracing` subscriber filter (default `info`) |
