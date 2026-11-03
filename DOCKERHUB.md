# forge — batch-inference coordinator

Fan a giant JSONL of prompts across N workers — any OpenAI-compatible engine
(vLLM / SGLang / llama.cpp / Ollama / …) or hosted provider endpoint
(OpenAI-compatible or Anthropic-Messages-compatible) — survive kills and spot
interruptions, and prove completeness, as a single static binary. This image **is** that binary: FROM scratch, ~5 MB, no
shell, no libc, nonroot. forge is the orchestration shell only — it never runs
inference and never provisions instances; your engines stay where they are.

Source, docs, benchmarks: <https://github.com/lucheeseng827/forge> (Apache-2.0)

## Where it fits

`forge` is the **coordinator** between the jobs you submit and the engines that
run them: it owns the work queue, sharding, lease/retry, checkpoint, and result
aggregation — and nothing else. It never runs inference and never provisions
GPUs; your engines stay where they are, and the boxes on the right are yours.

```
   CLIENTS                   FORGE                      WORKER FLEET
   (submit a batch)          (this image)               (run the inference)

 ┌──────────────┐
 │ OpenAI SDK   │  /v1/files · /v1/batches ─┐
 │ (serve-batch)│                           │
 └──────────────┘                           │    ┌──────────────┐    ┌──────────────────┐
 ┌──────────────┐                           │    │  forge       │    │ vLLM / SGLang    │
 │ forge CLI    │  run·resume·verify·cost ──┼──▶ │  queue·lease │──▶ │ llama.cpp/Ollama │
 │ (JSONL)      │                           │    │  retry·ckpt  │ ◀──│ (your spot GPUs) │
 └──────────────┘                           │    │  aggregate   │    └──────────────────┘
 ┌──────────────┐        ┌──────────────┐   │    └──────┬───────┘    ┌──────────────────┐
 │ your app     │  ────▶ │  forge-core  │ ◀─┘           │       ───▶ │ …or hosted APIs  │
 │ (embed lib)  │        │  fan-out     │               │            │ OpenAI·Anthropic │
 └──────────────┘        └──────────────┘               │            └──────────────────┘
                                                         │  OpenAI-compatible HTTP
                          results.jsonl + dead-letter  ◀─┤
                          checkpoint state.db (SQLite/redb)  ·  or S3/GCS/Azure results
```

- **Upstream** — whoever submits work: unmodified OpenAI-SDK code against the
  `serve-batch` REST front door (`/v1/files`, `/v1/batches`), the `forge` CLI on a
  JSONL of OpenAI Batch-format requests, or an app embedding `forge-core`. forge
  takes a batch, not a connection.
- **forge** — one static binary: shards the input into a durable queue, leases
  items to workers, retries/dead-letters, checkpoints every result by `custom_id`,
  and aggregates — so a kill or spot-eviction resumes with zero double-work.
- **Downstream** — any OpenAI-compatible engine (vLLM / SGLang / llama.cpp /
  Ollama) on your own GPUs, or a hosted OpenAI/Anthropic-compatible API. Results
  land as a matched JSONL + dead-letter sidecar locally, or in S3/GCS/Azure.

## Tags

| Tag | What |
|---|---|
| `latest`, `v0.2.0` | multi-arch manifest: `linux/amd64` + `linux/arm64` |
| `v0.2.0-amd64`, `v0.2.0-arm64` (and prior versions) | pinned single-arch |

## Quick start — run a batch

```sh
# input.jsonl: one OpenAI Batch-format request per line, keyed by custom_id
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$PWD:/work" -w /work \
  mancube/forge:v0.2.0 \
  run --input input.jsonl \
      --workers http://gpu1:8000,http://gpu2:8000 \
      --engine vllm --concurrency 256 \
      --out results.jsonl --checkpoint state.db
```

Kill it mid-run (or lose the box) and run the same command again — it
**resumes**: the checkpoint is the queue state, already-stored results are never
re-run, and `verify` proves every input id reached a terminal result.

```sh
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work mancube/forge:v0.2.0 \
  status --checkpoint state.db          # counts, success rate, failure mix
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work mancube/forge:v0.2.0 \
  verify --input input.jsonl --results results.jsonl
```

`--user` runs the container as the owner of the directory you mount. The image's
own user (uid `65532`) owns nothing on your machine or a CI runner, so without it
forge cannot create its checkpoint there and stops with
`unable to open database file`. Later images also say which directory is not
writable.

## Quick start — the OpenAI Batch REST front door

```sh
docker run --rm -p 8080:8080 -v forge-data:/data \
  mancube/forge:v0.2.0 \
  serve-batch --listen 0.0.0.0:8080 --data-dir /data \
    --workers http://gpu1:8000 --engine vllm --concurrency 256 \
    --api-key "$FORGE_API_KEY"
```

Unmodified OpenAI SDK code (`files.create` → `batches.create` → poll →
`files.content`) then runs its batch flow against **your own engines** — with
live per-item progress. (Results are retrievable once the batch reaches
`completed` — that is the only state which stamps `output_file_id`, so a
cancelled batch has no output file and there is no mid-run partial fetch
through the OpenAI file contract.) `/v1/health` is
the unauthenticated liveness route; everything else requires the bearer key.

## Kubernetes

The chart is published as an OCI artifact right beside this image:

```sh
helm install forge oci://registry-1.docker.io/mancube/forge-chart --version 0.2.0 \
  --set 'serveBatch.workers={http://vllm-0.engines.svc:8000}' \
  --set 'serveBatch.apiKey=<bearer key>'
```

The same chart ships in the repo
([`deploy/helm/forge`](https://github.com/lucheeseng827/forge/tree/main/deploy/helm/forge)):
`serve-batch` as a Deployment (PVC, probes, Secret-backed key) and one-shot
`forge run` as a Job whose PVC-backed checkpoint turns pod evictions into
resumes. It defaults to this image.

## Notes

- `--concurrency` must match each engine's own limit (vLLM `--max-num-seqs`,
  SGLang `--max-running-requests`, llama-server `--parallel`) — it is the
  ceiling of forge's adaptive (AIMD) limiter, not a wish.
- Measured coordinator footprint: ~10 MB RSS, <1% of one core while saturating
  a multi-node fleet — see
  [BENCHMARKS.md](https://github.com/lucheeseng827/forge/blob/main/BENCHMARKS.md).
- The container needs outbound HTTP(S) to your engines only. Upstream API keys
  (if any) come from the environment; nothing is ever written to the image.
- Checkpoints, results, and uploaded batch files are **plaintext** on the
  mounted volume — apply volume encryption/permissions per your data class.
