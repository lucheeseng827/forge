# forge — batch-inference coordinator

Fan a giant JSONL of prompts across N OpenAI-compatible engines (vLLM / SGLang /
llama.cpp), survive kills and spot interruptions, and prove completeness — as a
single static binary. This image **is** that binary: FROM scratch, ~5 MB, no
shell, no libc, nonroot. forge is the orchestration shell only — it never runs
inference and never provisions instances; your engines stay where they are.

Source, docs, benchmarks: <https://github.com/lucheeseng827/forge> (Apache-2.0)

## Tags

| Tag | What |
|---|---|
| `latest`, `v0.1.0` | multi-arch manifest: `linux/amd64` + `linux/arm64` |
| `v0.1.0-amd64`, `v0.1.0-arm64` | pinned single-arch |

## Quick start — run a batch

```sh
# input.jsonl: one OpenAI Batch-format request per line, keyed by custom_id
docker run --rm --network host \
  -v "$PWD:/work" -w /work \
  mancube/forge:v0.1.0 \
  run --input input.jsonl \
      --workers http://gpu1:8000,http://gpu2:8000 \
      --engine vllm --concurrency 256 \
      --out results.jsonl --checkpoint state.db
```

Kill it mid-run (or lose the box) and run the same command again — it
**resumes**: the checkpoint is the queue state, already-stored results are never
re-run, and `verify` proves every input id reached a terminal result.

```sh
docker run --rm -v "$PWD:/work" -w /work mancube/forge:v0.1.0 \
  status --checkpoint state.db          # counts, success rate, failure mix
docker run --rm -v "$PWD:/work" -w /work mancube/forge:v0.1.0 \
  verify --input input.jsonl --results results.jsonl
```

The mounted directory must be writable by uid `65532` (the image's nonroot
user), or add `--user "$(id -u)"`.

## Quick start — the OpenAI Batch REST front door

```sh
docker run --rm -p 8080:8080 -v forge-data:/data \
  mancube/forge:v0.1.0 \
  serve-batch --listen 0.0.0.0:8080 --data-dir /data \
    --workers http://gpu1:8000 --engine vllm --concurrency 256 \
    --api-key "$FORGE_API_KEY"
```

Unmodified OpenAI SDK code (`files.create` → `batches.create` → poll →
`files.content`) then runs its batch flow against **your own engines** — with
live per-item progress and partial results retrievable mid-run. `/v1/health` is
the unauthenticated liveness route; everything else requires the bearer key.

## Kubernetes

A Helm chart ships in the repo
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
