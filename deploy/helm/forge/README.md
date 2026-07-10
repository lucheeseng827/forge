# forge Helm chart

Runs the forge **coordinator** on Kubernetes. Engines are BYO — point the chart
at the OpenAI-compatible Services already fronting your vLLM / SGLang /
llama.cpp pods; the chart deliberately deploys none (forge never provisions).

Two shapes, independently toggleable:

| Shape | What | State |
|---|---|---|
| `serveBatch` (default on) | `forge serve-batch` Deployment + Service — the OpenAI Batch REST front door for unmodified SDK clients | data-dir PVC (`Recreate` strategy, 1 replica: the dir is single-owner) |
| `runJob` (default off) | one-shot `forge run` as a Job | checkpoint + results PVC — restarts/evictions **resume**, because a rerun against an existing checkpoint is a resume by construction |

## Quick start

```sh
# from the OCI registry (no checkout needed):
helm install forge oci://registry-1.docker.io/mancube/forge-chart --version 0.1.1 \
  --set 'serveBatch.workers={http://vllm-0.engines.svc:8000}' \
  --set 'serveBatch.apiKey=<key>'

# or from a repo checkout:
helm install forge deploy/helm/forge \
  --set 'serveBatch.workers={http://vllm-0.engines.svc:8000,http://vllm-1.engines.svc:8000}' \
  --set serveBatch.concurrency=256 \
  --set 'serveBatch.apiKey=<bearer key>'
```

The default image is the official multi-arch `docker.io/mancube/forge`
(linux/amd64 + linux/arm64, ~5 MB, FROM scratch). Building your own instead:
`docker build -t <registry>/forge:v0.1.1 .` from the repo root, push it, and
set `image.repository`.

Then any OpenAI SDK pointed at `http://<service>:8080/v1` runs its batch flow
(`files.create` → `batches.create` → poll → `files.content`) against your own
engines, with live per-item progress and mid-run partial results.

## One-shot batch as a Job

```sh
helm install nightly deploy/helm/forge \
  --set serveBatch.enabled=false \
  --set runJob.enabled=true \
  --set 'runJob.workers={http://vllm-0.engines.svc:8000}' \
  --set runJob.input=/input/batch.jsonl \
  --set-json 'runJob.inputVolume={"persistentVolumeClaim":{"claimName":"my-input"}}'
```

Checkpoint and results live on the Job's PVC; `backoffLimit` retries after an
eviction or node loss continue from the checkpoint instead of starting over.

## Notes

- **`serveBatch.concurrency` must match each engine's own knob** (vLLM
  `--max-num-seqs` etc.) — it is the AIMD ceiling, not a wish.
- Resources: the measured coordinator footprint is ~10 MB RSS and <1% of one
  core while saturating a multi-node fleet (see [BENCHMARKS.md](../../../BENCHMARKS.md)),
  so the default requests are tiny and honest.
- No API key = every route open. Fine inside a trusted namespace; set
  `serveBatch.apiKey` before exposing the Service anywhere else, and put TLS
  in front (Ingress/mesh) — forge itself is plain HTTP.
- Spot/preemptible node pools: schedule the *engines* there, keep the
  coordinator (tiny, stateful) on stable capacity; lease-expiry re-queues
  whatever a killed engine node was holding.
