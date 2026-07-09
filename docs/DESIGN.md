# forge — design & event flow

How forge works, exactly as built: the component map, the end-to-end event flow,
the state machine that makes crash/spot recovery structural, and the properties
this design buys that neither closed cloud Batch APIs nor DIY control-plane glue
currently offer. Flags and knobs live in [CONFIG.md](./CONFIG.md), wire contracts
in [API.md](./API.md), operational practice in [OPERATIONS.md](./OPERATIONS.md),
and the measured proofs in [BENCHMARKS.md](../BENCHMARKS.md).

## 1. The problem shape

Offline batch inference is ~5–10× cheaper per token than online serving (higher
GPU utilization), and spot capacity cuts another 60–90% off on-demand pricing —
but capturing both savings today means either:

- **a closed cloud Batch API**: a flat ~50% discount, a 24-hour window you can't
  see inside ("stuck at 0/N in progress"), no choice of model weights, no
  partial results, and results only at the end; or
- **DIY glue**: a heavyweight cluster framework stapled to your inference
  engines, plus hand-rolled checkpointing, retry logic, and spot-interruption
  babysitting — a distributed-systems project bolted onto what is conceptually
  "run this JSONL against these endpoints".

forge is the missing middle: a **single ~4 MB static binary** that does *only*
the orchestration shell — durable work queue, streaming ingest, lease/retry/
checkpoint, spot-drain, backpressure, result aggregation, cost accounting —
while the GPU heavy-lifting stays in the engines (vLLM / SGLang / llama.cpp or
anything OpenAI-compatible) where it belongs.

## 2. Design invariants

Everything below is enforced in the type system or CI, not by convention:

1. **Single homogeneous fan-out.** One job = one flat set of independent items.
   There is no `depends_on`, no fan-in, no successor edge, no topological sort
   anywhere in `forge-core` — arbitrary task graphs are
   [dagron](https://github.com/lucheeseng827/dagron)'s job, and a CI doctrine
   guard keeps it that way.
2. **Single-writer coordinator.** Exactly one process owns the queue; workers
   and agents only *propose* results, fenced by lease generation. No consensus
   protocol, no distributed lock service.
3. **At-least-once execution + idempotent writes = exactly-once *effect*.**
   Never exactly-once execution (that's a lie on top of HTTP); instead every
   result write is keyed by `custom_id`, so a re-execution's write is a no-op.
4. **The checkpoint is not an artifact.** It is the union of state that already
   has to exist: queue rows + the input byte-offset index + the emitted-id
   manifest. There is no snapshot to schedule, corrupt, or forget.
5. **The lean binary stays lean.** Cloud SDKs (`object_store`), the Arrow stack
   (`parquet`), and the rate limiter (`governor`) are off-by-default features;
   CI asserts they are absent from the default build.
6. **BYO everything.** Engines, capacity, and credentials are yours; forge never
   launches, tears down, or autoscales instances — provisioning belongs to your
   provisioner/operator.

## 3. Component map

Solid arrows are the hot path inside the single `forge` binary; dashed arrows
are the optional pieces (the co-located spot agent and the feature-gated result
backends).

```mermaid
flowchart TD
    closed["OpenAI / Anthropic / Bedrock batch file"] -.->|"forge import (normalize once)"| in
    in["input.jsonl<br/>one request per line, keyed by custom_id"]

    subgraph bin["forge — single static binary (run · resume · status · sweep · audit · verify · import · cost · export · serve-batch)"]
      shard["forge-shard<br/>streaming ingest + custom_id validation<br/>opt: --prefix-bucket reorder (bounded-RAM K-way)"]
      core["forge-core · BatchRun<br/>single-writer lease loop + reaper<br/>load-aware dispatch · store-then-ack"]
      worker["forge-worker · HttpWorker<br/>AIMD in-flight semaphore + full-jitter retry<br/>health probe gates dispatch<br/>cooldown learned from Retry-After / x-ratelimit-*"]
      store["forge-store · JsonlStore<br/>idempotent result sink keyed by custom_id"]
      rest["forge-batch · serve-batch<br/>OpenAI Batch REST front door<br/>/v1/files · /v1/batches"]
    end

    q[("forge-queue — .forge/state.db<br/>SQLite WAL (redb behind a feature)<br/>Pending / Leased / Done / DeadLetter")]
    rej["out.reject.jsonl<br/>parse-error sidecar"]
    engines["vLLM / SGLang / llama.cpp<br/>BYO OpenAI-compatible HTTP endpoints"]
    out["results.jsonl<br/>matched by custom_id, order-free"]
    dead["results.jsonl.dead.jsonl<br/>dead-letter file"]
    sdk["unmodified OpenAI SDK client"]

    sdk -.->|"batches.create / retrieve / files.content"| rest
    rest -.-> shard
    in --> shard
    shard -.->|"bad lines preserved"| rej
    shard -->|"idempotent hydrate"| q
    q <-->|"lease / ack / reap / dead-letter"| core
    core --> worker
    worker -->|"POST /v1/chat/completions or /v1/embeddings"| engines
    core -->|"put result THEN ack"| store
    store --> out
    store --> dead

    subgraph spot["spot box (optional, co-located with one engine)"]
      agent["forge-agent (second binary)<br/>watch · run"]
      spotsrc["forge-spot watcher<br/>polls 169.254.169.254 every ~5s<br/>(AWS / GCP / Azure metadata)"]
    end
    spotsrc -->|"Notice → one-way drain latch"| agent
    serve["forge-agent serve<br/>coordinator endpoint<br/>store-then-ack, lease-fenced"]
    agent -.->|"forge-proto HTTP<br/>POST /v1/lease · /v1/result · /v1/interruption"| serve
    serve -.-> q
    serve -.-> store

    store -.->|"object_store feature (write-if-absent)"| obj[("S3 / GCS / Azure")]
    out -.->|"forge export (parquet feature)"| pq["results Parquet"]
```

The crate split *is* the dependency story: `forge-core` is the leaf (types,
the `Queue` / `Worker` / `ResultStore` traits, the fan-out loop) and every other
crate is an interchangeable driver behind those traits — the same loop runs over
SQLite + HTTP + JSONL today and other backends tomorrow, with zero `dyn`
dispatch.

## 4. Event flow — `forge run` end to end

The load-bearing ordering is step 12: the result is checkpointed to the store
**before** the queue ack. That single ordering decision is what turns
at-least-once execution into an exactly-once *effect* (§5).

```mermaid
sequenceDiagram
    autonumber
    participant CLI as forge run (forge-cli)
    participant SH as forge-shard
    participant Q as forge-queue (SQLite WAL)
    participant BR as BatchRun (forge-core)
    participant W as HttpWorker (forge-worker)
    participant E as engine (vLLM / SGLang / llama.cpp)
    participant S as JsonlStore (forge-store)

    CLI->>SH: ingest_jsonl(input.jsonl) — streamed, never whole in RAM
    SH->>Q: enqueue in batches (idempotent ON CONFLICT DO NOTHING)
    CLI->>BR: BatchRun::new(queue, workers, store).run()
    loop until queue drained
        BR->>Q: reap() — expired leases back to Pending
        BR->>W: probe() GET /health — readiness gate + cached load
        BR->>Q: lease(n ≤ Σ concurrency_limit, adaptive TTL)
        Q-->>BR: batch — Pending→Leased, attempts += 1
        BR->>W: submit(item) — least-loaded ready worker, AIMD permit
        Note over W: wait out any active per-worker cooldown before dispatch
        W->>E: POST /v1/chat/completions (body passed through verbatim)
        alt 2xx (and passes the --require content check)
            E-->>W: 200 + usage (incl. cached_tokens / reasoning_tokens)
            W-->>BR: ItemResult + TokenUsage
            BR->>S: put(result) — idempotent by custom_id, BEFORE ack
            BR->>Q: ack(custom_id, lease generation) — Leased→Done
        else retries exhausted (poison item)
            E-->>W: 429/5xx/timeout — arm cooldown from Retry-After / x-ratelimit-*,<br/>full-jitter retry inside submit, then give up
            W-->>BR: Err (RetryPolicy exhausted)
            BR->>S: dead_letter(result) → results.jsonl.dead.jsonl
            BR->>Q: dead_letter — Leased→DeadLetter
        end
    end
    BR-->>CLI: JobTotals — done / dead / aggregate TokenUsage
    Note over Q,S: crash between put and ack? The item stays Leased, the lease expires,<br/>reap() re-queues it, and the re-run's put is a harmless no-op —<br/>at-least-once execution + idempotent writes = exactly-once effect.
```

Two flows sit on top of the same loop:

- **Resume** (`forge resume`): reopen the queue file, expire stale leases, seed
  the store's emitted-id set from the existing output, and run the identical
  loop. Nothing is special-cased; a resumed run *is* a run.
- **Batch REST** (`forge serve-batch`): `/v1/files` upload lands the input
  JSONL, `batches.create` is a job submit, batch status maps **live queue
  counts** (real per-item progress), and result retrieval streams output lines
  — retrievable **mid-run**, not only at completion.

## 5. The item state machine — delivery semantics

```text
                 lease txn (single writer)
   ┌─────────┐  leased_until=now+T, attempts+=1   ┌─────────┐
   │ Pending │ ─────────────────────────────────▶ │ Leased  │
   └─────────┘                                     └────┬────┘
        ▲                                               │
        │ reaper: leased_until < now                    │ result write to store SUCCEEDS
        │ (worker died / spot-killed / restart)         │ THEN ack
        │  ◀────────────────────────────────────────────┤
        │                                               ▼
        │                                          ┌─────────┐
        │                                          │  Done   │
        │                                          └─────────┘
        │ attempts ≥ max_attempts                  ┌──────────────┐
        └─────────────────────────────────────────│ DeadLetter   │
                          retry exhausted          └──────────────┘
```

| Transition | Trigger | Guarantee |
|---|---|---|
| `Pending → Leased` | The coordinator's single-writer lease txn sets `leased_until = now+T`, `attempts += 1`. | Bounded outstanding work = Σ `concurrency_limit`. |
| `Leased → Done` | **Only after** the idempotent result write succeeds, then ack — fenced by lease generation, so a stale worker can never close a re-leased item. | Exactly-once *effect*: a crash between inference and ack leaves the item `Leased`; the lease expires, it re-runs, and the second write is a no-op. |
| `Leased → Pending` | Reaper finds `leased_until < now` (worker died, spot-killed, coordinator restarted). | Loss bounded to the in-flight width — at most the items leased to the dead worker, and zero already-stored results. |
| `Leased → DeadLetter` | `attempts ≥ max_attempts` after full-jitter backoff on 429/5xx/timeout. | A poison item quarantines to the dead-letter file; one bad prompt can never wedge the job. |

Honest caveat, stated rather than hidden: non-deterministic sampling
(`temperature > 0`, no seed) means a re-executed item may produce a *different*
output. forge guarantees every id gets exactly one terminal result — pin seeds
if you need bit-reproducible resume.

**Spot interruption is the same machine, entered politely.** The optional
co-located `forge-agent` runs the `forge-spot` metadata watcher in the VM; an
interruption notice flips a one-way **drain latch** — stop pulling new leases,
let in-flight items finish if the ~30s window allows, leave the rest `Leased`
for lease-expiry re-queue. There is deliberately **no** "flush a big batch on
notice" path: it cannot survive the window, so the design never depends on it.
A missed notice degrades to the crash path above, which is already zero-loss
for stored results.

## 6. The backpressure stack

Five mechanisms compose, each answering a different question:

| Layer | Question it answers | Mechanism |
|---|---|---|
| Per-worker semaphore | "How many requests may be in flight *here*?" | Hard cap = the engine's own declared limit (`--max-num-seqs` etc.). |
| AIMD (always on) | "What is this endpoint's *real* ceiling right now?" | +1 permit per 8 clean 2xx toward the cap; halve on 429/5xx/connection error, the cut paid lazily as permits return. |
| Header cooldown (always on) | "What did the endpoint *tell* us to do?" | `Retry-After` / exhausted `x-ratelimit-*` budgets arm a shared per-worker cooldown, waited out at `submit()` entry so the whole fleet backs off in lockstep. Clamped to 120 s. |
| `governor` (opt-in feature) | "What rate ceiling did the *operator* promise?" | Global GCRA req/s cap per worker — concurrency and rate are different axes. |
| Load-aware dispatch + `--prefix-bucket` | "Which ready worker, and in what order?" | Bias toward the least-loaded engine (read from its own queue-depth metric, degrading to round-robin); optionally reorder input by `(model, system-prompt)` so the engine's automatic prefix cache stays hot and the hits come back as `cached_tokens`. |

## 7. What this design offers that alternatives don't

Capability-by-capability, all of it verifiable in this repo:

- **A single static binary is the whole deployment.** ~4 MB, no Python, no
  cluster runtime, no DB server, no container required. `scp` + run.
- **Kill -9 is a tested path, not an apology.** The measured
  crash → `resume` → zero-loss proof is in [BENCHMARKS.md](../BENCHMARKS.md):
  every id done exactly once, no input⨝output join needed afterward.
- **Resume-readiness is queryable.** `forge audit` reports pending / live-lease
  / orphaned-lease / done / dead and what a resume would reclaim — the answer
  batch operators otherwise reconstruct by hand-joining input against output.
- **Real per-item progress and mid-run partials.** Batch status is live queue
  counts, and results stream out while the run is still going — against a
  closed batch window you get neither.
- **The OpenAI Batch REST contract over your own GPUs.** Unmodified OpenAI SDK
  code (`batches.create` / `retrieve` / `files.content`) runs against
  `forge serve-batch` — with your model weights, your spot economics, and no
  fixed completion window.
- **Exact cost accounting, never estimates.** Every item's real `usage` is
  captured (including `cached_tokens` and `reasoning_tokens`); `forge cost`
  turns it into $/Mtok, tokens-per-dollar, and savings vs a named online
  baseline — and goes honestly negative when a small job doesn't amortize the
  fleet.
- **Completeness you can prove.** `forge verify` shows every input id reached a
  terminal state, at 50M-id scale in bounded RAM, via an exact external
  sort-merge — not a probabilistic filter that can silently vouch for a
  missing id.
- **An endpoint's real throughput is discovered, not tuned.** AIMD + header
  cooldown + load-aware dispatch converge on each engine's actual ceiling; the
  alternative is days of hand-tuning a batch script per fleet shape.
- **Off-ramp included.** `forge import` normalizes OpenAI / Anthropic / Bedrock
  batch files, so leaving a closed batch API is a file copy, not a rewrite.

## 8. Non-goals

Refusals are load-bearing; each keeps the core simple enough to trust:

| Not this | Why not | Where it belongs |
|---|---|---|
| Task graphs, `depends_on`, fan-in, cron | The moment items relate, you need a scheduler — a different product with different failure modes. | [dagron](https://github.com/lucheeseng827/dagron) |
| Provisioning / autoscaling | forge consumes interruption signals and re-queues; acquiring capacity is a solved, separate problem. | your provisioner / operator |
| Running inference in-process | Engines already do continuous batching better than any coordinator could. | vLLM / SGLang / llama.cpp |
| Exactly-once execution | Impossible over HTTP without engine cooperation; claiming it would be dishonest. | exactly-once *effect* via idempotent writes (§5) |
| Wire-level request batching | The engine's continuous batching packs concurrent requests; batching again on the wire only adds latency. | the engine |
