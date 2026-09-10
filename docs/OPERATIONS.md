# forge — operations runbook

How to deploy, back up, upgrade, monitor, and debug a forge run. Grounded in the
code as built; configuration details live in [CONFIG.md](./CONFIG.md), endpoint
details in [API.md](./API.md), the architecture + event flow in
[DESIGN.md](./DESIGN.md).

## Deploy

forge is a single static binary plus (optionally) `forge-agent` on each spot box.
There is no daemon, no DB server, no cluster membership: a "deployment" is *one
coordinator process per run* next to its checkpoint file.

```sh
# Build the static binary (Linux; ~4 MB, no shared libs — see README "Build a static binary"):
CC_x86_64_unknown_linux_musl=musl-gcc \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
  cargo build -p forge-cli --release --target x86_64-unknown-linux-musl

# Run under any supervisor (systemd-run, tmux, nohup). One coordinator per checkpoint:
forge run --input prompts.jsonl --workers http://gpu1:8000,http://gpu2:8000 \
  --out results.jsonl --checkpoint .forge/state.db
```

Tagged releases are built and published as static binaries by CI.

**Kubernetes:** [`deploy/helm/forge`](../deploy/helm/forge/README.md) runs the
coordinator in-cluster — `serve-batch` as a Deployment (data-dir PVC, health
probes, bearer-key Secret) and one-shot `forge run` as a Job whose PVC-backed
checkpoint turns pod evictions into resumes. Engines stay BYO Services; build
the image from the repo-root [`Dockerfile`](../Dockerfile).

Placement rules:

- **One coordinator per checkpoint DB.** The coordinator is the single writer; the
  queue's correctness assumes it. Don't run two `forge run`/`resume` processes
  against the same `--checkpoint`.
- Put `--checkpoint` and `--out` on local disk (or a volume that survives the
  coordinator box). NFS is untested for the SQLite WAL file; prefer local + backup.
- The optional topology for spot fleets: `forge-agent serve` next to the queue,
  `forge-agent run --coordinator … --engine http://127.0.0.1:8000 --cloud aws` on
  each spot box (see [API.md §2](./API.md#2-coordinator-http-protocol-forge-proto)).

## State & backup

**The checkpoint IS the run.** Everything needed to resume lives in three sibling
artifacts (formats in [CONFIG.md §state](./CONFIG.md#state--data-formats)):

| what | where | why it matters |
|---|---|---|
| Lease queue + job metadata | `--checkpoint` (SQLite WAL: `state.db` + `state.db-wal` + `state.db-shm`) | item states (`Pending/Leased/Done/DeadLetter`), attempts, the recorded output path `resume` uses |
| Results + dead-letters | `<out>`, `<out>.dead.jsonl` | the emitted-id set that makes resume idempotent is **rebuilt by scanning these files** — losing them and resuming would re-run finished items |
| Reject sidecar | `<out>.reject.jsonl` (`<checkpoint>.reject.jsonl` when `--out` is an object-store URL) | malformed input lines preserved for repair (not needed for resume) |

Backup procedure (crash-consistent):

```sh
# While the run is stopped: copy the whole directory (db + -wal + -shm + results files).
# While the run is live: snapshot the queue with SQLite's own backup, then copy the JSONLs.
sqlite3 .forge/state.db ".backup .forge/state.backup.db"
cp results.jsonl results.jsonl.dead.jsonl backups/
```

Restore = put the files back and `forge resume --checkpoint …`. Because writes are
store-then-ack and results are idempotent by `custom_id`, restoring a *slightly
stale* checkpoint is safe: at worst items re-run and their duplicate results are
dropped by the emitted-id dedup (exactly-once *effect*). Restoring results files
older than the checkpoint is the one bad combination — the queue would say `Done`
for ids the results file no longer holds; if you must, run `forge verify` and
`forge sweep` afterwards.

With the `object_store` backend, results live in the bucket and the `_manifest/`
prefix replaces the file scan; the checkpoint DB is then the only local state.

## Upgrade / rollback

- **Finish or drain a run before upgrading the binary.** The SQLite schema is
  additive (`CREATE TABLE IF NOT EXISTS`) but carries no `format_version`; there is
  no cross-version checkpoint promise yet.
- The safest upgrade unit is a *run*: complete → `forge verify` → upgrade → next run.
- Mid-run rollback to the same version that created the checkpoint is always fine
  (that's just a resume).
- `forge-proto` peers tolerate older/newer fields (`usage`/`latency_ms`/`attempts`
  are `#[serde(default)]`), but keep coordinator and agents on the same minor
  version — the protocol has no version negotiation.

## Monitoring

`forge status --checkpoint … --prometheus` emits textfile-collector metrics; run it
on a timer (cron/systemd) and scrape the output. Real output from a live run:

```text
forge_items{state="pending"} 0
forge_items{state="leased"} 0
forge_items{state="done"} 5
forge_items{state="dead"} 1
forge_failures{reason="server_error"} 1
forge_retried 0
forge_success_rate 0.8333333333333334
```

What to alert on:

| signal | alert when | meaning |
|---|---|---|
| `forge_items{state="pending"}` | not decreasing | throughput stalled — workers unready, AIMD floor, or engine wedged |
| `forge_items{state="leased"}` | high and flat while pending > 0 | leases parked on dead workers; the reaper will reclaim them after the lease TTL (up to `--lease-max-secs`) |
| `forge_success_rate` | below your job's floor | failure mix shifting — check `forge_failures{reason=…}` |
| `forge_failures{reason="rate_limited"}` | growing | engines saturated; AIMD is halving — consider more workers, not more concurrency |
| `forge_failures{reason="validation"}` | growing | the model is producing structurally bad output under `--require` |
| `forge_retried` | large fraction of total | retry churn (spot interruptions, flaky network) — each retry re-buys GPU time |

`forge status --json` emits the same numbers plus `attempts_histogram` for scripts.
## Exit codes — the orchestrator contract

`forge run`, `resume` and `verify` return distinct exit codes so a scheduler can
decide what to do without parsing logs. **These are a stability contract**; adding a
code is a minor change, changing an existing meaning is breaking.

| Code | Meaning | What a pipeline should do |
|---|---|---|
| `0` | Complete. Every item terminal and successful. | Continue. |
| `1` | Run-time failure (I/O, fleet, store). | Retry at your own risk; investigate. |
| `2` | **Incomplete** — items still pending or leased. | **`forge resume`.** This is expected to succeed and is cheap. |
| `3` | **Usage** — bad config or bad input (including a missing input file). | **Never retry.** A human must change something; a retry burns the same GPU minutes to fail identically. |
| `4` | Complete, but some items dead-lettered. | Treat as success; alert. `forge status --json` has the failure breakdown, and dead-letters are in `<out>.dead.jsonl`. |

The distinction that earns its keep is `2` vs `3`. Both are non-zero, so `set -e`
scripts behave as before, but only `2` is worth retrying.

Airflow/Argo sketch:

```bash
forge run --input in.jsonl --out out.jsonl --checkpoint ck.db --workers "$FLEET"
case $? in
  0|4) exit 0 ;;                                   # done (4 = alert, but done)
  2)   forge resume --checkpoint ck.db --workers "$FLEET" ;;
  3)   echo "config/input error — not retrying" >&2; exit 1 ;;
  *)   exit 1 ;;
esac
```

## Unattended runs — completion and stall hooks

A long batch runs while nobody is watching. Two **outbound** notifications close
that gap:

- `--on-complete <URL_OR_COMMAND>` — fires once the run reaches a terminal state,
  carrying the same counts as `status --json` plus `exit_code` and `outcome`.
- `--on-stall <URL_OR_COMMAND>` — fires **once** if no item reaches a terminal
  state for `--stall-secs` (default 900). This is the dead-spot-fleet case: the
  process is alive, the queue is not moving.

Both accept either form:

- an `http(s)://` URL — the JSON is POSTed. **This is the form that works in the
  official image**, which is `FROM scratch` and has no shell and no `curl` to exec.
- anything else — executed directly (not through a shell), payload on **stdin**.

```bash
forge run … --on-complete https://hooks.example.com/forge \
            --on-stall   https://hooks.example.com/forge-stall --stall-secs 600
```

A hook can never fail the run: a webhook that times out or a command that exits
non-zero is logged and ignored. A finished 50M-item job must not report failure
because a notification endpoint was down. Hook targets are parsed *before* any work
starts, so a typo is an immediate exit `3` rather than a six-hour surprise.

> **Scope note.** These are outbound only — forge reporting on its own run. They are
> not the inbound cron/triggers [`../README.md`](../README.md) rules out: nothing
> here decides what runs next, and no successor edge or `depends_on` is created. The
> dagron boundary is untouched.

## Reading the logs

Diagnostics go to **stderr**; a command's own output (including every `--json` payload)
goes to stdout, so `forge status --json | jq` is safe to pipe.

`--log-format json` (or `FORGE_LOG_FORMAT=json`, which is what a Deployment sets once)
emits one JSON object per line for a log shipper. `RUST_LOG` still selects levels.

Every process logs its identity on startup, which is the first thing a support ticket
needs:

```console
$ forge run … --log-format json
{"timestamp":"…","level":"INFO","message":"forge starting","version":"0.1.2","git_sha":"a1b2c3d","target":"x86_64-unknown-linux-musl","command":"run"}
```

The sha is baked in at build time from `FORGE_GIT_SHA`; it is **absent** rather than
guessed when a build did not supply one:

```bash
docker build --build-arg FORGE_GIT_SHA="$(git rev-parse --short HEAD)" .
```

**Progress heartbeat.** A run reports itself every 30s while it works, so a log stream
distinguishes a working job from a wedged one:

```
INFO progress done=41320 dead=3 total=50000 in_flight=256 items_per_sec=118.4 eta_secs=73 tokens=8912440
```

Silence for longer than that interval means the loop is not dispatching — check for the
`no ready worker; waiting for the fleet` line, and see `--on-stall` for the unattended
case. Tune or disable it with `RunConfig::progress_every` when embedding.

**Request ids.** Both HTTP servers put an `x-request-id` on every response and log it
against everything that request did. A caller's own `x-request-id` is echoed (sanitised)
so their traces join to forge's. A `500` returns only that id:

```json
{"error":{"type":"internal_error","message":"the server could not complete this request"},"request_id":"req_9f3c1a20b4d5e6f7"}
```

The detail is deliberately not in the body — it would tell the caller your filesystem
layout and nothing they can act on. Find it with the id:

```bash
kubectl logs deploy/forge | grep req_9f3c1a20b4d5e6f7
```

**`/v1/health`** is unauthenticated (a load balancer holds no credential) and reports
liveness, build identity and uptime — an uptime that keeps resetting is a crash loop. It
does **not** report batch or tenant counts: those are on `/metrics`, behind the key.

## Backing up a checkpoint

**A checkpoint cannot be backed up with `cp`.** The queue runs in WAL mode, so the
committed state at any instant is spread across `state.db`, `state.db-wal` and
`state.db-shm`. Copying those with three separate reads can capture a torn set — and the
failure is silent: the copy opens fine and is simply missing completed items, so a
restore re-runs work you already paid for.

Quiesce first (`SIGTERM`, then copy all three files together), or take a consistent
online copy while the run continues:

```rust
forge_queue::snapshot_db("state.db", "backup/state.db")?;   // SQLite VACUUM INTO
```

## Stopping a run

`run`, `resume` and `serve-batch` all drain on **SIGTERM** as well as Ctrl-C — the
signal Kubernetes sends on eviction, rollout and scale-down, that `docker stop` and
systemd send, and that follows a spot-instance reclaim.

A drain stops leasing new work and lets everything already in flight finish, storing
each result before acking it. Items never leased stay `pending`, so the process exits
`2` (Incomplete) and `forge resume` picks up exactly where it stopped. Nothing is lost
and nothing runs twice.

```console
$ kubectl delete pod forge-run-abc      # or a spot reclaim, or `docker stop`
SIGTERM received — draining: no new work will be leased, in-flight items will finish.
Send SIGKILL to abort immediately (leases expire and are reclaimed, so nothing is lost
but the GPU time).
done=41320 dead=3 tokens_this_run=8912440 (total items 50000)
$ echo $?
2
```

**A second signal does not escalate.** A drain only ever finishes what is already in
flight, so an impatient second Ctrl-C would throw away exactly the work the first one
was protecting. If you genuinely need it gone now, `SIGKILL` — that is safe here because
the queue is crash-safe: leases expire, the reaper reclaims them, and the idempotent
store write makes the re-run a no-op. A hard kill costs GPU minutes, never correctness.

Set the pod's `terminationGracePeriodSeconds` to comfortably exceed one item's latency —
the drain takes as long as the slowest in-flight generation, not as long as the job.

**`serve-batch` restarts resume in flight.** A batch that was running when the process
died is re-listed *and its fan-out restarted*, because every step is idempotent (the
hydrate skips ids already present, the queue is durable, the store dedups by
`custom_id`). Prior to this, such a batch reported `in_progress` forever with nothing
running it, which an SDK poll loop cannot distinguish from slow progress.

The acceptance gate for a finished job is `forge verify` (non-zero exit on any
missing id) — put it in CI/the pipeline, not a human's memory.

Logs: `RUST_LOG=info` (default) prints ingest stats, dead-letter warnings, and
drain/spot notices; `RUST_LOG=forge_core=debug` adds per-round lease/reap detail.

## Capacity guidance

- The coordinator is I/O-light: one process drives hundreds of workers; the SQLite
  queue leases in batches of 256 and all writes ride one WAL. Exercised in tests: a
  128-worker fleet stress (6k items, ≥100 workers used, none pushed past its cap —
  `forge-cli/tests/concurrency.rs`) and a 150-worker dispatch-assignment check
  (`forge-core/src/run.rs` tests).
- Fleet throughput ceiling = Σ per-worker `--concurrency` (the AIMD ceilings). Set
  each to the engine's own limit and let AIMD find the real value.
- RAM is bounded by design: ingest streams, `verify` spills sorted runs
  (`--run-capacity` ids in RAM ≈ tens of MB at the default 1M), results are
  append-only. The emitted-id dedup set is the one O(items) structure (~ id bytes ×
  items; ~1 GB per 50M 20-byte ids) — the `object_store` manifest exists for the
  same reason at scale.
- Disk: checkpoint ≈ input size (bodies are stored in the queue); results ≈ sum of
  response bodies. Budget both on the coordinator volume.
- The bounded-RAM claims are checked by opt-in 1M-line benchmarks
  (`forge-cli/tests/scale.rs`; run
  `cargo test -p forge-cli --test scale -- --ignored --nocapture`, size via
  `FORGE_SCALE_N`): the `verify` sweep must stay under a 150 MiB peak-RSS ceiling at
  1M ids even when forced to spill many sorted runs (RAM is O(`--run-capacity`), not
  O(N)); the full 1M-item fan-out soak asserts exactly-once completion under a
  runaway guard — a **soak**, not a strict O(1) claim, since the SQLite working set
  grows modestly with N (~0.7 MiB per 1k items observed). The strict bounded-RAM
  guarantee is the sweep and the object-store result path, not a 50M *local* run.

## Troubleshooting (symptom-first)

**`no worker became ready within 60s; check --workers / engine health`** — no
worker answered `GET /health` with a 2xx inside `--ready-grace-secs`. Curl the URL
yourself; remember vLLM needs minutes to load a big model — raise
`--ready-grace-secs` for cold engines.

**Run finishes with `dead=N` — items in `<out>.dead.jsonl`.** Every dead-letter
carries the classified reason in its `error.message`:
- `retries exhausted … HTTP 500/timeout/connection` → engine-side instability. Fix
  the cause, then put the failed items back on the **same** job:

  ```bash
  forge requeue --checkpoint state.db --out results.jsonl --reason 500 --dry-run
  forge requeue --checkpoint state.db --out results.jsonl --reason 500
  forge resume  --checkpoint state.db --out results.jsonl --workers "$FLEET"
  ```

  `requeue` moves dead-lettered items back to `pending` with a fresh retry budget
  **and prunes their records from `<out>.dead.jsonl`**. The prune is not tidiness —
  it is the whole feature. The store rebuilds its emitted-id set by scanning `<out>`
  *and* `<out>.dead.jsonl`, and skips any id already in it, so an item requeued while
  its dead record still stands would be re-leased, re-run against a real GPU, and
  have its fresh result **silently discarded**. You would pay for the retry and get
  nothing.

  Select with `--ids <file>` (takes `verify`'s `.missing.txt` or the `.dead.jsonl`
  directly), `--reason <substring>` to match the recorded error, or `--all`. There is
  no default: reversing a terminal state never guesses at scope. `--dry-run` prints
  the selection and changes nothing.

  **Stop the run first.** `requeue` refuses while any live lease exists on the
  checkpoint, because a running coordinator cached its emitted-id set at startup and
  pruning the file underneath it would reach nothing.

  Requeue keeps one job, one checkpoint and one results file. The alternative — a
  **new** job from a rebuilt input — is still there and is the right call when you
  need to change the requests themselves (a wrong model name, an oversized context)
  rather than just re-run them:

  ```bash
  forge subset --input in.jsonl --ids results.jsonl.dead.jsonl --out retry.jsonl
  # …edit retry.jsonl…
  forge run --input retry.jsonl --out retry.out.jsonl --checkpoint retry.db --workers "$FLEET"
  ```

  `subset` reads the `.dead.jsonl` shape directly — no reshaping, no shell join. Both
  paths stay on the correct side of the no-DAG boundary: neither creates an item
  relationship, and neither has an opinion about what runs next. `--invert` gives you
  the complement (everything *except* the listed ids) when that is the easier half to
  describe.
- `HTTP 4xx` (non-429) → the request itself is bad (wrong model name, oversized
  context). Fix the input line; it was never retried by design.
- `validation failed: …` → the 2xx body failed `--require` even after retries
  (soft-failure path); the item was **not** silently emitted.

**`ingested … rejected=N` warning; `<out>.reject.jsonl` exists** (beside the
checkpoint — `<checkpoint>.reject.jsonl` — when `--out` is an object-store URL). N
input lines were malformed or had an invalid/duplicate `custom_id`. The job
continued without them (they never abort a run). Repair the sidecar lines and
re-ingest as a new input file.

**`object-store output (s3://…) needs a `forge` built with `--features
object_store``.** You passed a `scheme://` URL as `--out` (or `--results` to
`cost`/`verify`) to the default lean binary, which writes local JSONL only. It fails
fast — before ingest — by design. Rebuild with
`cargo build -p forge-cli --release --features object_store`.

**`forge cost`/`forge verify` errors `no forge results found under …` or `multiple
runs under …; point --results at a single run`.** Read-back expects the *same*
`--out` URL the run used, with exactly one job prefix beneath
`<out>/results/`. "No results" = nothing was written there yet (typo'd URL, or the
run never emitted). "Multiple runs" = several jobs share the prefix — point at a
root that holds one run. Related footgun at *write* time: the job prefix is the
sanitized **checkpoint file stem** (`state.db` → `results/state/`), so two different
runs given the same `--out` root and same checkpoint stem interleave under one
prefix; give concurrent runs distinct checkpoint stems or distinct `--out` prefixes
(`object_job_id` in `forge-cli/src/main.rs`).

**Ingest re-run warns `input is shorter than the checkpoint offset
(replaced/truncated); rescanning from the start`.** The seek-resume guard: the
checkpoint's ingest offset points past the current input's EOF, so the input was
replaced or truncated. forge falls back to a full rescan (safe — `enqueue` dedups
already-hydrated ids) instead of seeking past EOF and silently ingesting nothing.
The caveat that is *not* detectable: an input **edited in place** to the same or
greater length can make the resumed seek land mid-line and read a garbled boundary —
dedup prevents duplicates, not skipped/corrupted lines. A resumed ingest must point
at the same, append-only input file; if the input changed, start a fresh checkpoint
(`ingest_jsonl_with_rejects` in `forge-shard/src/lib.rs`).

**Progress stalls but nothing errors.** Check `forge status`: if `leased` is high,
a worker died holding leases — they re-queue automatically when the (adaptive)
lease TTL expires, worst-case `--lease-max-secs` (default 30 min). To reclaim
immediately on a run that is *not* executing, `forge sweep --checkpoint …`. If
`pending` is high and `leased` low, no worker is ready (see the probe symptom
above).

**Throughput drops sharply mid-run.** AIMD halved a worker's in-flight limit after
429/5xx/connection errors — that is backpressure working, not a bug: the engine
signaled overload. The limit climbs back (+1 permit per 8 clean responses) once the
engine recovers. If it never recovers, the engine's own capacity is the ceiling —
add workers.

**A worker goes idle for a few seconds after a 429, whole fleet paused against
it.** Adaptive cooldown, not a hang. When an endpoint returns `Retry-After` or an
exhausted `x-ratelimit-reset-{requests,tokens}` budget, forge arms a per-worker
cooldown and waits it out at `submit()` entry — so every item bound for that
worker holds off in lockstep instead of hammering the limit. Clamped to 120s;
clears automatically. No config. If the endpoint is behind a shared gateway that
sends a very long `Retry-After`, the 120s clamp caps the stall. Nothing to do but
wait, or add workers on a different endpoint.

**A slow item keeps being re-dispatched (wasted GPU).** The item's latency exceeds
the lease TTL. The adaptive lease grows toward `--lease-max-secs` as successful
latencies are observed, but a *first* very-slow item can still be re-leased — raise
`--lease-secs` when p99 generation time is known to be long.

**`resume` warns `ignoring --out; resuming into the original run's output file`.**
Not an error: the checkpoint records the original `--out`, and dedup depends on
reading the prior rows there, so a diverging `--out` is ignored.

**`checkpoint has no recorded output path … — pass --out`.** The checkpoint
predates job-metadata recording; pass the original `--out` explicitly.

**`forge verify` exits `2` / `INCOMPLETE: N missing`.** Some input ids have no
terminal result — typically a run that was killed and never resumed to completion,
or results files that were pruned. The missing ids are in `<results>.missing.txt`.
If the checkpoint still exists, `forge resume` closes the gap. If it does not (the
usual reason results get pruned), turn the sidecar back into a runnable input:

```bash
forge subset --input in.jsonl --ids results.jsonl.missing.txt --out gap.jsonl
forge run --input gap.jsonl --out gap.out.jsonl --checkpoint gap.db --workers "$FLEET"
```

`verify` stays report-only by doctrine; the things that re-queue are
`sweep`/`resume`, and `subset` only writes a file.

**`forge cost` prints a negative `saved`.** Honest by design: the tokens captured
don't amortize the GPU bill you entered — the batch was too small for a dedicated
fleet, or you priced the wrong baseline.

**Spot box reclaimed mid-run.** Nothing to do. In-flight items' leases expire and
re-queue; already-stored results are durable; with `forge-agent --cloud …` the box
additionally drains gracefully inside the notice window. `forge_retried`/the
attempts histogram is where you see the re-queue cost.

**`database is locked` / SQLite busy errors.** Another process holds the checkpoint
(a second coordinator, a stuck `status` in a debugger, a backup tool locking the
file). The queue rides out 60 s of contention (`busy_timeout`), then errors. Ensure
one writer; take backups with `sqlite3 .backup` rather than copying a live db.

## Security posture

What listens, what authenticates, what is deliberately out of scope. Distinct from
a disclosure policy.

- **The `forge` CLI listens on nothing.** It makes outbound HTTP(S) to your worker
  URLs only. By default it sends **no credentials** — self-hosted engines are
  reachable without auth (private network/VPC, SSH tunnel, or a reverse-proxy that
  injects auth). Driving a **hosted** provider endpoint is the one opt-in exception:
  `FORGE_WORKER_API_KEY` (env-only, never a flag, never logged) is sent in the
  provider's own header style per the `--engine` hint.
- **`forge-agent serve` is an unauthenticated JSON-over-HTTP listener**, bound to
  `127.0.0.1:8080` by default. Anyone who can reach it can pull prompts (data
  exfiltration) and post results (forgery — fenced by lease generation against
  *staleness*, not against malice). Widen the bind only inside a trusted network
  boundary (VPC security group / firewall / mTLS reverse proxy). There is no TLS,
  authn, or authz in the OSS protocol; that is the operator's layer.
- **Data at rest is plaintext**: prompts in the checkpoint DB, responses in the
  results JSONL. Apply disk encryption / bucket policies per your data class; with
  the `object_store` backend, bucket IAM is the control.
- **Spot metadata polling** talks only to the link-local metadata service
  (`169.254.169.254`, AWS via IMDSv2 session tokens) and never provisions or
  terminates instances — there is no cloud-credential use anywhere in forge.
- Multi-tenancy is out of scope for forge: run one coordinator per tenant/job
  and isolate at the deployment layer.
