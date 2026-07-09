# forge benchmarks

Measured behavior, reproducible from the repo. Numbers are *shape*, not a leaderboard:
they are captured on one machine against a mock OpenAI-compatible engine so the
harness itself is the variable under test, not GPU throughput (the GPU heavy-lifting
lives in vLLM/SGLang and is out of forge's scope by design).

## Spot-kill zero-loss (crash mid-batch → resume → complete)

**The claim:** kill forge with `SIGKILL` in the middle of a batch — the way a spot
instance dies — and `resume` finishes the job with **every item done exactly once,
no input⨝output join, nothing lost, nothing double-run.** This is the differentiator
the demand signal says people currently hand-build (Ray issue #59522: users join
their output file back against their input on keys to find unprocessed rows; RunPod
SIGTERM guides; Spheron's shard-lease-checkpoint recipe). forge's lease queue *is*
the resume state, so it answers directly.

Run 2026-07-08, release build, against `examples/` slow mock (50–150 ms/request),
300-item batch, `--concurrency 8`, `--lease-secs 15`:

| stage | pending | live leases | orphaned | done | reclaimable | verify |
| --- | --- | --- | --- | --- | --- | --- |
| after `kill -9` (leases still held by the dead PID) | 4 | **8** | 0 | 288 | 4 | — |
| after the lease TTL expires | 4 | 0 | **8** | 288 | **12** | — |
| after `forge resume` | 0 | 0 | 0 | **300** | 0 | **✅ 0 missing / 0 dup** |

Read it: at the instant of the kill, the 8 requests that were in flight are still
*leased* to the now-dead process — `forge audit` shows them as **live**. Once their
lease TTL lapses they become **orphaned** (`interrupted: true`) — the fingerprint of
an interruption, and exactly the set `resume` reclaims alongside the 4 never-started
`pending`. After resume: 300/300 `done`, and `forge verify` confirms every input
`custom_id` has a terminal result — **300 unique outputs, 0 missing, 0 duplicates**.
No item was processed twice (lease fencing) and none was dropped (orphan reclamation).

### The audit surface

`forge audit --checkpoint <db>` is the join-free "what would resume reclaim" view —
the resume-readiness split the state counts (`forge status`) don't give you:

```text
pending=4 live_leases=8 orphaned_leases=0 done=288 dead=0 total=300
reclaimable=4 (4 pending + 0 orphaned) — `forge resume` re-dispatches these, join-free, zero-loss
live workers still holding leases: forge-17244=8
```

`--json` emits `{pending, live_leases, orphaned_leases, done, dead, total,
reclaimable, interrupted, live_holders}` for monitoring — `interrupted: true` +
`reclaimable > 0` is the alert condition for "a worker died, run `resume`".

### Reproduce

```sh
# 1. a slow OpenAI-compatible mock (any GET → 200 health, POST → canned completion + usage)
python examples/slow_mock.py 0.15 8099 &        # 150ms/request on :8099

# 2. a 300-item batch; kill forge ~6s in (mid-flight)
forge run --input in.jsonl --out out.jsonl --checkpoint ckpt.db \
  --workers http://127.0.0.1:8099 --concurrency 8 --lease-secs 15 &
sleep 6; kill -9 %2

# 3. audit → resume → verify
forge audit  --checkpoint ckpt.db                 # live leases held by the dead PID
sleep 16                                          # let the lease TTL lapse
forge audit  --checkpoint ckpt.db                 # now orphaned → reclaimable, interrupted:true
forge resume --checkpoint ckpt.db --workers http://127.0.0.1:8099 --concurrency 8
forge verify --input in.jsonl --results out.jsonl # exit 0: every id terminal, no dupes
```

**Why this is safe, not lucky:** a lease is fenced by `(custom_id, attempt)`, so a
result posted by a resurrected/duplicate worker for a stale attempt is rejected — an
item cannot be committed twice. An item is only ever `done` after its result is
durably stored (store-then-ack), so a crash between store and ack re-runs the store
(idempotent), never loses it. `resume` re-dispatches exactly `pending + orphaned`;
`done` rows are never re-leased (store-then-ack is the commit protocol).
