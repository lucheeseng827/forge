# forge benchmarks

Measured behavior, reproducible from the repo. Numbers are *shape*, not a leaderboard:
they are captured on one machine against a mock OpenAI-compatible engine so the
harness itself is the variable under test, not GPU throughput (the GPU heavy-lifting
lives in vLLM/SGLang and is out of forge's scope by design).

## Spot-kill zero-loss (crash mid-batch → resume → complete)

**The claim:** kill forge with `SIGKILL` in the middle of a batch — the way a spot
instance dies — and `resume` finishes the job with **every item done exactly once,
no input⨝output join, nothing lost, nothing double-run.** This is the capability
batch operators otherwise hand-build — joining the output file back against the
input on keys to find unprocessed rows, plus bespoke interruption/checkpoint
recipes. forge's lease queue *is* the resume state, so it answers directly.

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

## Fleet scheduling: sliding-window dispatch (no head-of-line blocking)

**The claim:** on a heterogeneous fleet — the normal spot-market case, where the
boxes you get are never identical — forge's throughput is the *sum* of what each
engine can do, not gated by the slowest one.

Dispatch was originally wave-based: lease a batch, dispatch it all, await the
whole batch, lease the next. That put a barrier at every batch boundary, so one
slow worker gated the fleet — measured below, a mixed fleet ran *slower than a
single fast box*. Dispatch is now a **sliding window with per-worker free-slot
accounting**: every completion immediately frees a slot that is topped up with a
fresh lease, and new leases are assigned only against workers with free capacity
(a saturated worker is skipped, never queued behind).

Run 2026-07-10, release build, Windows laptop, against `examples/engine_sim.py` —
a mock that models real engine behavior: a hard concurrency cap (like
`--max-num-seqs`), queueing beyond it, 429 + `Retry-After` past an admission
limit, and per-request base latency ±30% jitter. 400-item chat batch,
`--concurrency 8`. Fleet: A/B = cap 8 @ 600 ms, C (weak) = cap 4 @ 1200 ms.

| Fleet | wave dispatch | sliding window | ideal ceiling |
|---|---|---|---|
| 1 fast box | 10.86 items/s | **12.99 items/s** (+20%) | ~13.3 |
| 2 fast boxes | 21.08 items/s | **25.76 items/s** (+22%) | ~26.7 |
| 2 fast + 1 weak (mixed) | 9.15 items/s ⚠️ *slower than 1 box* | **25.59 items/s** (2.8×) | ~30 |

Per-engine serve counts in the mixed run confirm the shape: the weak box chews
its own share (49 items at its own pace) while the fast boxes stay saturated —
zero 429s, engine-side peak queue depth ≤ 4.

Baseline for scale: a naive sequential client against the same single box does
**1.58 items/s** — the single-box sliding-window number is ~97% of the engine's
theoretical ceiling, with no tuning.

Reproduce (three terminals + one run):

```sh
python3 examples/engine_sim.py 8001 8 600     # fast box A
python3 examples/engine_sim.py 8002 8 600     # fast box B
python3 examples/engine_sim.py 8003 4 1200    # weak box C

forge run --input batch.jsonl \
  --workers http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 \
  --concurrency 8 --out results.jsonl --checkpoint .forge/state.db
```

The regression is pinned by `forge-cli/tests/pipelining.rs`: a fast + slow worker
pair, round-robin assignment (worst case), asserting the fast worker takes ≥ 2/3
of the items — wave dispatch gave it exactly half.

## Overload safety: AIMD + header cooldown vs a naive concurrent client

**The claim:** misdeclare a worker's concurrency 8× over the engine's true cap
and forge still completes every item, converging on the engine's real limit —
where a naive concurrent client permanently loses work.

Same sim (true cap 8, tight admission queue), 200 items, declared concurrency 64:

| Client | completed | permanently lost | engine-side 429s |
|---|---|---|---|
| naive 64-thread client (fixed 1 s retry, 10 attempts) | 158/200 | **42** | 1107 |
| forge (`--concurrency 64`, AIMD + header cooldown) | **200/200** | 0 | 72 |

forge's AIMD halves the in-flight window on each 429 burst and the shared
per-worker cooldown honors `Retry-After`, so the whole fleet backs off in
lockstep — 94% less rejected traffic, and nothing dead-lettered.
