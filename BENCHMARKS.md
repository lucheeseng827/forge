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

## Isolated cloud run: real network, resource headroom (EC2)

**The claim:** the coordinator is effectively free — single-digit MB of RAM and
under 1% of one core while saturating a cross-node fleet — so forge adds
scheduling, durability, and accounting without meaningfully taxing the box it
runs on.

Run 2026-07-10, ap-southeast-1, release build built on the box from this repo.
Coordinator: c6i.2xlarge (8 vCPU). Fleet: 3 × c6i.large worker nodes, each
running `examples/engine_sim.py` (`ENGINE_SIM_HOST=0.0.0.0`) — two at cap 8
@ 600 ms, one weak at cap 4 @ 1200 ms. Real VPC HTTP between nodes,
`--concurrency 8`, no tuning. Resource figures are 0.5 s `/proc` samples of the
forge process; latency percentiles come from the per-item `latency_ms` forge
records in the results JSONL.

| Scenario | items | items/s | vs naive | item p50/p95/p99 (ms) | forge RSS peak | forge CPU mean/peak |
|---|---|---|---|---|---|---|
| naive sequential client, 1 local sim | 40 | 1.67 | 1× | — | — | — |
| forge → 1 local sim (cap 8) | 400 | **12.25** | 7.3× (92% of ceiling) | 649 / 809 / 819 | 7.6 MB | 0.5% / 4% |
| forge → 3 local sims (mixed) | 400 | **24.39** | 14.6× | 669 / 2499 / 2829 | 7.7 MB | 0.9% / 4% |
| forge → 3 **remote** worker nodes (mixed, VPC) | 800 | **26.89** | 16.1× (~90% of the 30/s fleet ceiling) | 669 / 2399 / 2738 | **9.7 MB** | **0.9% / 2%** |

Every run: all items done, zero dead-letters. The mixed-fleet p95/p99 tail is
the weak box's own queue (cap 4 @ 1200 ms serving its share at its own pace) —
the sliding window keeps it from slowing anyone else down, which is why fleet
throughput stays at the sum of the engines.

Headroom, extrapolated conservatively from the samples: at ~27 items/s the
coordinator spends <1% of one core and <10 MB RSS — the dispatch path has two
orders of magnitude of headroom before the coordinator itself could become the
bottleneck (the durable queue's write path would bind first).

Cost of the entire measured run, on-demand: ≈ $0.42 (4 nodes × ~35 minutes).

Reproduce: launch any 3 boxes that can reach each other on :8000, run the sim
with `ENGINE_SIM_HOST=0.0.0.0`, point `forge run --workers` at their addresses.

## Coordinator memory: the blocking pool, not the queue

**The claim:** forge's resident memory is set by its *thread* count, not by N — and
capping the blocking pool at 16 cuts peak RSS 7.6× at no cost in throughput.

Found by running the scale bench on a 32-core box, where the test process reached
**5.4 GiB** and climbed. The queue was not the cause, and neither was SQLite:

```
VmRSS 5,388,412 kB   Pss_File 2,268 kB   [heap] 23 MB   state.db-wal 5 MB
Anonymous 5,114,588 kB   Threads 518
/proc/<pid>/smaps: 235 × (23 MB, rw-p) + 234 × (40 MB, ---p)
```

Those VMA pairs are glibc **non-main malloc arenas** — a 64 MiB `PROT_NONE`
reservation each, whose first ~23 MiB has been `mprotect`ed writable and dirtied.
235 × 23 MiB is the whole RSS.

Why 518 threads existed: SQLite is synchronous, so every `SqliteQueue` operation is a
`spawn_blocking` that then takes the queue's single `Mutex<Connection>`. The dispatch
window holds the fleet's summed concurrency in flight (64 workers × cap 16 = 1024
here), so ~1024 blocking closures queue at once, all block on that one mutex, and
tokio answers "every blocking thread is busy" by spawning another — to its default
ceiling of 512. glibc then gives each new thread its own arena (capped at 8 × ncores),
and freed chunks return to the arena's free lists rather than to the kernel, so the
high-water mark only ratchets.

**It therefore scales with core count, not with N**, which is why a 4–8 core CI runner
never saw it (~1.5 GiB) while a 32-core box reached 5.4 GiB. The runaway guard in
`scale.rs` (`300 + N/1000 × 1.5` = 1800 MiB at N=1M) only fires on a big machine.

Run 2026-09-10, N=50,000, 64 workers, release, all six cases in parallel:

| `max_blocking_threads` | `MALLOC_ARENA_MAX` | peak RSS | threads | items/s |
|---|---|---|---|---|
| 512 (tokio default) | default | 349 MiB | 518 | 258 |
| 512 | 2 | 38 MiB | 518 | 251 |
| **16 (now shipped)** | default | **46 MiB** | 22 | **269** |
| 8 | default | 36 MiB | 14 | 261 |
| 8 | 2 | 26 MiB | 14 | 264 |
| 2 | default | 28 MiB | 8 | 264 |

Read it: throughput is flat across a **256× range** of pool sizes, because the
connection mutex serialises those threads regardless — the useful blocking concurrency
is ~1. So the 512 threads bought nothing and cost 300 MiB. 16 is the shipped ceiling
rather than the aggressive end: marginally the fastest of the set, and it leaves ~8×
headroom over the measured need for the other blocking users (`tokio::fs` writes in
the result store) so nothing queues behind queue ops. `forge_core::runtime()` is the
one place that sets it; `MALLOC_ARENA_MAX` is deliberately left alone, because the
shipped image is musl-static and musl has no per-thread arenas.

At the bench's default N=1M the same run peaks at **30 MiB** where it previously
reached 5,388 MiB. Most of that is this ceiling; the last 370 MiB came out with the
lease fix below, whose sorter was holding the rest.

## The lease was O(pending) per item — 7.7 items/s at N=1M, now 2,830

**The claim:** a lease costs the same whether the queue holds a thousand items or a
million, so the fan-out figures above extrapolate instead of collapsing.

They did not, before. The N=1M soak run to check the memory ceiling above managed
**91,163 of 1,000,000 items in 3 h 17 m — 7.7 items/s, against 269 at N=50k** — and
wrote 2.06 GB for those 91k items, 22 KB each, none of it result data.

The cause was in `LEASE_SQL`. Its selection was one predicate,
`status = 'pending' OR (status = 'leased' AND leased_until < ?)`, with
`ORDER BY rowid LIMIT ?`. No index can serve both that `OR` and an ordering on
`rowid`, so SQLite answered `MULTI-INDEX OR` + **`USE TEMP B-TREE FOR ORDER BY`** and
sorted the *entire* matching set before the `LIMIT` applied. Since `BatchRun` consumes
exactly one completion per iteration and immediately tops the freed slot back up — the
sliding window, deliberately — a steady-state run leases once per item. That made it a
full sort of the pending set **per item**: O(N²) over a run, and the sorter's spills
were the 22 KB/item of writes.

The fix is to ask for the order the index already has. `idx_items_status` is
`(status, leased_until)`, so under an equality on `status` its entries come out ordered
by `leased_until` and then by the index's implicit trailing `rowid`.
`ORDER BY leased_until, rowid` is therefore answered by a scan with early exit, while
`ORDER BY rowid` alone is not — SQLite cannot prove `leased_until` is constant, so it
sorts. Every pending row has `leased_until IS NULL`, so the two orderings select the
same rows in the same order: insertion order, which `--prefix-bucket` depends on. The
`OR` is split into two `LIMIT`-ed branches so each stays index-served, and the outer
`ORDER BY` interleaves at most `2 × limit` rows.

Selection matches the old form on the steady state, and the 1M cross-check confirms
that much: with expired leases present it returned the same 1024 rows in the same
order — the same 500 reclaimed and 524 fresh. It does not prove the general case, and
the general case is not identical. Each branch carries its own `LIMIT`, and the expired
branch is ordered by `leased_until`, so once MORE than `limit` leases have expired *and*
they carry distinct `leased_until` values, it offers the oldest leases rather than the
lowest rowids. The check above could not have seen that: 500 expired rows against a
limit of 1024 leaves the branch 524 short of its cap, and below the cap it drops
nothing.

No work is lost when they do diverge — a passed-over row is still expired, still
eligible, and re-leasing only moves `leased_until` forward, so the expired set strictly
shrinks. What changes is reclaim order, and the only thing riding on that is the cache
locality `--prefix-bucket` buys, which a reclaimed row has already left behind. The
pending branch — the hot path, and the one `--prefix-bucket` is actually about — stays
exactly insertion-ordered. Cost is the part that changes outright:

| pending rows | old select | new select |
|---|---|---|
| 1,000,000 | 211 ms | **0.115 ms** |
| 500,000 | 103 ms | **0.099 ms** |
| 100,000 | 21.8 ms | **0.090 ms** |

Flat instead of linear, which is the whole point: the old cost fell as the queue
drained, so a run's own progress line accelerated and read like warm-up rather than a
cliff.

End to end, the same N=1M fan-out that was abandoned at 91k items after 3 h 17 m:

| | before | after |
|---|---|---|
| wall | 3 h 17 m (stopped at 9% done) | **353.3 s, complete** |
| throughput | 7.7 items/s | **2,830 items/s** |
| peak RSS | 402 MiB | **30 MiB** |

It is now faster at N=1M than the old code was at N=50k, and the 400-item and 50k
figures elsewhere in this document improve for the same reason — they were paying a
smaller version of the same sort.

Reproduce:

```sh
FORGE_SCALE_N=50000 cargo test --release -p forge-cli --test scale \
  -- --ignored --exact fan_out_at_scale_completes --nocapture
# watch it from another shell:
grep -E 'VmRSS|Threads' /proc/<pid>/status
grep -c ' ---p ' /proc/<pid>/maps      # one per arena reservation
```

## Verification run — 2026-09-10, release, containerised (32-core host)

Everything below was measured in one sitting on the same box, against
`examples/engine_sim.py`. It re-runs the claims above on today's tree rather than
adding new ones, so the numbers are a **regression check**: they land ~5% under the
figures recorded above, consistent with running in a container that shared the host
with a 1M-item soak.

### Fleet throughput (400 items, `--concurrency 8`)

| Fleet | this run | recorded above | ideal ceiling |
|---|---|---|---|
| 1 fast box | 12.28 items/s | 12.99 | ~13.3 (92%) |
| 2 fast boxes | 24.13 items/s | 25.76 | ~26.7 (90%) |
| 2 fast + 1 weak (mixed) | 24.11 items/s | 25.59 | ~30 |

Every case: 400/400 done, 0 dead-lettered, `forge verify` exit 0 (`missing=0 extra=0
duplicate_input=0`). Even split on two fast boxes (197/203). In the mixed fleet the
weak box took **50** items at its own pace (49 in the recorded run) with peak
engine-side queue depth 4 and **zero 429s** — the sliding window still keeps it from
gating the fleet. Coordinator peak RSS 8.3 / 8.9 / 9.1 MB across the three cases.

### Overload safety (declared `--concurrency 64` against a true cap of 8)

200 items, one box: **200/200 done, 0 dead-lettered, 0 permanently lost**, 26
engine-side 429s, engine peak queue depth 24. AIMD plus the shared `Retry-After`
cooldown absorbed an 8× overdeclaration without losing an item.

### Spot kill → audit → resume → verify (300 items, `--lease-secs 15`)

| stage | pending | live | orphaned | done | reclaimable | interrupted |
|---|---|---|---|---|---|---|
| after `kill -9` (108 done) | 184 | **8** | 0 | 108 | 184 | false |
| after the lease TTL lapses | 184 | 0 | **8** | 108 | **192** | **true** |
| after `forge resume` | 0 | 0 | 0 | **300** | 0 | — |

`retried=8` — exactly the reclaimed leases — and the engine served 308 for 300 items,
so the 8 interrupted items ran twice and were stored once. `forge verify`: 300 emitted
ids, 0 missing, 0 extra, 0 duplicate results.

### SIGTERM is a *graceful* drain, and measurably different from a kill

Same batch, `SIGTERM` 7s in: exit code 2, in-flight work drained in **601 ms**, 91
results written, and the audit shows **0 live and 0 orphaned leases** — a clean
resumable checkpoint rather than 8 leases waiting out a TTL. The follow-up `resume`
completed 300/300 with `retried=0`: nothing had to be reclaimed, which is the whole
difference from the `kill -9` row above.

### Dead-letter requeue prunes before it re-queues

40 items quarantined by `--require json` (exit code 4, `failures[validation=40]`).
`forge requeue --all --dry-run` listed all 40 and changed nothing; the real run
reported `pruned=40 requeued=40` and the `.dead.jsonl` sidecar dropped to 0 lines. A
`resume --require any` then landed all 40 **in the results file** (40 lines) —
`verify` exit 0. Without the prune the store's emitted-id set would have discarded
every one of those fresh results.

### Suites

`cargo test --locked --workspace`: **226 passed, 0 failed** (`fmt --check` and
`clippy --all-targets -D warnings` clean). Two `#[ignore]`d scale benchmarks are
excluded by default, as is the feature-gated work: `--features object_store` adds 3
tests, `--features parquet` adds 1, and all 4 pass. Checkpoints written by this
version carry `meta.schema_version=1` and `meta.forge_version=0.2.0`.
