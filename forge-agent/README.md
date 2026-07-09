# forge-agent — the optional co-located spot-drain agent

`forge-agent` runs **inside** a spot/preemptible box, next to one engine, so it can
use the in-VM ~30s reclaim window gracefully — the interruption signal is poll-only
and local. It does three strictly-bounded things and nothing else: **lease-proxy**
(long-poll a single-writer coordinator, run each item against the local worker, post
results back), **health-gate** (only pull when the local engine is ready), and
**spot-drain** (watch for a preempt notice locally and stop pulling new leases).

Correctness never depends on the agent: it only *proposes* results (the coordinator
stays the single writer, fencing every post by lease generation), and a missed
notice, crashed agent, or dropped post all degrade to the same lease-expiry
re-queue. The agent is an optimization, never a scheduler — it has no task graph and
**never provisions**.

## What it does

- **`Coordinator` trait** — the transport boundary (`pull` / `post` / `notify`) over
  the `forge_proto` wire contract. Ships two impls: in-memory `InProcessCoordinator`
  (co-located single-box + test vehicle) and the networked `http::HttpCoordinator`
  (client) + `http::serve_coordinator` (server).
- **`run_agent`** — the loop: health-gate → `pull` a batch sized to engine
  concurrency → run-and-`post` concurrently (paced by the worker's own AIMD limiter)
  → repeat. Configured by `AgentConfig`, reports `AgentStats`.
- **`Drain`** — a one-way `Running → Draining` latch shared with the spot watcher;
  once flipped, the box never pulls a new lease again.
- **`spot_drain` + `CloudSource`** — drive the `forge_spot` watcher for `aws` / `gcp`
  / `azure` (`none` disables it); on a notice, latch `Drain` and forward an advisory
  `InterruptionNotice`.
- **Wire mapping helpers** — `wire_to_item`, `result_to_post`, `to_wire_usage` /
  `from_wire_usage` between `forge_core` types and the `forge_proto` shapes.

## Quickstart

The `forge-agent` binary has three subcommands:

```sh
# in-VM interruption detector — exit when a notice arms; hook your drain to the exit
forge-agent watch --cloud gcp --interval-secs 5 --worker-id gpu1

# the agent: lease from a remote coordinator, run a local engine, drain on a notice
forge-agent run --coordinator http://coordinator:8080 --engine http://localhost:8000 \
                --worker-id gpu1 --concurrency 64 --endpoint chat --cloud aws

# the coordinator side: expose a hydrated queue + result store over forge-proto HTTP
forge-agent serve --queue ckpt.db --out out.jsonl --bind 127.0.0.1:8080
```

The library `serve_coordinator` fn stays generic over the `forge-core` traits; the
`serve` subcommand wires a real `forge_queue::SqliteQueue` + `forge_store::JsonlStore`.
Note that `serve` is unauthenticated — it defaults to loopback; widen the bind only
behind your own network controls.

## Config

| Env | Purpose |
|-----|---------|
| `RUST_LOG` | `tracing` subscriber filter (default `info`) |
