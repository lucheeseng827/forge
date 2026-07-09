//! `forge-agent` — the optional co-located spot-drain binary.
//!
//! Run it **on the spot box**, next to one engine. It long-polls the local cloud
//! metadata service for an interruption notice; the moment one arms, it flips the
//! drain latch (and, with a coordinator transport, stops pulling new leases). The
//! agent never provisions and never gains a task graph — it is strictly spot-drain +
//! health-probe + lease-proxy (ARCHITECTURE §3).
//!
//! Three subcommands:
//! - `watch` — the in-VM interruption detector (poll the local metadata service; exit
//!   when a notice arms). Deployable on any spot box; wire your drain hook to the exit.
//! - `run` — the agent: long-poll a remote coordinator (`--coordinator`), run leases
//!   against a local engine (`--engine`), and (with `--cloud`) drain on a spot notice.
//! - `serve` — the coordinator side: expose a local queue + result store over the
//!   `forge-proto` HTTP protocol for agents to lease from and post back to.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use forge_agent::http::{serve_coordinator, HttpCoordinator};
use forge_agent::{run_agent, AgentConfig, CloudSource, Drain, InProcessCoordinator};
use forge_core::{EndpointKind, Worker, WorkerSpec};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;

#[derive(Parser)]
#[command(
    name = "forge-agent",
    about = "forge co-located spot-drain agent (optional)",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Watch the local cloud metadata service and exit when an interruption arms —
    /// the in-VM drain trigger. Hook your graceful-stop on this process exiting.
    Watch {
        /// Which cloud's metadata service to poll.
        #[arg(long, value_name = "aws|gcp|azure", default_value = "aws")]
        cloud: String,
        /// Poll interval in seconds (design to the smallest window: GCP/Azure ~30s).
        #[arg(long, default_value_t = 5)]
        interval_secs: u64,
        /// This box's worker id, stamped on the advisory notice.
        #[arg(long, default_value = "agent")]
        worker_id: String,
    },

    /// Run the agent: lease from a remote coordinator, run a local engine, drain on a
    /// spot notice (with `--cloud`). Exits when the run drains or a notice fires.
    Run {
        /// Coordinator base URL (e.g. http://coordinator:8080).
        #[arg(long)]
        coordinator: String,
        /// Local engine base URL (the OpenAI-compatible endpoint this box fronts).
        #[arg(long)]
        engine: String,
        /// This box's worker id (the lease owner / fencing key).
        #[arg(long, default_value = "agent")]
        worker_id: String,
        /// In-flight concurrency = the engine's own cap (vLLM `--max-num-seqs`, …).
        /// Must be ≥ 1 (0 would only ever pull empty grants and idle forever).
        #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u32).range(1..))]
        concurrency: u32,
        /// Endpoint kind for items that don't carry their own path.
        #[arg(
            long,
            value_name = "chat|completions|embeddings",
            default_value = "chat"
        )]
        endpoint: String,
        /// Spot watcher cloud; `none` to disable the in-VM drain.
        #[arg(long, value_name = "aws|gcp|azure|none", default_value = "none")]
        cloud: String,
        /// Lease visibility timeout, seconds.
        #[arg(long, default_value_t = 120)]
        lease_secs: u64,
        /// Spot-watch poll interval, seconds.
        #[arg(long, default_value_t = 5)]
        interval_secs: u64,
    },

    /// Serve the coordinator side: expose a local queue + result store over the
    /// `forge-proto` HTTP protocol. Hydrate the queue first (e.g. `forge run`).
    Serve {
        /// Path to the SQLite queue db (created if absent; hydrate it separately).
        #[arg(long)]
        queue: String,
        /// Path to the result JSONL the coordinator writes (store-then-ack).
        #[arg(long)]
        out: String,
        /// Bind address. Defaults to **loopback** — the coordinator serves
        /// unauthenticated lease/result/interruption posts, so a public bind
        /// (`0.0.0.0:…`) would let any reachable host pull prompts or forge results.
        /// Only widen it behind your own network controls (VPC / firewall / proxy).
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Default lease visibility timeout, seconds (when a pull doesn't ask).
        #[arg(long, default_value_t = 120)]
        lease_secs: u64,
        /// Accept threads.
        #[arg(long, default_value_t = 4)]
        threads: usize,
    },
}

fn endpoint_kind(s: &str) -> anyhow::Result<EndpointKind> {
    match s.trim().to_ascii_lowercase().as_str() {
        "chat" => Ok(EndpointKind::Chat),
        "completions" => Ok(EndpointKind::Completions),
        "embeddings" => Ok(EndpointKind::Embeddings),
        other => Err(anyhow::anyhow!(
            "unknown endpoint {other:?} (chat|completions|embeddings)"
        )),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Watch {
            cloud,
            interval_secs,
            worker_id,
        } => {
            let source = CloudSource::parse(&cloud)
                .map_err(|e| anyhow::anyhow!(e))?
                .ok_or_else(|| {
                    anyhow::anyhow!("`watch` needs a real cloud (aws|gcp|azure); got `none`")
                })?;

            tracing::info!(
                cloud = %cloud,
                interval_secs,
                worker_id = %worker_id,
                "forge-agent watching for spot interruption (poll-only; never provisions)"
            );

            // No remote coordinator wired here yet: the in-process coordinator records
            // the advisory notice locally. The latch flip is the operational signal —
            // when `drive` returns, the box is draining and this process exits so your
            // wrapper can stop pulling work / start the graceful shutdown.
            let drain = Drain::new();
            let coord = InProcessCoordinator::new();
            source
                .drive(
                    Duration::from_secs(interval_secs),
                    &drain,
                    &coord,
                    &worker_id,
                )
                .await;

            tracing::warn!("draining — interruption armed; stop pulling new work now");
            Ok(())
        }

        Cmd::Run {
            coordinator,
            engine,
            worker_id,
            concurrency,
            endpoint,
            cloud,
            lease_secs,
            interval_secs,
        } => {
            // `concurrency` is already range-validated to ≥ 1 by clap.
            let kind = endpoint_kind(&endpoint)?;
            let coord = HttpCoordinator::new(&coordinator)?;
            let worker = HttpWorker::new(
                WorkerSpec::new(&worker_id, &engine, kind).concurrency(concurrency as usize),
            )?;
            // Health-probe once up front so the first lease isn't pulled into a dead
            // engine (the loop re-probes thereafter).
            worker.probe().await;

            let drain = Drain::new();
            let cfg = AgentConfig {
                worker_id: worker_id.clone(),
                max_lease: concurrency,
                lease_secs,
                poll_idle: Duration::from_secs(1),
                max_idle_polls: 0, // a daemon: run until a notice drains it
            };
            let watcher = CloudSource::parse(&cloud).map_err(|e| anyhow::anyhow!(e))?;

            tracing::info!(
                coordinator = %coordinator,
                engine = %engine,
                worker_id = %worker_id,
                concurrency,
                cloud = %cloud,
                "forge-agent running (lease-proxy + optional in-VM drain)"
            );

            // Run the agent loop and the spot watcher together. The watcher only flips
            // the drain latch and then idles; the agent loop owns completion.
            let agent = run_agent(&cfg, &coord, &worker, &drain);
            let watch = async {
                if let Some(src) = &watcher {
                    src.drive(
                        Duration::from_secs(interval_secs),
                        &drain,
                        &coord,
                        &worker_id,
                    )
                    .await;
                }
                std::future::pending::<()>().await
            };
            let stats = tokio::select! {
                r = agent => r?,
                _ = watch => unreachable!("watcher idles forever after draining"),
            };
            tracing::info!(
                processed = stats.processed,
                dead_lettered = stats.dead_lettered,
                drained = stats.drained,
                "forge-agent stopped"
            );
            Ok(())
        }

        Cmd::Serve {
            queue,
            out,
            bind,
            lease_secs,
            threads,
        } => {
            let q = Arc::new(SqliteQueue::open(&queue)?);
            let s = Arc::new(JsonlStore::new(&out));
            let server = serve_coordinator(
                &bind,
                Arc::clone(&q),
                Arc::clone(&s),
                Duration::from_secs(lease_secs),
                threads,
            )?;
            tracing::info!(
                addr = %server.addr(),
                queue = %queue,
                out = %out,
                "forge-coordinator serving forge-proto (agent leases here; single-writer)"
            );
            // Serve until the process is killed; `server`'s threads run independently.
            // Keep it alive (and the type as the arm's Result) by parking here.
            let _keep = server;
            std::future::pending::<anyhow::Result<()>>().await
        }
    }
}
