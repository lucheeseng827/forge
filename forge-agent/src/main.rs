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
//!
//! `serve` and `run` share one secret, `FORGE_COORDINATOR_TOKEN`, read from the
//! environment only so it never appears in a process listing. `serve` requires it for
//! any bind that is reachable from another machine.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use forge_agent::http::{serve_coordinator_with, CoordinatorConfig, HttpCoordinator};
use forge_agent::{run_agent, AgentConfig, CloudSource, Drain, InProcessCoordinator};
use forge_core::{EndpointKind, Worker, WorkerSpec};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;

/// The shared coordinator secret. Environment-only: a flag value would land in
/// `/proc/<pid>/cmdline` and in every process listing on the box.
const TOKEN_ENV: &str = "FORGE_COORDINATOR_TOKEN";

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
    /// spot notice (with `--cloud`). Exits when the coordinator reports the queue
    /// empty of work (nothing pending, nothing leased anywhere) or a notice fires.
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
        /// Bind address. Defaults to **loopback**. Any other bind (`0.0.0.0:…`, a
        /// routable address) requires `FORGE_COORDINATOR_TOKEN`: the coordinator hands
        /// out raw prompts and accepts the results that become the record, so an open
        /// one lets any reachable host read prompts or forge results.
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Default lease visibility timeout, seconds (when a pull doesn't ask).
        #[arg(long, default_value_t = 120)]
        lease_secs: u64,
        /// Accept threads.
        #[arg(long, default_value_t = 4)]
        threads: usize,
        /// Serve a non-loopback bind with NO token. Only for a network where every
        /// host that can reach the port is already trusted with every prompt and
        /// result (an isolated VPC, a pod reachable only through a mesh that
        /// authenticates). Ignored when FORGE_COORDINATOR_TOKEN is set.
        #[arg(long)]
        insecure_no_auth: bool,
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

/// Refuse to serve an open coordinator on a bind other machines can reach.
///
/// The default bind is loopback, so this only fires for someone who widened it — which
/// is exactly the moment to ask for a token, rather than discovering later that every
/// prompt in the queue was readable from the subnet.
fn check_exposure(bind: &str, has_token: bool, insecure_no_auth: bool) -> anyhow::Result<()> {
    if has_token {
        if insecure_no_auth {
            tracing::warn!(
                "--insecure-no-auth is ignored: {TOKEN_ENV} is set, so the coordinator requires it"
            );
        }
        return Ok(());
    }
    if forge_core::auth::is_loopback_bind(bind) {
        return Ok(());
    }
    if insecure_no_auth {
        tracing::warn!(
            bind,
            "serving the coordinator with NO authentication on a non-loopback bind \
             (--insecure-no-auth): any host that can reach it can lease prompts and post results"
        );
        return Ok(());
    }
    anyhow::bail!(
        "refusing to serve the coordinator on {bind} without a token: any host that can reach \
         it could lease prompts and post forged results. Set {TOKEN_ENV} here and on every \
         `forge-agent run`, bind to loopback, or pass --insecure-no-auth if every host that \
         can reach this port is already trusted."
    )
}

fn main() -> anyhow::Result<()> {
    // The blocking pool is capped here rather than left at tokio's 512 — see
    // `forge_core::runtime`.
    forge_core::runtime().block_on(run())
}

async fn run() -> anyhow::Result<()> {
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
            let mut coord = HttpCoordinator::new(&coordinator)?;
            let authenticated = match forge_core::auth::secret_from_env(TOKEN_ENV)? {
                Some(token) => {
                    coord = coord.with_token(token)?;
                    true
                }
                None => false,
            };
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
                // 0 = never exit on the empty-grant heuristic. The exit that matters is
                // the coordinator's `drained` flag, which is a fact rather than a guess;
                // this fallback only fires against a coordinator too old to send it, and
                // guessing "finished" from "empty" can strand a re-queued item.
                max_idle_polls: 0,
            };
            let watcher = CloudSource::parse(&cloud).map_err(|e| anyhow::anyhow!(e))?;

            tracing::info!(
                coordinator = %coordinator,
                engine = %engine,
                worker_id = %worker_id,
                concurrency,
                cloud = %cloud,
                authenticated,
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
            insecure_no_auth,
        } => {
            // Decide on auth BEFORE opening anything: a refusal must not leave a
            // half-created queue file behind.
            let token = forge_core::auth::secret_from_env(TOKEN_ENV)?;
            check_exposure(&bind, token.is_some(), insecure_no_auth)?;

            let q = Arc::new(SqliteQueue::open(&queue)?);
            let s = Arc::new(JsonlStore::new(&out));
            let mut cfg = CoordinatorConfig::new(Duration::from_secs(lease_secs), threads);
            let authenticated = token.is_some();
            if let Some(token) = token {
                cfg = cfg.token(token);
            }
            let server = serve_coordinator_with(&bind, Arc::clone(&q), Arc::clone(&s), cfg)?;
            tracing::info!(
                addr = %server.addr(),
                queue = %queue,
                out = %out,
                authenticated,
                "forge-coordinator serving forge-proto (agent leases here; single-writer)"
            );
            // Serve until the process is killed; `server`'s threads run independently.
            // Keep it alive (and the type as the arm's Result) by parking here.
            let _keep = server;
            std::future::pending::<anyhow::Result<()>>().await
        }
    }
}
