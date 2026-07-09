//! `forge` — the single static CLI binary, a thin driver over `forge-core`.
//!
//! Subcommands: `run` (hydrate + fan out), `resume` (continue from a checkpoint),
//! `status` (queue cardinalities + retry metrics), `sweep` (re-queue expired
//! leases), `import` (normalize an OpenAI/Anthropic/Bedrock batch file), `cost`
//! (the token cost-arbitrage report), and `verify` (bounded-RAM completeness sweep).
//! forge is a batch-inference work distributor — it has no DAG, cron, or fleet
//! provisioning.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use forge_batch::{serve_batch, BatchConfig, HttpFleet};
use forge_core::{
    compute_cost, BatchRun, CostInputs, EndpointKind, EngineHint, JobTotals, Queue, ResponseCheck,
    RetryPolicy, RunConfig, WorkerSpec,
};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;

#[derive(Parser)]
#[command(
    name = "forge",
    version,
    about = "Offline/batch inference coordinator — fan a giant JSONL across N (spot) GPUs running vLLM/SGLang/llama.cpp. NOT a workflow/DAG scheduler."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Hydrate the queue from INPUT and fan it across WORKERS into OUT.
    Run(RunArgs),
    /// Continue an interrupted run from CHECKPOINT (queue already hydrated).
    Resume(ResumeArgs),
    /// Print queue cardinalities (pending / leased / done / dead).
    Status(StatusArgs),
    /// Resume-readiness audit: what a `resume` would reclaim (pending + orphaned
    /// leases from dead/spot-killed workers) vs what is genuinely still in flight,
    /// plus which workers hold live leases. The join-free "what's left" answer.
    Audit(AuditArgs),
    /// Re-queue leased-but-no-result items (expired leases), then exit.
    Sweep(SweepArgs),
    /// Normalize an OpenAI/Anthropic/Bedrock batch file into forge-native JSONL.
    Import(ImportArgs),
    /// Report the cost arbitrage (forge $/Mtok, tokens/$, $ saved vs online) from a
    /// results file's real token usage.
    Cost(CostArgs),
    /// Verify completeness: confirm every input `custom_id` has a terminal result
    /// (done or dead-letter), in bounded memory. Exits non-zero if any are missing.
    Verify(VerifyArgs),
    /// Serve the real OpenAI Batch REST API (`/v1/files`, `/v1/batches`) as a front door
    /// over the engine — point unmodified OpenAI SDK code at it. A REST skin over the
    /// existing fan-out; NOT a DAG/workflow server.
    ServeBatch(ServeBatchArgs),
    /// Export a results JSONL to columnar Parquet (one row per `custom_id`), streaming
    /// with bounded memory. Requires a build with `--features parquet`.
    #[cfg(feature = "parquet")]
    Export(ExportArgs),
}

/// Worker fleet + dispatch knobs, shared by `run` and `resume`.
#[derive(clap::Args)]
struct WorkerArgs {
    /// Comma-separated OpenAI-compatible base URLs (BYO endpoints).
    #[arg(long, value_delimiter = ',', required = true)]
    workers: Vec<String>,
    /// Engine behind the workers (health-probe shape / concurrency knob only).
    #[arg(long, value_enum, default_value_t = EngineArg::Vllm)]
    engine: EngineArg,
    /// Endpoint kind every item hits.
    #[arg(long, value_enum, default_value_t = EndpointArg::Chat)]
    endpoint: EndpointArg,
    /// Per-worker in-flight cap (= the engine's max-num-seqs / --parallel).
    #[arg(long, default_value_t = 256)]
    concurrency: usize,
    /// Visibility-timeout floor in seconds before the reaper re-queues a lease.
    #[arg(long, default_value_t = 300)]
    lease_secs: u64,
    /// Cap on the adaptive lease TTL in seconds. The effective TTL grows from
    /// --lease-secs toward this as the run learns item latency, so a slow generation
    /// isn't re-queued mid-flight.
    #[arg(long, default_value_t = 1800)]
    lease_max_secs: u64,
    /// Seconds to wait for a worker to become ready before giving up.
    #[arg(long, default_value_t = 60)]
    ready_grace_secs: u64,
    /// Max attempts per item before dead-lettering. Must be ≥ 1 (the worker treats a
    /// value below 1 as 1, so `0` is rejected rather than silently doing something
    /// else than the flag says).
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..))]
    max_attempts: u32,
    /// Content check applied to every 2xx response. A failing body is retried like a
    /// 5xx and then dead-lettered, so bad data is never silently written.
    /// `any` = accept anything (default); `nonempty` = output must be non-empty;
    /// `json` = the model's output text must parse as JSON (structured-output guard).
    #[arg(long, value_enum, default_value_t = RequireArg::Any)]
    require: RequireArg,
    /// Disable load-aware dispatch (B3) and force flat round-robin. Load-aware bias is
    /// on by default and is itself a no-op unless an engine exposes a queue-depth
    /// metric (vLLM /metrics, llama.cpp /slots, SGLang /get_server_info).
    #[arg(long, default_value_t = false)]
    no_load_aware: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum RequireArg {
    Any,
    Nonempty,
    Json,
}

impl From<RequireArg> for ResponseCheck {
    fn from(r: RequireArg) -> Self {
        match r {
            RequireArg::Any => ResponseCheck::Any,
            RequireArg::Nonempty => ResponseCheck::NonEmpty,
            RequireArg::Json => ResponseCheck::Json,
        }
    }
}

#[derive(clap::Args)]
struct RunArgs {
    /// Input JSONL (OpenAI Batch contract; one request per line keyed by custom_id).
    #[arg(long)]
    input: PathBuf,
    /// Output results JSONL (keyed by custom_id, order not guaranteed).
    #[arg(long)]
    out: PathBuf,
    /// Durable checkpoint DB (the queue/resume state).
    #[arg(long)]
    checkpoint: PathBuf,
    /// Reorder the input by shared prompt prefix `(model, system-prompt)` before
    /// hydrating, so prefix-similar items are contiguous and the engine's automatic
    /// prefix cache (vLLM/SGLang) stays hot across a run of them. Bounded-RAM
    /// preprocessing pass (K temp files beside the checkpoint); opt-in.
    #[arg(long, default_value_t = false)]
    prefix_bucket: bool,
    #[command(flatten)]
    workers: WorkerArgs,
}

#[derive(clap::Args)]
struct ResumeArgs {
    /// Results JSONL — optional. By default `resume` reuses the output path the
    /// original `run` recorded in the checkpoint (prior rows live there and dedup
    /// relies on reading them back). Pass `--out` only to override an old
    /// checkpoint that predates recorded job metadata; a value that diverges from
    /// the recorded path is ignored with a warning.
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long)]
    checkpoint: PathBuf,
    #[command(flatten)]
    workers: WorkerArgs,
}

#[derive(clap::Args)]
struct ServeBatchArgs {
    /// Address to bind the REST server on.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,
    /// Root dir for uploaded files (`files/`) and per-batch checkpoints + results
    /// (`batches/<id>/`). Enough is persisted here that a restart re-lists batches.
    #[arg(long)]
    data_dir: PathBuf,
    /// Optional bearer token. When set, every route except `/v1/health` requires
    /// `Authorization: Bearer <token>`; when unset the server is open (trusted network).
    #[arg(long)]
    api_key: Option<String>,
    /// Number of HTTP accept threads sharing the listener.
    #[arg(long, default_value_t = 2)]
    threads: usize,
    #[command(flatten)]
    workers: WorkerArgs,
}

#[derive(clap::Args)]
struct StatusArgs {
    #[arg(long)]
    checkpoint: PathBuf,
    /// Emit a machine-readable JSON report (for monitoring / alerting) instead of
    /// the human one-liner.
    #[arg(long)]
    json: bool,
    /// Emit Prometheus text-exposition metrics (for the node_exporter textfile
    /// collector / a scrape). Takes precedence over --json.
    #[arg(long)]
    prometheus: bool,
}

#[derive(clap::Args)]
struct AuditArgs {
    #[arg(long)]
    checkpoint: PathBuf,
    /// Emit a machine-readable JSON report instead of the human summary.
    #[arg(long)]
    json: bool,
}

#[derive(clap::Args)]
struct SweepArgs {
    #[arg(long)]
    checkpoint: PathBuf,
}

#[derive(clap::Args)]
struct ImportArgs {
    /// Source batch JSONL from a closed Batch API (OpenAI `body` / Anthropic
    /// `params` / Bedrock `recordId`+`modelInput` are all accepted).
    #[arg(long)]
    input: PathBuf,
    /// Normalized forge-native output JSONL (`custom_id`/`method`/`url`/`body`),
    /// ready for `forge run --input`.
    #[arg(long)]
    out: PathBuf,
    /// Default endpoint for URL inference when a line has no `url` and the body
    /// shape is ambiguous.
    #[arg(long, value_enum, default_value_t = EndpointArg::Chat)]
    endpoint: EndpointArg,
}

#[derive(clap::Args)]
struct CostArgs {
    /// Results written by `forge run` (token usage is summed from them): a local JSONL
    /// path, or the object-store URL you passed as `--out` (`s3://…`, needs `--features
    /// object_store`) — usage is summed straight from the `done/` objects.
    #[arg(long)]
    results: PathBuf,
    /// Total GPU spend for the run, USD. Takes precedence over the per-hour form.
    #[arg(long)]
    gpu_cost: Option<f64>,
    /// Spot $/GPU-hour — combined with --gpu-hours (× --gpus) when --gpu-cost is absent.
    #[arg(long)]
    gpu_usd_per_hour: Option<f64>,
    /// Wall-clock GPU-hours the run took.
    #[arg(long)]
    gpu_hours: Option<f64>,
    /// Number of GPUs (multiplies the per-hour cost).
    #[arg(long, default_value_t = 1.0)]
    gpus: f64,
    /// Online-API baseline $ per 1M input tokens (what you'd otherwise pay).
    #[arg(long)]
    online_per_mtok_input: Option<f64>,
    /// Online-API baseline $ per 1M output tokens (defaults to the input price).
    #[arg(long)]
    online_per_mtok_output: Option<f64>,
    /// Online-API $ per 1M **cached-input** tokens (providers bill prompt-cache hits at
    /// a discount). When set, cached tokens are priced here in the online baseline; when
    /// omitted they're priced at the full input rate (never overstates savings).
    #[arg(long)]
    online_per_mtok_cached_input: Option<f64>,
    /// Machine-readable JSON output.
    #[arg(long)]
    json: bool,
}

#[derive(clap::Args)]
struct VerifyArgs {
    /// The forge-native input JSONL the run was hydrated from (the id universe).
    #[arg(long)]
    input: PathBuf,
    /// The results written by `forge run`: a local JSONL (its `<results>.dead.jsonl`
    /// sibling is read automatically so a dead-lettered id still counts as terminal), or
    /// the object-store URL you passed as `--out` (`s3://…`, needs `--features
    /// object_store`) — every terminal id (done or dead) is read from its `_manifest/`.
    #[arg(long)]
    results: PathBuf,
    /// Write every missing id (one per line) here. Defaults to `<results>.missing.txt`
    /// (or `<input>.missing.txt` for object-store results); created only if something is
    /// missing.
    #[arg(long)]
    missing_out: Option<PathBuf>,
    /// Don't write a missing-id sidecar (report counts only).
    #[arg(long, default_value_t = false)]
    no_missing_out: bool,
    /// Ids buffered in RAM before spilling a sorted run (lower = less memory). The
    /// sweep is exact regardless; this only trades RAM for temp-file count.
    #[arg(long, default_value_t = 1_000_000)]
    run_capacity: usize,
    /// Machine-readable JSON output.
    #[arg(long)]
    json: bool,
}

#[cfg(feature = "parquet")]
#[derive(clap::Args)]
struct ExportArgs {
    /// Results JSONL written by `forge run`.
    #[arg(long)]
    results: PathBuf,
    /// Output Parquet file (one row per `custom_id`).
    #[arg(long)]
    out: PathBuf,
    /// Rows buffered before a row group is flushed (bounds RAM at scale).
    #[arg(long, default_value_t = 50_000)]
    row_group_rows: usize,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum EngineArg {
    Vllm,
    Sglang,
    Llamacpp,
    Router,
}

impl From<EngineArg> for EngineHint {
    fn from(e: EngineArg) -> Self {
        match e {
            EngineArg::Vllm => EngineHint::Vllm,
            EngineArg::Sglang => EngineHint::Sglang,
            EngineArg::Llamacpp => EngineHint::LlamaCpp,
            EngineArg::Router => EngineHint::Router,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum EndpointArg {
    Chat,
    Completions,
    Embeddings,
}

impl From<EndpointArg> for EndpointKind {
    fn from(e: EndpointArg) -> Self {
        match e {
            EndpointArg::Chat => EndpointKind::Chat,
            EndpointArg::Completions => EndpointKind::Completions,
            EndpointArg::Embeddings => EndpointKind::Embeddings,
        }
    }
}

impl WorkerArgs {
    /// The retry envelope from `--max-attempts`. The **worker** owns the retry loop,
    /// so this must be applied to each `HttpWorker` (via `with_retry`) for the flag
    /// to take effect — `RunConfig.retry` alone is not read by the dispatch loop.
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts,
            ..RetryPolicy::default()
        }
    }

    /// The resolved [`WorkerSpec`]s (URLs + endpoint + concurrency + engine hint), used
    /// by `serve-batch`'s [`HttpFleet`] which rebuilds a fresh `HttpWorker` fleet per
    /// batch. Mirrors [`build_workers`](Self::build_workers) minus the client construction.
    fn worker_specs(&self) -> Vec<WorkerSpec> {
        self.workers
            .iter()
            .enumerate()
            .map(|(i, url)| {
                WorkerSpec::new(format!("w{i}"), url.clone(), self.endpoint.into())
                    .concurrency(self.concurrency)
                    .engine_hint(self.engine.into())
            })
            .collect()
    }

    fn build_workers(&self) -> anyhow::Result<Vec<HttpWorker>> {
        self.workers
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let spec = WorkerSpec::new(format!("w{i}"), url.clone(), self.endpoint.into())
                    .concurrency(self.concurrency)
                    .engine_hint(self.engine.into());
                HttpWorker::new(spec)
                    .map(|w| {
                        w.with_retry(self.retry_policy())
                            .with_validation(self.require.into())
                    })
                    .with_context(|| format!("building worker {url}"))
            })
            .collect()
    }

    fn run_config(&self) -> RunConfig {
        RunConfig {
            lease_for: Duration::from_secs(self.lease_secs),
            lease_max: Duration::from_secs(self.lease_max_secs.max(self.lease_secs)),
            ready_grace: Duration::from_secs(self.ready_grace_secs),
            retry: self.retry_policy(),
            load_aware: !self.no_load_aware,
            ..RunConfig::default()
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match Cli::parse().cmd {
        Cmd::Run(a) => cmd_run(a).await,
        Cmd::Resume(a) => cmd_resume(a).await,
        Cmd::Status(a) => cmd_status(a).await,
        Cmd::Audit(a) => cmd_audit(a).await,
        Cmd::Sweep(a) => cmd_sweep(a).await,
        Cmd::Import(a) => cmd_import(a).await,
        Cmd::Cost(a) => cmd_cost(a).await,
        Cmd::Verify(a) => cmd_verify(a).await,
        Cmd::ServeBatch(a) => cmd_serve_batch(a).await,
        #[cfg(feature = "parquet")]
        Cmd::Export(a) => cmd_export(a).await,
    }
}

/// Serve the OpenAI Batch REST front door over the configured worker fleet. Blocks until
/// Ctrl-C, then stops the accept threads. In-flight batch runs are not force-joined —
/// their results are already durable in each batch's `ckpt.db` + `out.jsonl`.
async fn cmd_serve_batch(a: ServeBatchArgs) -> anyhow::Result<()> {
    if a.workers.workers.is_empty() {
        anyhow::bail!("serve-batch needs at least one --workers endpoint for the fleet");
    }
    let fleet = HttpFleet::new(
        a.workers.worker_specs(),
        a.workers.retry_policy(),
        a.workers.require.into(),
    );
    let cfg = BatchConfig {
        bind: a.listen.clone(),
        data_dir: a.data_dir,
        api_key: a.api_key,
        fleet,
        run_config: a.workers.run_config(),
        threads: a.threads,
    };
    let server = serve_batch(cfg).context("starting the batch server")?;
    println!(
        "forge serve-batch listening on http://{} — point your OpenAI SDK's base_url here",
        server.addr()
    );
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!(error = %e, "could not listen for Ctrl-C; shutting down");
    }
    println!("shutting down");
    server.shutdown();
    Ok(())
}

#[cfg(feature = "parquet")]
async fn cmd_export(a: ExportArgs) -> anyhow::Result<()> {
    let opts = forge_store::ParquetOpts {
        row_group_rows: a.row_group_rows.max(1),
        ..Default::default()
    };
    let rows = forge_store::jsonl_to_parquet(&a.results, &a.out, opts)
        .await
        .context("exporting results to Parquet")?;
    println!(
        "exported {rows} row(s): {} -> {}",
        a.results.display(),
        a.out.display()
    );
    Ok(())
}

/// Derive a sibling artifact path by appending `suffix` to `base`
/// (`results.jsonl` + `.reject.jsonl` → `results.jsonl.reject.jsonl`), matching the
/// store's dead-letter naming so all of a run's outputs sit together.
fn sibling(base: &Path, suffix: &str) -> PathBuf {
    let mut s = base.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

async fn cmd_run(a: RunArgs) -> anyhow::Result<()> {
    let out = a.out.to_string_lossy().into_owned();
    let object_out = is_object_url(&out);
    ensure_object_store_build(&out, object_out)?;

    let queue = SqliteQueue::open(&a.checkpoint)?;
    // Record the output path (URL or local) so `resume` reuses it (avoids a split export).
    queue
        .record_job(&a.input.to_string_lossy(), &out)
        .context("recording job metadata")?;
    // Rejected (malformed / invalid custom_id) lines are preserved for repair: beside
    // the results for a local run, or beside the checkpoint when results go to a remote
    // object store (there is no local results file to sit next to).
    let reject_path = if object_out {
        sibling(&a.checkpoint, ".reject.jsonl")
    } else {
        sibling(&a.out, ".reject.jsonl")
    };
    // Optional prefix-bucket reorder: a bounded-RAM pass that groups the input by
    // (model, system-prompt) so an engine's automatic prefix cache stays hot. Writes a
    // reordered temp file beside the checkpoint and hydrates from that.
    let bucketed;
    let ingest_input: &Path = if a.prefix_bucket {
        bucketed = sibling(&a.checkpoint, ".bucketed.jsonl");
        let bs = forge_shard::bucket::bucket_jsonl(&a.input, &bucketed, 256)
            .await
            .context("prefix-bucketing input")?;
        tracing::info!(
            lines = bs.lines,
            buckets = bs.buckets_used,
            "prefix-bucketed input for cache-friendly ordering"
        );
        &bucketed
    } else {
        &a.input
    };
    let stats = forge_shard::ingest_jsonl_with_rejects(&queue, ingest_input, Some(&reject_path))
        .await
        .context("ingesting input JSONL")?;
    tracing::info!(
        lines = stats.lines_read,
        hydrated = stats.hydrated,
        rejected = stats.rejected,
        "ingested"
    );
    if stats.rejected > 0 {
        tracing::warn!(
            rejected = stats.rejected,
            file = %reject_path.display(),
            "some input lines were rejected; see the parse-error sidecar"
        );
    }

    let workers = a.workers.build_workers()?;
    let cfg = a.workers.run_config();
    let totals = if object_out {
        run_into_object_store(&out, &object_job_id(&a.checkpoint), queue, workers, cfg).await?
    } else {
        BatchRun::new(queue, workers, JsonlStore::new(&a.out))
            .with_config(cfg)
            .run()
            .await
            .context("running batch")?
    };

    println!(
        "done={} dead={} tokens_this_run={} (total items {})",
        totals.items_done,
        totals.items_dead,
        totals.tokens_total(),
        totals.items_total
    );
    Ok(())
}

/// Does `--out` name an object store (a `scheme://` prefix) rather than a local path?
/// `s3://` / `gs://` / `az://` / `file://` / `memory://` are object stores; a plain
/// path (`out.jsonl`, `/data/out.jsonl`) is the local JSONL sink.
fn is_object_url(out: &str) -> bool {
    match out.split_once("://") {
        Some((scheme, _)) => {
            !scheme.is_empty()
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        }
        None => false,
    }
}

/// Fail fast (before ingest) if an object-store `--out` is requested but this binary
/// was built without the `object_store` feature — the lean default writes local JSONL
/// only.
fn ensure_object_store_build(out: &str, object_out: bool) -> anyhow::Result<()> {
    if object_out && !cfg!(feature = "object_store") {
        anyhow::bail!(
            "object-store output ({out}) needs a `forge` built with `--features \
             object_store`; the default binary writes local JSONL only"
        );
    }
    Ok(())
}

/// A stable job id for object-store result namespacing (`{out}/results/{job}`), derived
/// from the checkpoint file name so `run` and `resume` (same `--checkpoint`) agree
/// without recording an extra field. Sanitized to a safe object-key component.
///
/// **Caveat — the id is only as unique as the checkpoint stem.** Two runs whose
/// checkpoints share a file stem (e.g. the default `state.db` → `"state"`) map to the
/// **same** `{out}/results/state/` prefix, so pointing both at the same `--out` root would
/// interleave their results under one job (and `objstore_open_run`'s single-job check
/// still passes). Distinct concurrent runs sharing an `--out` root therefore need distinct
/// checkpoint stems (or distinct `--out` prefixes); resume of the *same* run is unaffected.
fn object_job_id(checkpoint: &Path) -> String {
    let cleaned: String = checkpoint
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("forge")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "forge".to_string()
    } else {
        cleaned
    }
}

/// Run the batch into an object-store result sink (feature-gated). The default lean
/// binary never reaches this with a real store — [`ensure_object_store_build`] bails
/// first — but the stub keeps the call site compiling without the feature.
#[cfg(feature = "object_store")]
async fn run_into_object_store(
    out: &str,
    job: &str,
    queue: SqliteQueue,
    workers: Vec<HttpWorker>,
    cfg: RunConfig,
) -> anyhow::Result<JobTotals> {
    let store =
        forge_store::objstore_from_out(out, job).context("opening the object-store output")?;
    BatchRun::new(queue, workers, store)
        .with_config(cfg)
        .run()
        .await
        .context("running batch to object storage")
}

#[cfg(not(feature = "object_store"))]
async fn run_into_object_store(
    out: &str,
    _job: &str,
    _queue: SqliteQueue,
    _workers: Vec<HttpWorker>,
    _cfg: RunConfig,
) -> anyhow::Result<JobTotals> {
    anyhow::bail!("object-store output ({out}) needs `--features object_store`")
}

/// Sum token usage from object-store results — `forge cost --results <url>` (feature-gated).
#[cfg(feature = "object_store")]
async fn object_sum_usage(results: &str) -> anyhow::Result<forge_core::UsageTotals> {
    let run = forge_store::objstore_open_run(results)
        .await
        .context("opening the object-store results")?;
    run.sum_usage()
        .await
        .context("summing token usage from object storage")
}

#[cfg(not(feature = "object_store"))]
async fn object_sum_usage(results: &str) -> anyhow::Result<forge_core::UsageTotals> {
    anyhow::bail!("object-store results ({results}) need `--features object_store`")
}

/// Completeness sweep over object-store results — `forge verify --results <url>`
/// (feature-gated). Dump the terminal ids (the manifest — a marker per done AND dead id)
/// to a temp file, then run the existing exact, bounded-RAM sweep over it; no dead
/// sidecar, since the manifest already includes the dead ids.
#[cfg(feature = "object_store")]
async fn object_verify(
    input: &Path,
    results: &str,
    cfg: &forge_shard::VerifyConfig,
) -> anyhow::Result<forge_shard::VerifyReport> {
    let run = forge_store::objstore_open_run(results)
        .await
        .context("opening the object-store results")?;
    let ids = std::env::temp_dir().join(format!("forge-verify-ids-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&ids);
    run.dump_emitted_ids(&ids)
        .await
        .context("listing object-store result ids")?;
    let report = forge_shard::verify_completeness(input, &ids, None::<&Path>, cfg)
        .await
        .context("verifying completeness");
    let _ = std::fs::remove_file(&ids);
    report
}

#[cfg(not(feature = "object_store"))]
async fn object_verify(
    _input: &Path,
    results: &str,
    _cfg: &forge_shard::VerifyConfig,
) -> anyhow::Result<forge_shard::VerifyReport> {
    anyhow::bail!("object-store results ({results}) need `--features object_store`")
}

/// Pick the results file for a resume: prefer the path the original `run` recorded
/// in the checkpoint; fall back to `--out` only for older checkpoints without it.
fn resolve_resume_out(queue: &SqliteQueue, supplied: Option<&Path>) -> anyhow::Result<PathBuf> {
    match queue
        .job_output_uri()
        .context("reading recorded job metadata")?
    {
        Some(recorded) => {
            let recorded = PathBuf::from(recorded);
            if let Some(s) = supplied {
                if s != recorded {
                    tracing::warn!(
                        recorded = %recorded.display(),
                        supplied = %s.display(),
                        "ignoring --out; resuming into the original run's output file"
                    );
                }
            }
            Ok(recorded)
        }
        None => supplied.map(Path::to_path_buf).ok_or_else(|| {
            anyhow::anyhow!(
                "checkpoint has no recorded output path (created before job metadata) — pass --out"
            )
        }),
    }
}

async fn cmd_resume(a: ResumeArgs) -> anyhow::Result<()> {
    let queue = SqliteQueue::open(&a.checkpoint)?;
    let out = resolve_resume_out(&queue, a.out.as_deref())?;
    let out_str = out.to_string_lossy().into_owned();
    let object_out = is_object_url(&out_str);
    ensure_object_store_build(&out_str, object_out)?;

    let workers = a.workers.build_workers()?;
    let cfg = a.workers.run_config();
    let totals = if object_out {
        run_into_object_store(&out_str, &object_job_id(&a.checkpoint), queue, workers, cfg).await?
    } else {
        BatchRun::new(queue, workers, JsonlStore::new(&out))
            .with_config(cfg)
            .run()
            .await
            .context("resuming batch")?
    };

    println!(
        "done={} dead={} tokens_this_run={} (total items {})",
        totals.items_done,
        totals.items_dead,
        totals.tokens_total(),
        totals.items_total
    );
    Ok(())
}

/// Machine-readable run report (`forge status --json`). Surfaces the
/// queue cardinalities plus the observability signals batch users ask for:
/// a terminal success rate, the retry-cost distribution, and the failure mix.
#[derive(serde::Serialize)]
struct StatusReport {
    pending: u64,
    leased: u64,
    done: u64,
    dead: u64,
    total: u64,
    /// `done / (done + dead)` over terminal items; `null` until something is terminal.
    success_rate: Option<f64>,
    /// Terminal items that needed more than one attempt (the retry-cost headline).
    retried: u64,
    /// `attempts → count` over terminal items (done + dead-letter).
    attempts_histogram: std::collections::BTreeMap<u32, u64>,
    /// Dead-lettered items grouped by classified reason (validation / server_error
    /// / client_error / rate_limited / timeout / connection / other).
    failure_reasons: std::collections::BTreeMap<String, u64>,
}

/// Render the report as Prometheus text-exposition metrics.
fn prometheus(r: &StatusReport) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(s, "# HELP forge_items Items by queue/terminal state.");
    let _ = writeln!(s, "# TYPE forge_items gauge");
    for (state, v) in [
        ("pending", r.pending),
        ("leased", r.leased),
        ("done", r.done),
        ("dead", r.dead),
    ] {
        let _ = writeln!(s, "forge_items{{state=\"{state}\"}} {v}");
    }
    let _ = writeln!(s, "# HELP forge_failures Dead-lettered items by reason.");
    let _ = writeln!(s, "# TYPE forge_failures gauge");
    for (reason, v) in &r.failure_reasons {
        let _ = writeln!(s, "forge_failures{{reason=\"{reason}\"}} {v}");
    }
    let _ = writeln!(
        s,
        "# HELP forge_retried Terminal items that needed >1 attempt."
    );
    let _ = writeln!(s, "# TYPE forge_retried gauge");
    let _ = writeln!(s, "forge_retried {}", r.retried);
    if let Some(rate) = r.success_rate {
        let _ = writeln!(s, "# HELP forge_success_rate Terminal success ratio.");
        let _ = writeln!(s, "# TYPE forge_success_rate gauge");
        let _ = writeln!(s, "forge_success_rate {rate}");
    }
    s
}

async fn cmd_status(a: StatusArgs) -> anyhow::Result<()> {
    let queue = SqliteQueue::open(&a.checkpoint)?;
    let c = queue.counts().await.context("reading queue counts")?;
    let hist = queue
        .attempt_histogram()
        .await
        .context("reading attempt histogram")?;
    let failures = queue
        .failure_breakdown()
        .await
        .context("reading failure breakdown")?;

    let terminal = c.done + c.dead;
    let success_rate = (terminal > 0).then(|| c.done as f64 / terminal as f64);
    let retried: u64 = hist
        .iter()
        .filter(|(att, _)| *att > 1)
        .map(|(_, n)| n)
        .sum();
    let attempts_histogram: std::collections::BTreeMap<u32, u64> = hist.into_iter().collect();
    let failure_reasons: std::collections::BTreeMap<String, u64> = failures.into_iter().collect();

    let report = StatusReport {
        pending: c.pending,
        leased: c.leased,
        done: c.done,
        dead: c.dead,
        total: c.total(),
        success_rate,
        retried,
        attempts_histogram,
        failure_reasons,
    };

    if a.prometheus {
        print!("{}", prometheus(&report));
    } else if a.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        let rate = report
            .success_rate
            .map(|r| format!("{:.1}%", r * 100.0))
            .unwrap_or_else(|| "n/a".into());
        print!(
            "pending={} leased={} done={} dead={} total={} success_rate={} retried={}",
            report.pending,
            report.leased,
            report.done,
            report.dead,
            report.total,
            rate,
            report.retried,
        );
        if !report.failure_reasons.is_empty() {
            let parts: Vec<String> = report
                .failure_reasons
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            print!(" failures[{}]", parts.join(" "));
        }
        println!();
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct AuditReport {
    /// Never started — a fresh `resume` picks these up.
    pending: u64,
    /// Genuinely in flight (a worker holds an unexpired lease).
    live_leases: u64,
    /// Dropped by dead / spot-killed workers — a `resume`/`sweep` reclaims them.
    orphaned_leases: u64,
    done: u64,
    dead: u64,
    total: u64,
    /// `pending + orphaned_leases` — exactly what `resume` will re-dispatch, with no
    /// input⨝output join. Zero-loss: every reclaimed item is re-run, none double-run.
    reclaimable: u64,
    /// True when `orphaned_leases > 0` — the fingerprint of an interruption.
    interrupted: bool,
    /// `(leased_by, count)` for the live leases — who is still running.
    live_holders: std::collections::BTreeMap<String, u64>,
}

async fn cmd_audit(a: AuditArgs) -> anyhow::Result<()> {
    let queue = SqliteQueue::open(&a.checkpoint)?;
    // One locked snapshot, not two independent ones — a worker acking/leasing an
    // item between separate `counts()`/`resume_audit()` calls could otherwise make
    // `reclaimable` disagree with the DB state it's supposed to describe.
    let (c, audit) = queue
        .audit_snapshot(forge_core::now_ms())
        .await
        .context("reading audit snapshot")?;

    let reclaimable = c.pending + audit.orphaned_leases;
    let report = AuditReport {
        pending: c.pending,
        live_leases: audit.live_leases,
        orphaned_leases: audit.orphaned_leases,
        done: c.done,
        dead: c.dead,
        total: c.total(),
        reclaimable,
        interrupted: audit.orphaned_leases > 0,
        live_holders: audit.live_holders.into_iter().collect(),
    };

    if a.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!(
            "pending={} live_leases={} orphaned_leases={} done={} dead={} total={}",
            report.pending,
            report.live_leases,
            report.orphaned_leases,
            report.done,
            report.dead,
            report.total,
        );
        println!(
            "reclaimable={} ({} pending + {} orphaned) — `forge resume` re-dispatches these, join-free, zero-loss",
            report.reclaimable, report.pending, report.orphaned_leases,
        );
        if report.interrupted {
            println!(
                "INTERRUPTED: {} item(s) dropped by dead/spot-killed workers — run `forge resume` (or `forge sweep`) to reclaim",
                report.orphaned_leases,
            );
        }
        if !report.live_holders.is_empty() {
            let parts: Vec<String> = report
                .live_holders
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            println!("live workers still holding leases: {}", parts.join(" "));
        }
    }
    Ok(())
}

async fn cmd_sweep(a: SweepArgs) -> anyhow::Result<()> {
    let queue = SqliteQueue::open(&a.checkpoint)?;
    let requeued = queue.reap().await.context("sweeping expired leases")?;
    println!("re-queued {requeued} expired lease(s)");
    Ok(())
}

/// A subset-of-total percentage, `0.0` when the denominator is zero.
fn pct(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

async fn cmd_cost(a: CostArgs) -> anyhow::Result<()> {
    let results = a.results.to_string_lossy().into_owned();
    let object_results = is_object_url(&results);
    ensure_object_store_build(&results, object_results)?;
    let totals = if object_results {
        object_sum_usage(&results).await?
    } else {
        forge_store::sum_usage(&a.results)
            .await
            .context("summing token usage from results")?
    };
    // Prefer an explicit total; otherwise derive it from spot rate × hours × GPUs.
    let gpu_cost_usd = a.gpu_cost.or(match (a.gpu_usd_per_hour, a.gpu_hours) {
        (Some(rate), Some(hours)) => Some(rate * hours * a.gpus),
        _ => None,
    });
    let report = compute_cost(
        totals,
        CostInputs {
            gpu_cost_usd,
            online_per_mtok_input: a.online_per_mtok_input,
            online_per_mtok_output: a.online_per_mtok_output,
            online_per_mtok_cached_input: a.online_per_mtok_cached_input,
        },
    );

    if a.json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    println!(
        "items={} prompt_tokens={} completion_tokens={} total_tokens={}",
        report.items, report.prompt_tokens, report.completion_tokens, report.total_tokens,
    );
    if report.cached_tokens > 0 || report.reasoning_tokens > 0 {
        println!(
            "  of which: cached_input={} ({:.1}% of prompt) · reasoning={} ({:.1}% of completion)",
            report.cached_tokens,
            pct(report.cached_tokens, report.prompt_tokens),
            report.reasoning_tokens,
            pct(report.reasoning_tokens, report.completion_tokens),
        );
    }
    if let Some(cost) = report.forge_cost_usd {
        println!(
            "forge:  ${:.4}  (${:.4}/Mtok · {:.0} tokens/$)",
            cost,
            report.forge_usd_per_mtok.unwrap_or(0.0),
            report.tokens_per_usd.unwrap_or(0.0),
        );
    }
    if let Some(online) = report.online_cost_usd {
        print!("online: ${online:.4}");
        if let (Some(saved), Some(pct)) = (report.savings_usd, report.savings_pct) {
            print!("  →  saved ${saved:.4} ({pct:.1}%)");
        }
        println!();
    }
    if report.forge_cost_usd.is_none() && report.online_cost_usd.is_none() {
        println!(
            "(pass --gpu-cost (or --gpu-usd-per-hour --gpu-hours) and \
             --online-per-mtok-input for the cost-arbitrage figures)"
        );
    }
    Ok(())
}

async fn cmd_import(a: ImportArgs) -> anyhow::Result<()> {
    // Unusable / duplicate lines are preserved beside the output for repair.
    let reject = sibling(&a.out, ".reject.jsonl");
    let stats = forge_shard::import_batch(&a.input, &a.out, Some(&reject), a.endpoint.into())
        .await
        .context("importing batch file")?;
    println!(
        "imported {} -> {} (written={} rejected={} duplicates={})",
        a.input.display(),
        a.out.display(),
        stats.written,
        stats.rejected,
        stats.duplicates,
    );
    if stats.rejected + stats.duplicates > 0 {
        tracing::warn!(
            rejected = stats.rejected,
            duplicates = stats.duplicates,
            file = %reject.display(),
            "some lines were not imported; see the reject sidecar"
        );
    }
    Ok(())
}

async fn cmd_verify(a: VerifyArgs) -> anyhow::Result<()> {
    let results = a.results.to_string_lossy().into_owned();
    let object_results = is_object_url(&results);
    ensure_object_store_build(&results, object_results)?;

    let missing_out = if a.no_missing_out {
        None
    } else {
        // The missing-ids sidecar is always a local file; for object-store results there
        // is no local results file to sit beside, so default it next to the input.
        Some(a.missing_out.clone().unwrap_or_else(|| {
            let anchor: &Path = if object_results { &a.input } else { &a.results };
            sibling(anchor, ".missing.txt")
        }))
    };
    let cfg = forge_shard::VerifyConfig {
        run_capacity: a.run_capacity.max(1),
        missing_out: missing_out.clone(),
    };

    let report = if object_results {
        object_verify(&a.input, &results, &cfg).await?
    } else {
        // The store writes dead-letters to `<results>.dead.jsonl`; a dead-lettered id is
        // terminal, so it counts toward completeness.
        let dead = sibling(&a.results, ".dead.jsonl");
        forge_shard::verify_completeness(&a.input, &a.results, Some(&dead), &cfg)
            .await
            .context("verifying completeness")?
    };

    if a.json {
        // A compact, stable JSON line for CI/monitoring.
        let obj = serde_json::json!({
            "input_ids": report.input_ids,
            "emitted_ids": report.emitted_ids,
            "missing": report.missing,
            "extra": report.extra,
            "duplicate_input": report.duplicate_input,
            "rejected_input": report.rejected_input,
            "complete": report.is_complete(),
            "runs_spilled": report.runs_spilled,
        });
        println!("{}", serde_json::to_string(&obj)?);
    } else {
        println!(
            "input_ids={} emitted_ids={} missing={} extra={} duplicate_input={} rejected_input={}",
            report.input_ids,
            report.emitted_ids,
            report.missing,
            report.extra,
            report.duplicate_input,
            report.rejected_input,
        );
        if report.is_complete() {
            println!("complete: every input id has a terminal result ✅");
        } else if let Some(p) = &missing_out {
            println!(
                "INCOMPLETE: {} missing — ids written to {}",
                report.missing,
                p.display()
            );
        } else {
            println!("INCOMPLETE: {} missing", report.missing);
        }
    }

    // Non-zero exit exactly when something is missing — the CI/acceptance gate.
    if !report.is_complete() {
        anyhow::bail!("{} input id(s) have no terminal result", report.missing);
    }
    Ok(())
}
