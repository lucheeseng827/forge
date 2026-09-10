//! `forge` — the single static CLI binary, a thin driver over `forge-core`.
//!
//! Subcommands: `run` (hydrate + fan out), `resume` (continue from a checkpoint),
//! `status` (queue cardinalities + retry metrics), `sweep` (re-queue expired
//! leases), `import` (normalize an OpenAI/Anthropic/Bedrock batch file), `cost`
//! (the token cost-arbitrage report), and `verify` (bounded-RAM completeness sweep).
//! forge is a batch-inference work distributor — it has no DAG, cron, or fleet
//! provisioning.

mod hooks;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use forge_batch::{serve_batch, BatchConfig, HttpFleet};
use forge_core::{
    compute_cost, BatchRun, CostInputs, EndpointKind, EngineHint, ForgeError, JobTotals, Queue,
    ResponseCheck, RetryPolicy, RunConfig, WorkerSpec,
};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;

/// One `--workers` entry, resolved.
struct ParsedWorker {
    id: String,
    url: String,
    engine: Option<EngineArg>,
}

/// Parse a `--workers` entry: `[name=][engine:]url`.
///
/// Three shapes, in increasing specificity — the first is what every existing
/// invocation already writes, so nothing breaks:
///
/// - `http://gpu1:8000`                — fleet-wide `--engine`, auto id `w0`
/// - `sglang:http://gpu1:8000`         — this worker overrides the engine hint
/// - `spill=openai-api:https://api…`   — named, so it can take its own API key
///   via `FORGE_WORKER_API_KEY_SPILL`
///
/// **Why the engine prefix is unambiguous.** A URL contains a colon
/// (`http://…`), so splitting on the first one is only safe because the prefix is
/// accepted *only* when it names a known engine. `http` is not an engine, so
/// `http://gpu1:8000` parses as a bare URL — the check is what makes the syntax work
/// rather than a guess about how many colons a URL has.
///
/// The name, when given, becomes the `worker_id`: it shows up in every result line
/// and in the load-aware dispatch logs, and it selects that worker's API key.
fn parse_worker_entry(index: usize, entry: &str) -> anyhow::Result<ParsedWorker> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err(ForgeError::Config("empty --workers entry".into()).into());
    }

    // `name=` prefix. Split on the FIRST `=`, which cannot appear in the scheme or
    // host of a URL (a query string can hold one, but that follows a `?`).
    let (name, rest) = match entry.split_once('=') {
        Some((n, r)) if !n.is_empty() && !n.contains("//") && !n.contains(':') => {
            (Some(n.trim().to_string()), r.trim())
        }
        _ => (None, entry),
    };

    // `engine:` prefix — accepted only when the prefix names a known engine.
    let (engine, url) = match rest.split_once(':') {
        Some((maybe_engine, tail)) => match EngineArg::from_str_name(maybe_engine) {
            Some(e) => (Some(e), tail.to_string()),
            None => (None, rest.to_string()),
        },
        None => (None, rest.to_string()),
    };

    // An engine prefix consumed a colon, so what follows must still be a full URL.
    // Catching this here gives a usable message instead of a confusing connection
    // failure to a host called "gpu1".
    if engine.is_some() && !url.contains("://") {
        return Err(ForgeError::Config(format!(
            "--workers entry {entry:?}: after the engine prefix, {url:?} is not a URL \
             (expected something like `sglang:http://host:8000`)"
        ))
        .into());
    }

    if url.is_empty() {
        return Err(ForgeError::Config(format!("--workers entry {entry:?} has no URL")).into());
    }

    Ok(ParsedWorker {
        id: name.unwrap_or_else(|| format!("w{index}")),
        url,
        engine,
    })
}

/// Process exit codes. **This is a stability contract**, not an implementation
/// detail: orchestrators (Airflow, Dagster, Argo, a CronJob) branch on these, and an
/// automated retry decision depends on telling them apart. Adding a code is a minor
/// change; changing what an existing one means is breaking.
///
/// The distinction that matters operationally is `Incomplete` vs `Usage`: retrying an
/// incomplete run is correct and cheap (`forge resume` picks up where it stopped),
/// while retrying a malformed input burns the same GPU minutes to fail identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exit {
    /// Everything terminal and successful. Nothing to do.
    Complete = 0,
    /// A run-time failure (I/O, worker, store). Generic; retry at the operator's risk.
    Failed = 1,
    /// Work remains — items still pending or leased. **`forge resume` is the correct
    /// response**, and it is expected to succeed.
    Incomplete = 2,
    /// Bad configuration or bad input. Retrying is pointless; a human must change
    /// something. Never retry on this code.
    Usage = 3,
    /// Every item reached a terminal state, but some dead-lettered. The run is
    /// finished — this is a success with a caveat, not a failure — but a pipeline
    /// usually wants to alert.
    DeadLetters = 4,
}

impl From<Exit> for ExitCode {
    fn from(e: Exit) -> Self {
        ExitCode::from(e as u8)
    }
}

impl Exit {
    /// Classify a run's totals. Incompleteness outranks dead-letters: if work remains,
    /// the actionable next step is `resume`, whatever else happened.
    fn from_totals(t: &JobTotals) -> Self {
        if t.items_done + t.items_dead < t.items_total {
            Exit::Incomplete
        } else if t.items_dead > 0 {
            Exit::DeadLetters
        } else {
            Exit::Complete
        }
    }
}

/// Map an error to an exit code. `Config` and `Ingest` are the "you gave me something
/// wrong" family, and are the ones a pipeline must not retry.
fn exit_for_error(err: &anyhow::Error) -> Exit {
    match err.downcast_ref::<ForgeError>() {
        Some(ForgeError::Config(_)) | Some(ForgeError::Ingest(_)) => Exit::Usage,
        // A file that is not there will not be there on the retry either — an
        // orchestrator that treats this as transient burns a slot to fail identically.
        Some(ForgeError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Exit::Usage,
        _ => Exit::Failed,
    }
}

#[derive(Parser)]
#[command(
    name = "forge",
    version,
    about = "Offline/batch inference coordinator — fan a giant JSONL across N workers: any OpenAI-compatible engine (vLLM/SGLang/llama.cpp/…) or hosted provider endpoint. NOT a workflow/DAG scheduler."
)]
struct Cli {
    /// Log rendering: `text` for a terminal, `json` for a log shipper (one object per
    /// line). Also settable as `FORGE_LOG_FORMAT`, which is what a Deployment sets once
    /// rather than on every command. `RUST_LOG` still selects levels; this only changes
    /// how they are printed.
    ///
    /// The env var is read by hand rather than through clap's `env` feature: that
    /// feature renders `[env: VAR=<value>]` into `--help` for every argument that uses
    /// it, which is how a secret ended up in help output once already. Not worth
    /// enabling repo-wide for one cosmetic knob.
    #[arg(long, global = true)]
    log_format: Option<forge_core::LogFormat>,
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
    /// Put dead-lettered items back on the queue of the SAME checkpoint, and prune
    /// their records from `<out>.dead.jsonl` so the re-run's results are actually
    /// kept. Terminal state is reversed deliberately and only for items you name;
    /// nothing is scheduled and no item relationship is created.
    Requeue(RequeueArgs),
    /// Normalize an OpenAI/Anthropic/Bedrock batch file into forge-native JSONL.
    Import(ImportArgs),
    /// Filter an input JSONL down to a list of `custom_id`s — the return leg after a
    /// partly-failed run. Takes `verify`'s `.missing.txt` or the store's `.dead.jsonl`
    /// directly. A stateless file filter: it schedules nothing and records nothing.
    Subset(SubsetArgs),
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

impl Cmd {
    /// The subcommand's name, for the startup log line.
    fn name(&self) -> &'static str {
        match self {
            Cmd::Run(_) => "run",
            Cmd::Resume(_) => "resume",
            Cmd::Status(_) => "status",
            Cmd::Audit(_) => "audit",
            Cmd::Sweep(_) => "sweep",
            Cmd::Requeue(_) => "requeue",
            Cmd::Import(_) => "import",
            Cmd::Subset(_) => "subset",
            Cmd::Cost(_) => "cost",
            Cmd::Verify(_) => "verify",
            Cmd::ServeBatch(_) => "serve-batch",
            #[cfg(feature = "parquet")]
            Cmd::Export(_) => "export",
        }
    }
}

/// Outbound notification knobs, shared by `run` and `resume`.
///
/// These are **outbound only** — forge reporting on its own run. They are not the
/// inbound cron/triggers `README.md` rules out: nothing here decides what runs next.
/// See `hooks.rs` for the boundary argument in full.
#[derive(clap::Args, Clone, Debug)]
struct HookArgs {
    /// Notify when the run reaches a terminal state. Either an `http(s)://` URL (the
    /// JSON status is POSTed — the only form that works in the `FROM scratch` image,
    /// which has no shell) or a program to execute with the JSON on stdin.
    #[arg(long, value_name = "URL_OR_COMMAND")]
    on_complete: Option<String>,
    /// Notify once if the run stops making progress for `--stall-secs`. This is the
    /// 02:00 case: a spot fleet died and nobody is watching.
    #[arg(long, value_name = "URL_OR_COMMAND")]
    on_stall: Option<String>,
    /// Seconds without a newly-terminal item before `--on-stall` fires. Only read
    /// when `--on-stall` is set.
    #[arg(long, default_value_t = 900)]
    stall_secs: u64,
}

impl HookArgs {
    fn complete_hook(&self) -> anyhow::Result<Option<hooks::Hook>> {
        self.on_complete
            .as_deref()
            .map(|t| {
                hooks::Hook::parse(t).map_err(|e| ForgeError::Config(format!("--on-complete: {e}")))
            })
            .transpose()
            .map_err(Into::into)
    }

    fn stall_hook(&self) -> anyhow::Result<Option<hooks::Hook>> {
        self.on_stall
            .as_deref()
            .map(|t| {
                hooks::Hook::parse(t).map_err(|e| ForgeError::Config(format!("--on-stall: {e}")))
            })
            .transpose()
            .map_err(Into::into)
    }
}

/// Worker fleet + dispatch knobs, shared by `run` and `resume`.
#[derive(clap::Args)]
struct WorkerArgs {
    /// Comma-separated worker entries: `[name=][engine:]url`.
    ///
    /// A bare URL uses the fleet-wide `--engine`. Prefix an entry with an engine name
    /// to override it for that worker only, and with `name=` to set its worker id —
    /// which appears in every result line and selects that worker's API key via
    /// `FORGE_WORKER_API_KEY_<NAME>` (falling back to `FORGE_WORKER_API_KEY`).
    ///
    /// One run can therefore span a self-hosted fleet and a hosted API, which is what
    /// lets work spill onto a hosted endpoint when spot capacity disappears:
    ///
    ///   --workers 'http://gpu1:8000,sglang:http://gpu2:8000,spill=openai-api:https://api.openai.com/v1'
    #[arg(long, value_delimiter = ',', required = true)]
    workers: Vec<String>,
    /// Engine behind the workers (health-probe shape / concurrency knob only).
    #[arg(long, value_enum, default_value_t = EngineArg::Vllm)]
    engine: EngineArg,
    /// Endpoint kind every item hits.
    #[arg(long, value_enum, default_value_t = EndpointArg::Chat)]
    endpoint: EndpointArg,
    /// Per-worker in-flight cap (= the engine's max-num-seqs / --parallel).
    /// `run`/`serve-batch` default to 256 when omitted; `resume` defaults to the
    /// value the original `run` recorded in the checkpoint, so a bare resume can
    /// never restart AIMD at a ceiling the fleet was not configured for.
    #[arg(long)]
    concurrency: Option<usize>,
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
    #[command(flatten)]
    hooks: HookArgs,
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
    #[command(flatten)]
    hooks: HookArgs,
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
struct RequeueArgs {
    /// The run's checkpoint. Items go back on THIS queue — the point of the command is
    /// to keep the job, its ingest checkpoint and its results file intact, rather than
    /// starting a second job whose output you then have to merge.
    #[arg(long)]
    checkpoint: PathBuf,
    /// The run's results file, exactly as passed to `run --out`. Its sibling
    /// `<out>.dead.jsonl` is what gets pruned.
    ///
    /// **Required, and not inferable.** The store seeds its emitted-id set from the
    /// dead-letter file, so an item requeued while its dead record still stands is
    /// re-leased, re-run against a real GPU, and has its fresh result silently
    /// discarded. Requeueing without pruning is not a partial success; it is a way to
    /// spend money and get nothing.
    #[arg(long)]
    out: PathBuf,
    /// Requeue only these ids. Accepts either shape forge emits: bare ids one per line
    /// (`verify`'s `.missing.txt`) or JSONL records carrying `custom_id` (the store's
    /// `.dead.jsonl`) — the same input `subset` takes.
    #[arg(long, conflicts_with_all = ["all", "reason"])]
    ids: Option<PathBuf>,
    /// Requeue every dead-lettered item in the checkpoint.
    #[arg(long, default_value_t = false, conflicts_with_all = ["ids", "reason"])]
    all: bool,
    /// Requeue only dead-letters whose recorded error contains this substring — e.g.
    /// `--reason timeout` after fixing an engine that was falling over, while leaving
    /// the HTTP 4xx items (bad requests, which retrying cannot fix) quarantined.
    #[arg(long, conflicts_with_all = ["ids", "all"])]
    reason: Option<String>,
    /// Print what would be requeued and change nothing.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// Machine-readable JSON output.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(clap::Args)]
struct SubsetArgs {
    /// The original input JSONL to filter.
    #[arg(long)]
    input: PathBuf,
    /// Ids to select. Accepts either shape forge emits: bare ids one per line
    /// (`forge verify`'s `.missing.txt`) or JSONL records carrying `custom_id`
    /// (the store's `.dead.jsonl`). Blank lines and `#` comments are ignored.
    #[arg(long)]
    ids: PathBuf,
    /// Where to write the filtered JSONL.
    #[arg(long)]
    out: PathBuf,
    /// Emit the lines NOT in the id list, instead of the ones that are.
    #[arg(long, default_value_t = false)]
    invert: bool,
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
    /// A hosted OpenAI-compatible API (probe skipped; FORGE_WORKER_API_KEY → Bearer).
    OpenaiApi,
    /// A hosted Anthropic-compatible Messages API (probe skipped;
    /// FORGE_WORKER_API_KEY → x-api-key + anthropic-version).
    AnthropicApi,
}

impl EngineArg {
    /// Match a `--workers` engine prefix. Kept in lockstep with the clap value names
    /// so `sglang:` and `--engine sglang` mean the same thing.
    fn from_str_name(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "vllm" => EngineArg::Vllm,
            "sglang" => EngineArg::Sglang,
            "llamacpp" | "llama-cpp" | "llama_cpp" => EngineArg::Llamacpp,
            "router" => EngineArg::Router,
            "openai-api" | "openai_api" | "openai" => EngineArg::OpenaiApi,
            "anthropic-api" | "anthropic_api" | "anthropic" => EngineArg::AnthropicApi,
            _ => return None,
        })
    }
}

impl From<EngineArg> for EngineHint {
    fn from(e: EngineArg) -> Self {
        match e {
            EngineArg::Vllm => EngineHint::Vllm,
            EngineArg::Sglang => EngineHint::Sglang,
            EngineArg::Llamacpp => EngineHint::LlamaCpp,
            EngineArg::OpenaiApi => EngineHint::OpenAiApi,
            EngineArg::AnthropicApi => EngineHint::AnthropicApi,
            EngineArg::Router => EngineHint::Router,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum EndpointArg {
    Chat,
    Completions,
    Embeddings,
    /// Anthropic-native Messages (/v1/messages) — items carry the Messages body shape.
    Messages,
}

impl From<EndpointArg> for EndpointKind {
    fn from(e: EndpointArg) -> Self {
        match e {
            EndpointArg::Chat => EndpointKind::Chat,
            EndpointArg::Completions => EndpointKind::Completions,
            EndpointArg::Messages => EndpointKind::Messages,
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
    fn worker_specs(&self, concurrency: usize) -> anyhow::Result<Vec<WorkerSpec>> {
        // NOT infallible: the comment here used to claim `build_workers` surfaces the
        // error before this runs, but `serve-batch` calls this directly and never
        // reaches `build_workers`. With `.ok()` a failing entry was silently dropped —
        // `--workers 'http://a,,http://b'` served a two-worker fleet from a
        // three-entry flag with no message. The emptiness check upstream counts the raw
        // strings, not the resolved specs, so an all-bad flag would have served with no
        // fleet at all.
        self.workers
            .iter()
            .enumerate()
            .map(|(i, entry)| self.spec_for(i, entry, concurrency))
            .collect()
    }

    /// Resolve one `--workers` entry into a [`WorkerSpec`].
    fn spec_for(&self, i: usize, entry: &str, concurrency: usize) -> anyhow::Result<WorkerSpec> {
        let parsed = parse_worker_entry(i, entry)?;
        Ok(WorkerSpec::new(parsed.id, parsed.url, self.endpoint.into())
            .concurrency(concurrency)
            // A per-entry hint wins; otherwise the fleet-wide `--engine` applies,
            // so a single-engine fleet is written exactly as before.
            .engine_hint(parsed.engine.map_or_else(|| self.engine.into(), Into::into)))
    }

    fn build_workers(&self, concurrency: usize) -> anyhow::Result<Vec<HttpWorker>> {
        self.workers
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let spec = self.spec_for(i, entry, concurrency)?;
                let url = spec.base_url.clone();
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
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // Flag beats env beats default. An unparseable env var is a warning, not a fatal:
    // refusing to start a six-hour batch over a log-rendering preference would be a
    // worse outcome than printing text.
    let format = cli
        .log_format
        .unwrap_or_else(|| match std::env::var("FORGE_LOG_FORMAT") {
            Ok(v) => v.parse().unwrap_or_else(|e| {
                eprintln!("WARN {e}; falling back to text logs");
                forge_core::LogFormat::Text
            }),
            Err(_) => forge_core::LogFormat::Text,
        });
    forge_core::init_logging(format, cli.cmd.name());

    let result = match cli.cmd {
        Cmd::Run(a) => cmd_run(a).await,
        Cmd::Resume(a) => cmd_resume(a).await,
        Cmd::Status(a) => cmd_status(a).await.map(|()| Exit::Complete),
        Cmd::Audit(a) => cmd_audit(a).await.map(|()| Exit::Complete),
        Cmd::Sweep(a) => cmd_sweep(a).await.map(|()| Exit::Complete),
        Cmd::Requeue(a) => cmd_requeue(a).await.map(|()| Exit::Complete),
        Cmd::Import(a) => cmd_import(a).await.map(|()| Exit::Complete),
        Cmd::Subset(a) => cmd_subset(a).await.map(|()| Exit::Complete),
        Cmd::Cost(a) => cmd_cost(a).await.map(|()| Exit::Complete),
        Cmd::Verify(a) => cmd_verify(a).await,
        Cmd::ServeBatch(a) => cmd_serve_batch(a).await.map(|()| Exit::Complete),
        #[cfg(feature = "parquet")]
        Cmd::Export(a) => cmd_export(a).await.map(|()| Exit::Complete),
    };

    match result {
        Ok(code) => code.into(),
        Err(e) => {
            // Match anyhow's own top-level rendering (`{:?}` prints the cause chain),
            // which is what operators are used to reading in logs.
            eprintln!("Error: {e:?}");
            exit_for_error(&e).into()
        }
    }
}

/// Serve the OpenAI Batch REST front door over the configured worker fleet. Blocks until
/// Ctrl-C, then stops the accept threads. In-flight batch runs are not force-joined —
/// their results are already durable in each batch's `ckpt.db` + `out.jsonl`.
/// Wait for a shutdown signal — **SIGTERM as well as Ctrl-C**.
///
/// SIGTERM is the one that matters and the one that was missing. Kubernetes sends it
/// on eviction, rollout and scale-down; `docker stop` sends it; systemd sends it; a
/// spot-instance reclaim sends it after the ACPI soft-off. SIGINT (Ctrl-C) only ever
/// arrives from a human at a terminal — which is the case that was already handled and
/// the one that matters least, because a human can see what happened.
///
/// Waiting on Ctrl-C alone means the graceful path never runs anywhere it counts: the
/// process sits until the grace period expires and is then SIGKILLed mid-write.
///
/// Non-unix targets get Ctrl-C only, which is all those platforms have here.
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                // Losing SIGTERM handling is worth saying out loud: it silently turns
                // every orchestrated stop back into a hard kill.
                tracing::warn!(error = %e, "could not install a SIGTERM handler; only Ctrl-C will drain");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT";
            }
        };
        tokio::select! {
            _ = term.recv() => "SIGTERM",
            r = tokio::signal::ctrl_c() => {
                if let Err(e) = r {
                    tracing::warn!(error = %e, "ctrl-c listener failed");
                }
                "SIGINT"
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl-c listener failed");
        }
        "SIGINT"
    }
}

/// Flip `flag` on the first shutdown signal, in a background task.
///
/// Returns the join handle so the caller can abort it — a run that finishes normally
/// must not leave a signal listener alive holding the process open.
///
/// **One signal means drain, and a second does NOT mean die.** A drain finishes only
/// the items already in flight; an impatient operator's second Ctrl-C would otherwise
/// throw away exactly the work the first one was protecting. The escape hatch is the
/// one the OS already provides and that no program can intercept: SIGKILL. That is
/// safe here precisely because the queue is crash-safe — leases expire and the reaper
/// reclaims them — so a hard kill costs GPU minutes, never correctness.
fn spawn_drain_on_signal(flag: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let sig = shutdown_signal().await;
        eprintln!(
            "{sig} received — draining: no new work will be leased, in-flight items \
             will finish. Send SIGKILL to abort immediately (leases expire and are \
             reclaimed, so nothing is lost but the GPU time)."
        );
        flag.store(true, Ordering::SeqCst);
    })
}

async fn cmd_serve_batch(a: ServeBatchArgs) -> anyhow::Result<()> {
    if a.workers.workers.is_empty() {
        anyhow::bail!("serve-batch needs at least one --workers endpoint for the fleet");
    }
    let specs = a
        .workers
        .worker_specs(a.workers.concurrency.unwrap_or(DEFAULT_CONCURRENCY))?;
    let fleet = HttpFleet::new(specs, a.workers.retry_policy(), a.workers.require.into());
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
    let sig = shutdown_signal().await;
    println!("{sig} received — shutting down: cancelling and joining running batches");
    // `shutdown` joins every per-batch supervisor, so results already written stay and
    // each batch's `ckpt.db` is left resumable. Without SIGTERM here this never ran
    // under any orchestrator — the process was simply SIGKILLed after the grace period.
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

async fn cmd_run(a: RunArgs) -> anyhow::Result<Exit> {
    let out = a.out.to_string_lossy().into_owned();
    let object_out = is_object_url(&out);
    ensure_object_store_build(&out, object_out)?;
    // Parse hook targets BEFORE any work: a typo in --on-complete should fail in the
    // first millisecond as a usage error, not after a six-hour run.
    let complete_hook = a.hooks.complete_hook()?;
    let stall_hook = a.hooks.stall_hook()?;
    // Fail fast on an object-store input the same way `--out` does: before ingest,
    // before anything is written, and with an actionable message.
    let input_raw = a.input.to_string_lossy().into_owned();
    let object_in = is_object_url(&input_raw);
    if object_in && !cfg!(feature = "object_store") {
        return Err(ForgeError::Config(format!(
            "object-store input ({input_raw}) needs a `forge` built with `--features \
             object_store`; the default binary reads local files only"
        ))
        .into());
    }
    // `--prefix-bucket` writes a reordered temp file and re-reads it by path, so it has
    // no meaning for a stream. Refusing is better than silently ignoring the flag and
    // leaving the operator to wonder why their cache-hit rate did not move.
    if object_in && a.prefix_bucket {
        return Err(ForgeError::Config(
            "--prefix-bucket cannot be combined with an object-store --input: it is a \
             local reordering pass over a seekable file. Stage the input locally to use \
             it, or drop the flag."
                .to_string(),
        )
        .into());
    }

    let queue = SqliteQueue::open(&a.checkpoint)?;
    let concurrency = a.workers.concurrency.unwrap_or(DEFAULT_CONCURRENCY);
    // Record the output path (URL or local) AND the fleet's per-worker concurrency
    // so `resume` reuses both (avoids a split export, and a bare resume can never
    // restart AIMD at a ceiling the fleet was not configured for).
    queue
        .record_job(&a.input.to_string_lossy(), &out, concurrency)
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
    let stats = ingest_any(&queue, ingest_input, &reject_path)
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

    let workers = a.workers.build_workers(concurrency)?;
    let cfg = a.workers.run_config();
    let watchdog = stall_hook.map(|h| spawn_stall_watchdog(&a.checkpoint, h, a.hooks.stall_secs));
    // The graceful-drain flag: one signal listener, shared by both output paths.
    let drain = Arc::new(AtomicBool::new(false));
    let signals = spawn_drain_on_signal(Arc::clone(&drain));
    let totals = if object_out {
        run_into_object_store(
            &out,
            &object_job_id(&a.checkpoint),
            queue,
            workers,
            cfg,
            Arc::clone(&drain),
        )
        .await?
    } else {
        BatchRun::new(queue, workers, JsonlStore::new(&a.out))
            .with_config(cfg)
            .with_shutdown(Arc::clone(&drain))
            .run()
            .await
            .context("running batch")?
    };
    // A finished run must not be held open by a listener waiting for a signal.
    signals.abort();
    if let Some(w) = watchdog {
        w.abort();
    }

    println!(
        "done={} dead={} tokens_this_run={} (total items {})",
        totals.items_done,
        totals.items_dead,
        totals.tokens_total(),
        totals.items_total
    );
    let exit = Exit::from_totals(&totals);
    if let Some(h) = complete_hook {
        h.fire(&terminal_payload(&totals, exit, &a.checkpoint))
            .await;
    }
    Ok(exit)
}

/// Hydrate the queue from `input`, which may be a local path OR an object-store URL
/// (`s3://` / `gs://` / `az://`). Both paths run the SAME line-oriented ingest — the
/// object-store case only supplies a different reader — so batching, the reject
/// sidecar, and the `Shard` bookkeeping that makes a re-run seek instead of rescan are
/// identical either way.
async fn ingest_any<Q: Queue>(
    queue: &Q,
    input: &Path,
    reject_path: &Path,
) -> anyhow::Result<forge_shard::IngestStats> {
    let raw = input.to_string_lossy().into_owned();
    if !is_object_url(&raw) {
        return Ok(forge_shard::ingest_jsonl_with_rejects(queue, input, Some(reject_path)).await?);
    }
    #[cfg(not(feature = "object_store"))]
    {
        let _ = reject_path;
        Err(ForgeError::Config(format!(
            "object-store input ({raw}) needs a `forge` built with `--features object_store`;              the default lean binary reads local files only. Rebuild with              `cargo build -p forge-cli --release --features object_store`."
        ))
        .into())
    }
    #[cfg(feature = "object_store")]
    {
        // The caller owns positioning for the reader-based ingest, so ask the queue
        // where it got to and let the store tell us where it could actually start
        // (a truncated/replaced object rewinds to 0 rather than reading an empty range).
        let (offset, line) = queue.hydrated_through().await?.unwrap_or((0, 0));
        let (reader, start) = forge_store::open_input_stream(&raw, offset).await?;
        let start_line = if start == offset { line } else { 0 };
        Ok(forge_shard::ingest_reader_with_rejects(
            queue,
            reader,
            start,
            start_line,
            Some(reject_path),
        )
        .await?)
    }
}

/// Spawn a background watchdog that fires `hook` once if the run stops making
/// progress. It polls a SECOND read-only handle on the same checkpoint — safe because
/// the queue runs in WAL mode with a 60s busy timeout (`forge-queue/src/sqlite.rs`),
/// which is the same way `forge status` reads a checkpoint under an active run.
///
/// "Progress" is a newly-terminal item (done + dead). A run that is merely slow does
/// not stall; a run whose fleet has died does, because nothing reaches terminal.
/// Fires at most once — a dead fleet must not become a notification loop.
fn spawn_stall_watchdog(
    checkpoint: &Path,
    hook: hooks::Hook,
    stall_secs: u64,
) -> tokio::task::JoinHandle<()> {
    let path = checkpoint.to_path_buf();
    // Poll often enough to be responsive, rarely enough to stay invisible next to the
    // run's own queue traffic, and never longer than the stall budget itself.
    let poll = Duration::from_secs(stall_secs.clamp(1, 30));
    tokio::spawn(async move {
        let queue = match SqliteQueue::open(&path) {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!(error = %e, "stall watchdog could not open the checkpoint; disabled");
                return;
            }
        };
        let mut last_terminal = u64::MAX;
        let mut since = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(poll).await;
            let Ok(c) = queue.counts().await else {
                continue;
            };
            let terminal = c.done + c.dead;
            if last_terminal == u64::MAX || terminal > last_terminal {
                last_terminal = terminal;
                since = tokio::time::Instant::now();
                continue;
            }
            if since.elapsed() >= Duration::from_secs(stall_secs) {
                let payload = serde_json::json!({
                    "event": "stall",
                    "checkpoint": path.to_string_lossy(),
                    "stalled_secs": since.elapsed().as_secs(),
                    "pending": c.pending,
                    "leased": c.leased,
                    "done": c.done,
                    "dead": c.dead,
                    "total": c.total(),
                    "hint": "no item reached a terminal state in this window — the fleet may be gone; `forge audit` shows what a resume would reclaim",
                });
                hook.fire(&payload).await;
                return; // once only
            }
        }
    })
}

/// The JSON handed to `--on-complete`, and the same shape a pipeline gets from
/// `status --json` plus the run's outcome.
fn terminal_payload(totals: &JobTotals, exit: Exit, checkpoint: &Path) -> serde_json::Value {
    serde_json::json!({
        "event": "complete",
        "checkpoint": checkpoint.to_string_lossy(),
        "exit_code": exit as u8,
        "outcome": match exit {
            Exit::Complete => "complete",
            Exit::DeadLetters => "complete_with_dead_letters",
            Exit::Incomplete => "incomplete",
            _ => "unknown",
        },
        "items_total": totals.items_total,
        "items_done": totals.items_done,
        "items_dead": totals.items_dead,
        "tokens_total": totals.tokens_total(),
    })
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
    drain: Arc<AtomicBool>,
) -> anyhow::Result<JobTotals> {
    let store =
        forge_store::objstore_from_out(out, job).context("opening the object-store output")?;
    BatchRun::new(queue, workers, store)
        .with_config(cfg)
        .with_shutdown(drain)
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
    _drain: Arc<AtomicBool>,
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

/// The per-worker in-flight cap used when neither a flag nor recorded job
/// metadata supplies one.
const DEFAULT_CONCURRENCY: usize = 256;

/// Pick the per-worker concurrency for a resume. An explicit `--concurrency` wins
/// (the fleet may genuinely have changed), with a warning when it diverges from
/// what the original `run` recorded; otherwise the recorded value is reused. Only
/// a checkpoint from an older build (no recorded value) falls back to the flag
/// default — loudly, because restarting AIMD at a ceiling the fleet was never
/// configured for is how valid items end up dead-lettered in a 429 storm.
fn resolve_resume_concurrency(
    queue: &SqliteQueue,
    supplied: Option<usize>,
) -> anyhow::Result<usize> {
    let recorded = queue
        .job_concurrency()
        .context("reading recorded job metadata")?;
    Ok(match (supplied, recorded) {
        (Some(s), Some(r)) => {
            if s != r {
                tracing::warn!(
                    supplied = s,
                    recorded = r,
                    "overriding the checkpoint's recorded --concurrency"
                );
            }
            s
        }
        (Some(s), None) => s,
        (None, Some(r)) => r,
        (None, None) => {
            tracing::warn!(
                default = DEFAULT_CONCURRENCY,
                "checkpoint has no recorded concurrency (older build); using the default — pass --concurrency to match your fleet"
            );
            DEFAULT_CONCURRENCY
        }
    })
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

async fn cmd_resume(a: ResumeArgs) -> anyhow::Result<Exit> {
    // Parsed first: a bad hook target is a usage error, not a post-run surprise.
    let complete_hook = a.hooks.complete_hook()?;
    let stall_hook = a.hooks.stall_hook()?;
    let watchdog = stall_hook.map(|h| spawn_stall_watchdog(&a.checkpoint, h, a.hooks.stall_secs));
    let queue = SqliteQueue::open(&a.checkpoint)?;
    let out = resolve_resume_out(&queue, a.out.as_deref())?;
    let out_str = out.to_string_lossy().into_owned();
    let object_out = is_object_url(&out_str);
    ensure_object_store_build(&out_str, object_out)?;

    let concurrency = resolve_resume_concurrency(&queue, a.workers.concurrency)?;
    let workers = a.workers.build_workers(concurrency)?;
    let cfg = a.workers.run_config();
    // The graceful-drain flag: one signal listener, shared by both output paths.
    let drain = Arc::new(AtomicBool::new(false));
    let signals = spawn_drain_on_signal(Arc::clone(&drain));
    let totals = if object_out {
        run_into_object_store(
            &out_str,
            &object_job_id(&a.checkpoint),
            queue,
            workers,
            cfg,
            Arc::clone(&drain),
        )
        .await?
    } else {
        BatchRun::new(queue, workers, JsonlStore::new(&out))
            .with_config(cfg)
            .with_shutdown(Arc::clone(&drain))
            .run()
            .await
            .context("resuming batch")?
    };
    // A finished run must not be held open by a listener waiting for a signal.
    signals.abort();
    if let Some(w) = watchdog {
        w.abort();
    }

    println!(
        "done={} dead={} tokens_this_run={} (total items {})",
        totals.items_done,
        totals.items_dead,
        totals.tokens_total(),
        totals.items_total
    );
    let exit = Exit::from_totals(&totals);
    if let Some(h) = complete_hook {
        h.fire(&terminal_payload(&totals, exit, &a.checkpoint))
            .await;
    }
    Ok(exit)
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

/// Put dead-lettered items back on the queue, and prune the store records that would
/// otherwise throw their re-run results away.
///
/// **Order is the correctness argument, not a detail.** The store is pruned FIRST and
/// the queue flipped SECOND. Reversed, a crash between the two leaves items runnable
/// whose dead records still stand — they re-run against a real GPU and `put`
/// short-circuits their results into the void, silently. In this order the crash window
/// leaves items still marked dead whose records are already gone, which `forge verify`
/// reports as missing and which re-running this same command heals (both halves are
/// idempotent, and the requeue is driven by the QUEUE, not by the file that was pruned).
async fn cmd_requeue(a: RequeueArgs) -> anyhow::Result<()> {
    let out = a.out.to_string_lossy().into_owned();
    if is_object_url(&out) {
        anyhow::bail!(
            "--out {out} is an object-store URL. Requeue prunes the dead-letter sidecar \
             in place, which this command only does for a local file. Copy the run's \
             dead-letter object down, requeue against a local --out, and re-upload — or \
             rebuild a runnable input with `forge subset` and run it as a new job."
        );
    }
    if a.ids.is_none() && !a.all && a.reason.is_none() {
        anyhow::bail!(
            "nothing selected: pass --ids <file>, --reason <substring>, or --all. \
             Requeue reverses a terminal state, so it never guesses at the scope."
        );
    }

    let queue = SqliteQueue::open(&a.checkpoint).context("opening checkpoint")?;

    // A live coordinator is a hard refusal, not a warning. It holds the store's
    // emitted-id set in MEMORY, seeded when it started; pruning the file underneath it
    // changes nothing it can see, so every requeued item would still have its result
    // discarded — the exact failure this command exists to prevent, made invisible by
    // appearing to succeed.
    let (_counts, audit) = queue
        .audit_snapshot(forge_core::now_ms())
        .await
        .context("reading checkpoint state")?;
    if audit.live_leases > 0 {
        let holders: Vec<String> = audit
            .live_holders
            .iter()
            .map(|(w, n)| format!("{w} ({n})"))
            .collect();
        anyhow::bail!(
            "refusing to requeue while a coordinator is running on this checkpoint: \
             {} live lease(s) held by {}. That process cached the emitted-id set at \
             startup, so pruning the dead-letter file now would not reach it and every \
             requeued item's result would still be discarded. Stop the run, then requeue.",
            audit.live_leases,
            if holders.is_empty() {
                "an unnamed worker".to_string()
            } else {
                holders.join(", ")
            }
        );
    }

    let dead = queue.dead_letters().await.context("listing dead-letters")?;
    let selected: Vec<String> = if a.all {
        dead.iter().map(|(id, _)| id.clone()).collect()
    } else if let Some(reason) = &a.reason {
        dead.iter()
            .filter(|(_, e)| e.as_deref().is_some_and(|e| e.contains(reason.as_str())))
            .map(|(id, _)| id.clone())
            .collect()
    } else {
        let wanted = forge_shard::load_ids(a.ids.as_ref().unwrap())
            .await
            .context("reading --ids")?;
        dead.iter()
            .filter(|(id, _)| wanted.contains(id))
            .map(|(id, _)| id.clone())
            .collect()
    };

    if selected.is_empty() {
        // Not an error: "nothing matched" is a legitimate and common answer, and
        // exiting non-zero here would break a cron that requeues timeouts nightly.
        if a.json {
            println!(
                "{}",
                serde_json::json!({
                    "dead_total": dead.len(), "selected": 0, "requeued": 0,
                    "pruned": 0, "dry_run": a.dry_run,
                })
            );
        } else {
            println!(
                "nothing to requeue: {} dead-lettered item(s) in {}, none matched the \
                 selection",
                dead.len(),
                a.checkpoint.display()
            );
        }
        return Ok(());
    }

    let dead_path = forge_store::JsonlStore::new(&a.out)
        .dead_path()
        .to_path_buf();
    if a.dry_run {
        if a.json {
            println!(
                "{}",
                serde_json::json!({
                    "dead_total": dead.len(), "selected": selected.len(),
                    "requeued": 0, "pruned": 0, "dry_run": true,
                    "ids": selected, "dead_file": dead_path.to_string_lossy(),
                })
            );
        } else {
            println!(
                "would requeue {} of {} dead-lettered item(s), pruning their records \
                 from {}",
                selected.len(),
                dead.len(),
                dead_path.display()
            );
            for id in selected.iter().take(20) {
                println!("  {id}");
            }
            if selected.len() > 20 {
                println!("  … and {} more", selected.len() - 20);
            }
        }
        return Ok(());
    }

    // Store first. See the ordering note above.
    let want: std::collections::HashSet<String> = selected.iter().cloned().collect();
    let prune = forge_store::prune_dead_letters(&dead_path, &want)
        .await
        .context("pruning the dead-letter file")?;
    let requeued = queue
        .requeue_dead(&selected)
        .await
        .context("re-queueing dead-lettered items")?;

    if a.json {
        println!(
            "{}",
            serde_json::json!({
                "dead_total": dead.len(),
                "selected": selected.len(),
                "requeued": requeued.len(),
                "pruned": prune.removed,
                "dead_records_kept": prune.kept,
                "dead_records_unreadable": prune.lines_unreadable,
                "not_in_dead_file": prune.not_found,
                "dry_run": false,
            })
        );
    } else {
        println!(
            "requeued {} item(s); pruned {} dead-letter record(s) from {}",
            requeued.len(),
            prune.removed,
            dead_path.display()
        );
        if !prune.not_found.is_empty() {
            // Stated, not swallowed: the queue and the store can legitimately disagree
            // (a previous prune ran, or the item died before the store saw it), but an
            // operator deciding whether the retry is safe needs the number.
            println!(
                "  note: {} selected id(s) had no record in the dead-letter file \
                 (already pruned, or never written there)",
                prune.not_found.len()
            );
        }
        if prune.lines_unreadable > 0 {
            println!(
                "  note: {} unreadable line(s) in the dead-letter file were left \
                 untouched",
                prune.lines_unreadable
            );
        }
        println!(
            "  re-run with `forge resume --checkpoint {} …` to run them",
            a.checkpoint.display()
        );
    }
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

async fn cmd_subset(a: SubsetArgs) -> anyhow::Result<()> {
    let stats = forge_shard::subset_jsonl(&a.input, &a.ids, &a.out, a.invert)
        .await
        .context("subsetting input JSONL")?;
    println!(
        "subset {} -> {} (ids={} read={} written={} unmatched={} unparsable={})",
        a.input.display(),
        a.out.display(),
        stats.ids_loaded,
        stats.lines_read,
        stats.written,
        stats.ids_unmatched,
        stats.unparsable,
    );
    // An id that never appeared in the input almost always means the list and the
    // input are from different jobs — silently emitting a short file would send the
    // operator off to run a batch that is missing exactly the work they wanted.
    if stats.ids_unmatched > 0 {
        tracing::warn!(
            unmatched = stats.ids_unmatched,
            "some ids were not found in the input — is this the same job's input file?"
        );
    }
    if stats.unparsable > 0 {
        tracing::warn!(
            unparsable = stats.unparsable,
            "some input lines had no usable custom_id and were skipped"
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

async fn cmd_verify(a: VerifyArgs) -> anyhow::Result<Exit> {
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
    // `Incomplete` (2) rather than a bail (1): incompleteness is a VALID answer that
    // the report above already stated plainly, and the distinction tells a pipeline
    // that resuming is the correct response. Still non-zero, so `set -e` behaves.
    if !report.is_complete() {
        return Ok(Exit::Incomplete);
    }
    Ok(Exit::Complete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_concurrency_prefers_recorded_then_flag_then_default() {
        let q = SqliteQueue::open_in_memory().unwrap();

        // Older checkpoint (no recorded value): flag wins, else the loud default.
        assert_eq!(resolve_resume_concurrency(&q, Some(4)).unwrap(), 4);
        assert_eq!(
            resolve_resume_concurrency(&q, None).unwrap(),
            DEFAULT_CONCURRENCY
        );

        // A run recorded its fleet config: a bare resume reuses it...
        q.record_job("in.jsonl", "out.jsonl", 8).unwrap();
        assert_eq!(resolve_resume_concurrency(&q, None).unwrap(), 8);
        // ...and an explicit flag still wins (fleet may have changed), warned.
        assert_eq!(resolve_resume_concurrency(&q, Some(16)).unwrap(), 16);
    }
}
