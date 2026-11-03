//! forge-batch — a real **OpenAI Batch REST API** front door over the forge engine.
//!
//! Point unmodified OpenAI SDK code (`client.batches.create(...)`, `client.files...`)
//! at this server and a giant JSONL fans across the worker fleet forge was configured
//! with, surviving crashes exactly like `forge run`. It is a thin HTTP skin — the same
//! deliberately-minimal `tiny_http` server the OSS coordinator and the ee intake use —
//! that *composes* the existing crates:
//!
//! - **[`forge_shard::ingest_jsonl_with_rejects`]** hydrates a per-batch
//!   [`forge_queue::SqliteQueue`] from the uploaded input file.
//! - **[`forge_core::BatchRun`]** fans that queue across the fleet into a per-batch
//!   [`forge_store::JsonlStore`].
//! - the **queue counts are the progress state** — this crate reads them, it does not
//!   track a second copy.
//!
//! It adds **no** task graph, DAG, or fan-in (forge's invariant): a batch is one
//! homogeneous fan-out of independent items.
//!
//! ## Routes
//!
//! | method | path | purpose | auth |
//! |---|---|---|---|
//! | `GET`  | `/v1/health` | liveness + batch count | none |
//! | `POST` | `/v1/files` | upload a batch input JSONL → File object | bearer |
//! | `GET`  | `/v1/files/{id}` | retrieve a File object | bearer |
//! | `GET`  | `/v1/files/{id}/content` | stream file bytes (output files reshaped to OpenAI batch-output lines) | bearer |
//! | `POST` | `/v1/batches` | create + start a batch | bearer |
//! | `GET`  | `/v1/batches` | list all batches (`{object:"list",data:[...]}`) | bearer |
//! | `GET`  | `/v1/batches/{id}` | retrieve a batch (status mapped from queue counts) | bearer |
//! | `POST` | `/v1/batches/{id}/cancel` | cancel a batch | bearer |
//!
//! Bearer auth is enforced on everything except `/v1/health` **iff** an `api_key` was
//! configured; with no key the server is open (fine for a trusted network / a sidecar).
//!
//! ## What survives a `serve-batch` restart
//!
//! Each batch persists a small `batch.json` under `<data-dir>/batches/<id>/`, and the
//! file registry persists `<data-dir>/files.json`. On startup both are re-loaded, so
//! existing batches re-list and finished results are still fetchable. What does **not**
//! survive: the in-flight run *task* — a batch that was `in_progress` when the process
//! died is not auto-resumed (its `ckpt.db` is intact, so `forge resume` could continue
//! it). A GET on such a batch reads its live queue counts; if it turns out already
//! drained, it is finalized to `completed` on read.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use forge_core::{now_ms, BatchRun, ForgeError, Queue, RunConfig, Worker};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;
use serde::{Deserialize, Serialize};

pub mod openai;
pub use openai::{
    BatchObject, CreateBatchRequest, FileObject, ListEnvelope, OutputLine, RequestCounts,
};

// Lifecycle status strings (the OpenAI Batch `status` values we use).
const S_IN_PROGRESS: &str = "in_progress";
const S_COMPLETED: &str = "completed";
const S_CANCELLING: &str = "cancelling";
const S_CANCELLED: &str = "cancelled";
const S_FAILED: &str = "failed";

/// A short random lowercase-hex string (e.g. for `file-<hex>` / `batch-<hex>` ids).
pub(crate) fn rand_hex(bytes: usize) -> String {
    use rand::RngCore;
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    let mut s = String::with_capacity(bytes * 2);
    for b in buf {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Cap on a `POST /v1/files` upload body — generous for a large batch input JSONL.
const MAX_UPLOAD_BYTES: u64 = 512 * 1024 * 1024;
/// Cap on a `POST /v1/batches` request body — this is a small control JSON
/// (`input_file_id`/`endpoint`/`completion_window`), never a batch payload itself.
const MAX_BATCH_REQUEST_BYTES: u64 = 1024 * 1024;

/// Read at most `max + 1` bytes from `reader`. This server can be bound to `0.0.0.0`
/// with no `--api-key` (open by design, for a trusted network), so an unbounded
/// `read_to_end`/`read_to_string` on the request body is an OOM vector any caller can
/// trigger, authenticated or not. `Ok(None)` means the body exceeded `max`.
fn read_body_capped(reader: &mut dyn Read, max: u64) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    reader.take(max + 1).read_to_end(&mut buf)?;
    if buf.len() as u64 > max {
        Ok(None)
    } else {
        Ok(Some(buf))
    }
}

// ───────────────────────────── worker fleet ─────────────────────────────

/// A factory for the worker fleet a batch runs against. `serve_batch` is generic over
/// this so production wires a real [`HttpFleet`] (BYO OpenAI-compatible endpoints)
/// while tests wire an in-process mock — the same seam `BatchRun` already exposes by
/// being generic over [`Worker`].
///
/// A **fresh** `Vec` of workers is built per batch (each carries its own keep-alive
/// client + AIMD limiter), so concurrent batches don't share in-flight limiters.
pub trait WorkerFleet: Send + Sync + 'static {
    type W: Worker + Send + Sync + 'static;

    /// Build the fleet for one batch. Returns `Err` if a worker can't be constructed
    /// (e.g. a malformed base URL).
    fn build(&self) -> Result<Vec<Self::W>, ForgeError>;
}

/// The production fleet: real [`HttpWorker`]s over BYO OpenAI-compatible endpoints,
/// carrying the same retry + response-validation policy the `forge run` CLI applies.
pub struct HttpFleet {
    specs: Vec<forge_core::WorkerSpec>,
    retry: forge_core::RetryPolicy,
    validation: forge_core::ResponseCheck,
}

impl HttpFleet {
    /// Build from the resolved worker specs + policies (the CLI derives these from its
    /// shared `WorkerArgs`, exactly as `forge run` does).
    pub fn new(
        specs: Vec<forge_core::WorkerSpec>,
        retry: forge_core::RetryPolicy,
        validation: forge_core::ResponseCheck,
    ) -> Self {
        Self {
            specs,
            retry,
            validation,
        }
    }
}

impl WorkerFleet for HttpFleet {
    type W = HttpWorker;

    fn build(&self) -> Result<Vec<HttpWorker>, ForgeError> {
        self.specs
            .iter()
            .map(|spec| {
                HttpWorker::new(spec.clone())
                    .map(|w| w.with_retry(self.retry).with_validation(self.validation))
            })
            .collect()
    }
}

// ───────────────────────────── config + server ─────────────────────────────

/// Everything `serve_batch` needs to stand up the front door.
pub struct BatchConfig<F: WorkerFleet> {
    /// Bind address, e.g. `0.0.0.0:8080` or `127.0.0.1:0` for an ephemeral test port.
    pub bind: String,
    /// Root under which `files/`, `batches/`, and `files.json` live.
    pub data_dir: PathBuf,
    /// Optional bearer key. `Some` → require `Authorization: Bearer <key>` on every
    /// route except `/v1/health`; `None` → open.
    pub api_key: Option<String>,
    /// The worker fleet every batch fans out across.
    pub fleet: F,
    /// The run tunables (lease TTLs, ready-grace, retry, load-aware) — the CLI derives
    /// these from its shared `WorkerArgs`.
    pub run_config: RunConfig,
    /// Number of accept threads sharing the listener.
    pub threads: usize,
}

/// A running batch server. Call [`shutdown`](BatchServer::shutdown) to stop + join both
/// the accept threads (the OSS `CoordinatorServer` / ee `IntakeServer` pattern) and every
/// still-running per-batch supervisor thread.
pub struct BatchServer {
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    threads: Vec<JoinHandle<()>>,
    registry: Arc<Mutex<Registry>>,
}

impl BatchServer {
    /// The bound address (useful when binding to port 0 in tests).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A base URL a client (or the OpenAI SDK's `base_url`) can point at.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Signal the accept threads to stop and join them, then cancel + join every
    /// still-running per-batch supervisor thread. Without this, a `shutdown` while a
    /// batch was `in_progress` left its supervisor thread running detached, holding a
    /// `tokio::runtime::Handle` that could outlive its runtime — a real leak for an
    /// embedder that starts/stops `serve_batch` repeatedly.
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.threads {
            let _ = t.join();
        }
        let handles: Vec<JoinHandle<()>> = {
            let mut reg = self.registry.lock().unwrap();
            for entry in reg.batches.values_mut() {
                entry.cancel.store(true, Ordering::SeqCst);
            }
            reg.batches
                .values_mut()
                .filter_map(|e| e.task.take())
                .collect()
        };
        for t in handles {
            let _ = t.join();
        }
    }
}

/// Serve the OpenAI Batch REST front door. Must be called from within a Tokio runtime
/// — the blocking accept threads bridge into the async engine (queue counts, ingest,
/// the fan-out task) via the current runtime handle, exactly like `serve_coordinator`
/// / `serve_intake`.
pub fn serve_batch<F: WorkerFleet>(cfg: BatchConfig<F>) -> Result<BatchServer, ForgeError> {
    let state = Arc::new(ServerState::bootstrap(cfg)?);
    resume_in_flight(&state);

    let server = Arc::new(
        tiny_http::Server::http(&state.bind).map_err(|e| ForgeError::Config(e.to_string()))?,
    );
    let addr = server
        .server_addr()
        .to_ip()
        .ok_or_else(|| ForgeError::Config("server bound to a non-IP address".into()))?;
    let stop = Arc::new(AtomicBool::new(false));
    let handle = tokio::runtime::Handle::current();

    let mut handles = Vec::new();
    for _ in 0..state.threads.max(1) {
        let server = Arc::clone(&server);
        let stop = Arc::clone(&stop);
        let state = Arc::clone(&state);
        let rt = handle.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match server.recv_timeout(Duration::from_millis(200)) {
                    Ok(Some(req)) => handle_request(&rt, req, &state),
                    Ok(None) => continue, // idle timeout — re-check `stop`
                    Err(_) => break,
                }
            }
        }));
    }

    Ok(BatchServer {
        stop,
        addr,
        threads: handles,
        registry: Arc::clone(&state.registry),
    })
}

// ───────────────────────────── registry / persistence ─────────────────────────────

/// The persisted-to-disk truth for one batch (`<data-dir>/batches/<id>/batch.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchRecord {
    id: String,
    endpoint: String,
    input_file_id: String,
    completion_window: String,
    status: String,
    created_at: i64,
    in_progress_at: Option<i64>,
    completed_at: Option<i64>,
    cancelled_at: Option<i64>,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
}

/// The persisted truth for one file (`<data-dir>/files.json` maps `id` → this).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileRecord {
    id: String,
    filename: String,
    bytes: u64,
    created_at: i64,
    /// `"batch"` (input) / `"batch_output"` / `"batch_error"`.
    purpose: String,
    /// Absolute path to the bytes on disk.
    path: PathBuf,
    kind: FileKind,
}

impl FileRecord {
    fn to_object(&self) -> FileObject {
        FileObject {
            id: self.id.clone(),
            object: "file".to_string(),
            bytes: self.bytes,
            created_at: self.created_at,
            filename: self.filename.clone(),
            purpose: self.purpose.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum FileKind {
    /// An uploaded input file — served as-is.
    Input,
    /// A finished batch's success/dead results — reshaped to OpenAI batch-output lines.
    Output,
    Error,
}

/// In-memory batch entry: the persisted record plus the non-persisted runtime handles.
struct BatchEntry {
    record: BatchRecord,
    dir: PathBuf,
    /// Cooperative cancel flag: the run supervisor watches it and drops the in-flight
    /// [`BatchRun`] future when it flips, which stops dispatching new items.
    cancel: Arc<AtomicBool>,
    /// The OS thread running this batch's fan-out (on its own current-thread runtime).
    /// `Some` iff the run was started **in this process**; `None` for a batch re-listed
    /// after a restart (its thread is gone).
    task: Option<JoinHandle<()>>,
    /// A lazily-opened read handle to `ckpt.db` for reading live counts (a second WAL
    /// connection; the run supervisor's thread owns the writer).
    read_queue: Option<Arc<SqliteQueue>>,
}

impl BatchEntry {
    fn ckpt_path(&self) -> PathBuf {
        self.dir.join("ckpt.db")
    }
    fn out_path(&self) -> PathBuf {
        self.dir.join("out.jsonl")
    }
    fn dead_path(&self) -> PathBuf {
        self.dir.join("out.jsonl.dead.jsonl")
    }
    /// The reshaped, OpenAI-batch-output-shaped, immutable sibling of [`Self::out_path`]
    /// — written once at finalize time by [`reshape_results_once`].
    fn output_reshaped_path(&self) -> PathBuf {
        self.dir.join("output.reshaped.jsonl")
    }
    /// The reshaped, immutable sibling of [`Self::dead_path`].
    fn error_reshaped_path(&self) -> PathBuf {
        self.dir.join("error.reshaped.jsonl")
    }
}

#[derive(Default)]
struct Registry {
    batches: HashMap<String, BatchEntry>,
    files: HashMap<String, FileRecord>,
}

/// Process-lifetime counters for `/metrics`.
///
/// These are deliberately **counters**, not gauges: they only ever increase, so
/// `rate()` and `increase()` work on them. The registry already tells you the current
/// level (how many batches are in progress right now) — what it cannot tell you is
/// throughput, which is the question an alert actually asks ("are we still completing
/// batches?"). A gauge-only exposition, which is all forge had, cannot answer that.
///
/// They reset on restart, as counters should; Prometheus handles the reset.
#[derive(Debug, Default)]
struct Metrics {
    batches_created: AtomicU64,
    batches_completed: AtomicU64,
    batches_cancelled: AtomicU64,
    batches_failed: AtomicU64,
    files_uploaded: AtomicU64,
    http_requests: AtomicU64,
    http_errors: AtomicU64,
}

/// Shared server state behind the accept threads.
struct ServerState<F: WorkerFleet> {
    metrics: Metrics,
    bind: String,
    threads: usize,
    data_dir: PathBuf,
    api_key: Option<String>,
    fleet: Arc<F>,
    run_config: RunConfig,
    /// `Arc`-wrapped (rather than a bare `Mutex`) so [`BatchServer`] — which, unlike
    /// `ServerState`, isn't generic over `F` — can hold its own clone and join every
    /// per-batch supervisor thread on shutdown, not just the HTTP accept threads.
    registry: Arc<Mutex<Registry>>,
    /// When this process started serving, for `/v1/health`. An uptime that keeps
    /// resetting is the signature of a crash loop, and it is the one thing a probe can
    /// see that a bare `ok` cannot express.
    started: Instant,
}

impl<F: WorkerFleet> ServerState<F> {
    fn bootstrap(cfg: BatchConfig<F>) -> Result<Self, ForgeError> {
        let data_dir = cfg.data_dir;
        std::fs::create_dir_all(data_dir.join("files"))?;
        std::fs::create_dir_all(data_dir.join("batches"))?;

        let mut registry = Registry::default();
        // Re-load the file registry.
        //
        // **Errors are propagated, not swallowed.** This used to be a nest of
        // `if let Ok(..)`, so an unreadable or truncated `files.json` started the server
        // with an EMPTY registry — and the next upload persisted that empty map, turning
        // a recoverable file into permanent loss of every file record, silently. A
        // registry that cannot be read is a state a human has to see.
        let files_json = data_dir.join("files.json");
        match std::fs::read(&files_json) {
            Ok(bytes) => {
                registry.files = serde_json::from_slice(&bytes).map_err(|e| {
                    ForgeError::Store(format!(
                        "file registry {} is unreadable: {e}. It maps every uploaded \
                         file id to its bytes on disk, so starting without it would \
                         report `no such input_file_id` for work that is still there — \
                         and the next upload would overwrite it, making the loss \
                         permanent. The uploaded bytes are intact under {}/files/; \
                         restore the registry from a backup, or move it aside \
                         deliberately to start a fresh one.",
                        files_json.display(),
                        data_dir.display(),
                    ))
                })?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // first boot
            Err(e) => return Err(e.into()),
        }

        // Re-list existing batches from their per-dir batch.json.
        //
        // A batch directory whose `batch.json` cannot be parsed is skipped with a loud
        // warning rather than failing the boot: unlike the file registry, one bad batch
        // record costs only that batch's listing, and refusing to start would take the
        // whole server down over a single job. Its `ckpt.db` and results are untouched
        // on disk either way.
        if let Ok(entries) = std::fs::read_dir(data_dir.join("batches")) {
            for entry in entries.flatten() {
                let dir = entry.path();
                let meta = dir.join("batch.json");
                if !meta.exists() {
                    continue;
                }
                match std::fs::read(&meta)
                    .map_err(|e| e.to_string())
                    .and_then(|b| {
                        serde_json::from_slice::<BatchRecord>(&b).map_err(|e| e.to_string())
                    }) {
                    Ok(record) => {
                        registry.batches.insert(
                            record.id.clone(),
                            BatchEntry {
                                record,
                                dir,
                                cancel: Arc::new(AtomicBool::new(false)),
                                task: None,
                                read_queue: None,
                            },
                        );
                    }
                    Err(e) => tracing::error!(
                        path = %meta.display(),
                        error = %e,
                        "unreadable batch record; skipping it in the listing (its \
                         checkpoint and results are untouched on disk)"
                    ),
                }
            }
        }

        Ok(Self {
            metrics: Metrics::default(),
            bind: cfg.bind,
            threads: cfg.threads,
            data_dir,
            api_key: cfg.api_key,
            fleet: Arc::new(cfg.fleet),
            run_config: cfg.run_config,
            registry: Arc::new(Mutex::new(registry)),
            started: Instant::now(),
        })
    }

    fn files_json(&self) -> PathBuf {
        self.data_dir.join("files.json")
    }

    /// Persist the whole file registry (called under the registry lock after a change).
    fn persist_files(&self, files: &HashMap<String, FileRecord>) {
        if let Ok(bytes) = serde_json::to_vec_pretty(files) {
            if let Err(e) = write_atomic(&self.files_json(), &bytes) {
                // Worth shouting about: this file is the ONLY record of which uploaded
                // bytes belong to which file id. Losing it strands every batch that
                // references one.
                tracing::error!(error = %e, "could not persist the file registry");
            }
        }
    }
}

/// Write `bytes` to `path` atomically: temp file beside the target, fsync, rename.
///
/// A plain `fs::write` truncates first and then writes, so a crash — or an OOM kill, or
/// a node losing power — in that window leaves a **truncated** registry. Since the
/// loader used to swallow every parse error and start blank, the next write then
/// persisted the blank map and the loss became permanent. Atomicity is what makes the
/// loader's refusal (see [`ServerState::bootstrap`]) a recoverable state rather than a
/// dead end: a rename either happened or it did not, so what is on disk is always one
/// whole registry or the other.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        // fsync before the rename: a rename that lands ahead of its data leaves a
        // zero-length file after a power cut, which is exactly the corruption this
        // function exists to prevent.
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Persist one batch record to its `batch.json`.
///
/// Atomic for the same reason as the file registry: this is what a restart re-lists a
/// batch from, and a half-written record is a batch the server can no longer describe.
fn persist_batch(dir: &Path, record: &BatchRecord) {
    if let Ok(bytes) = serde_json::to_vec_pretty(record) {
        if let Err(e) = write_atomic(&dir.join("batch.json"), &bytes) {
            tracing::error!(batch = %record.id, error = %e, "could not persist batch record");
        }
    }
}

fn record_to_object(record: &BatchRecord, counts: RequestCounts) -> BatchObject {
    BatchObject {
        id: record.id.clone(),
        object: "batch".to_string(),
        endpoint: record.endpoint.clone(),
        input_file_id: record.input_file_id.clone(),
        completion_window: record.completion_window.clone(),
        status: record.status.clone(),
        output_file_id: record.output_file_id.clone(),
        error_file_id: record.error_file_id.clone(),
        created_at: record.created_at,
        in_progress_at: record.in_progress_at,
        completed_at: record.completed_at,
        cancelled_at: record.cancelled_at,
        request_counts: counts,
    }
}

// ───────────────────────────── run-task lifecycle ─────────────────────────────

/// Finalize a batch to a terminal state, minting the output/error File resources that
/// back its results. Idempotent and cancel-aware: it only transitions a batch that is
/// still `in_progress` (so a cancel that raced the task's completion wins). Called both
/// by the run task on completion and by a GET that finds a re-listed batch already
/// drained.
fn finalize_completed<F: WorkerFleet>(state: &ServerState<F>, id: &str) {
    let mut reg = state.registry.lock().unwrap();
    let Some(entry) = reg.batches.get(id) else {
        return;
    };
    if entry.record.status != S_IN_PROGRESS {
        return; // already terminal (completed/cancelled/failed) — don't clobber
    }
    let out_path = entry.out_path();
    let dead_path = entry.dead_path();
    let output_reshaped_path = entry.output_reshaped_path();
    let error_reshaped_path = entry.error_reshaped_path();
    let dir = entry.dir.clone();

    // Reshape each raw results file into its immutable, OpenAI-shaped sibling ONCE, then
    // mint a File resource pointing at that reshaped file (not the raw one) — so repeated
    // GETs of the same output/error file always return byte-identical content.
    reshape_results_once(&out_path, &output_reshaped_path);
    reshape_results_once(&dead_path, &error_reshaped_path);
    let output_file_id =
        mint_result_file(&mut reg.files, &output_reshaped_path, id, FileKind::Output);
    let error_file_id = mint_result_file(&mut reg.files, &error_reshaped_path, id, FileKind::Error);
    state.persist_files(&reg.files);

    let entry = reg.batches.get_mut(id).unwrap();
    entry.record.status = S_COMPLETED.to_string();
    state
        .metrics
        .batches_completed
        .fetch_add(1, Ordering::Relaxed);
    entry.record.completed_at = Some(now_ms());
    entry.record.output_file_id = output_file_id;
    entry.record.error_file_id = error_file_id;
    persist_batch(&dir, &entry.record);
}

/// Mark a batch `failed` (the run task errored, e.g. no worker ever became ready).
fn finalize_failed<F: WorkerFleet>(state: &ServerState<F>, id: &str) {
    let mut reg = state.registry.lock().unwrap();
    let Some(entry) = reg.batches.get_mut(id) else {
        return;
    };
    if entry.record.status != S_IN_PROGRESS {
        return;
    }
    entry.record.status = S_FAILED.to_string();
    state.metrics.batches_failed.fetch_add(1, Ordering::Relaxed);
    entry.record.completed_at = Some(now_ms());
    persist_batch(&entry.dir, &entry.record);
}

/// Reshape a raw forge results file (`out.jsonl` / its `.dead.jsonl` sibling) into the
/// OpenAI batch-output line shape **once**, writing the result to `dst`. Doing this at
/// finalize time — rather than on every `GET /v1/files/{id}/content` — is what makes the
/// output file actually immutable: [`openai::reshape_result_line`] mints a fresh random
/// `id` (and, absent an upstream one, a fresh `request_id`) on every call, so reshaping
/// on each read made two successive GETs of the "same" file disagree with each other.
/// Returns the byte length written, or `None` if `src` doesn't exist / reshapes to nothing.
fn reshape_results_once(src: &Path, dst: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(src).ok()?;
    let mut out = String::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(reshaped) = openai::reshape_result_line(line) {
            if let Ok(s) = serde_json::to_string(&reshaped) {
                out.push_str(&s);
                out.push('\n');
            }
        }
    }
    if out.is_empty() {
        return None;
    }
    std::fs::write(dst, out.as_bytes()).ok()?;
    Some(out.len() as u64)
}

/// Register a File resource backing an already-reshaped, immutable results file.
/// Returns the new file id (or `None` when there's nothing to back — e.g. no dead items).
fn mint_result_file(
    files: &mut HashMap<String, FileRecord>,
    path: &Path,
    batch_id: &str,
    kind: FileKind,
) -> Option<String> {
    let meta = std::fs::metadata(path).ok()?;
    if meta.len() == 0 {
        return None;
    }
    let id = format!("file-{}", rand_hex(12));
    let (suffix, purpose) = match kind {
        FileKind::Output => ("output", "batch_output"),
        FileKind::Error => ("error", "batch_error"),
        FileKind::Input => ("input", "batch"),
    };
    files.insert(
        id.clone(),
        FileRecord {
            id: id.clone(),
            filename: format!("{batch_id}_{suffix}.jsonl"),
            bytes: meta.len(),
            created_at: now_ms(),
            purpose: purpose.to_string(),
            path: path.to_path_buf(),
            kind,
        },
    );
    Some(id)
}

/// Restart the supervisor for every batch left `in_progress` by a previous process.
///
/// Without this a restart produces a **zombie**: `bootstrap` re-lists the batch with
/// `task: None` and its persisted status still `in_progress`, so `GET /v1/batches/{id}`
/// answers `in_progress` forever while nothing is running it. The customer's SDK polls
/// a status that will never change, and the job simply stops — the exact 3am failure,
/// with the API actively reporting that everything is fine. (A batch that *drained*
/// before the crash was already finalized on read; the hole was the half-done one,
/// which is the common case, because a process is most likely to die while it is busy.)
///
/// Resuming is safe because every step is idempotent: `ingest_jsonl` skips ids already
/// hydrated, the queue is the durable source of truth, and the store dedups by
/// `custom_id`. It is the same path `forge resume` takes — this just takes it
/// automatically, because nobody is awake to type it.
///
/// A batch whose input file is no longer in the registry cannot be resumed; it is
/// failed with a reason rather than left claiming progress it will never make.
fn resume_in_flight<F: WorkerFleet>(state: &Arc<ServerState<F>>) {
    // Collect first, then spawn: holding the registry lock across a thread spawn that
    // itself wants the lock is a deadlock waiting to be discovered at 3am.
    let pending: Vec<(String, PathBuf, Option<PathBuf>, Arc<AtomicBool>)> = {
        let reg = state.registry.lock().unwrap();
        reg.batches
            .values()
            .filter(|e| e.record.status == S_IN_PROGRESS)
            .map(|e| {
                let input = reg
                    .files
                    .get(&e.record.input_file_id)
                    .map(|f| f.path.clone());
                (
                    e.record.id.clone(),
                    e.dir.clone(),
                    input,
                    Arc::clone(&e.cancel),
                )
            })
            .collect()
    };
    if pending.is_empty() {
        return;
    }
    tracing::info!(
        batches = pending.len(),
        "resuming batches left in flight by a previous process"
    );

    for (id, dir, input, cancel) in pending {
        let Some(input_path) = input else {
            tracing::error!(
                batch = %id,
                "cannot resume: its input file is no longer in the registry. Failing it \
                 rather than leaving it reporting in_progress forever."
            );
            finalize_failed(state, &id);
            continue;
        };
        let handle = {
            let state = Arc::clone(state);
            let id2 = id.clone();
            std::thread::Builder::new()
                .name(format!("forge-batch-{id}"))
                .spawn(move || run_supervisor(state, id2, dir, input_path, cancel))
        };
        match handle {
            Ok(t) => {
                let mut reg = state.registry.lock().unwrap();
                if let Some(e) = reg.batches.get_mut(&id) {
                    e.task = Some(t);
                }
            }
            Err(e) => {
                // Same rule as `create_batch`: no supervisor means nothing will ever
                // advance this batch, and `in_progress` would be a lie.
                tracing::error!(batch = %id, error = %e, "could not spawn resume supervisor");
                finalize_failed(state, &id);
            }
        }
    }
}

/// The per-batch supervisor, run on its **own** OS thread + current-thread runtime.
///
/// Why a dedicated runtime rather than `tokio::spawn`: the [`Worker`] trait's
/// `async fn submit`/`probe` are `async-fn-in-trait`, whose futures are not `Send`, so a
/// [`BatchRun`] cannot be spawned onto the shared multi-thread runtime. A per-batch
/// current-thread runtime drives the (non-`Send`) fan-out locally — the same
/// dedicated-runtime pattern the agent tests use for the coordinator server. It also
/// gives an honest **cooperative cancel**: the run future is raced against a watcher of
/// the [`BatchEntry::cancel`] flag; flipping the flag drops the run future at its next
/// await point (results already written stay; in-flight leases expire in `ckpt.db`).
fn run_supervisor<F: WorkerFleet>(
    state: Arc<ServerState<F>>,
    id: String,
    dir: PathBuf,
    input_path: PathBuf,
    cancel: Arc<AtomicBool>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        // A current-thread runtime still ships tokio's 512-thread blocking pool, and
        // every queue op is a `spawn_blocking` — see `forge_core::runtime`.
        .max_blocking_threads(forge_core::MAX_BLOCKING_THREADS)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(batch = %id, error = %e, "could not build batch runtime");
            finalize_failed(&state, &id);
            return;
        }
    };

    let outcome = rt.block_on(async {
        tokio::select! {
            biased;
            _ = watch_cancel(&cancel) => None,               // cancelled → stop
            r = run_batch_inner(&state, &dir, &input_path) => Some(r),
        }
    });

    match outcome {
        Some(Ok(())) => finalize_completed(&state, &id),
        Some(Err(e)) => {
            tracing::warn!(batch = %id, error = %e, "batch run failed");
            finalize_failed(&state, &id);
        }
        // Cancelled: the cancel handler already set the terminal state; do nothing (and
        // `finalize_*` would no-op anyway, since the status is no longer `in_progress`).
        None => tracing::info!(batch = %id, "batch run cancelled"),
    }
}

/// Poll the cancel flag; resolve once it flips. The 100ms granularity bounds how long
/// after a `cancel` request the run future keeps dispatching.
async fn watch_cancel(cancel: &AtomicBool) {
    while !cancel.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Hydrate the queue from the input file, then drive [`BatchRun`] over the fleet into a
/// per-batch [`JsonlStore`]. Runs entirely on the supervisor's current-thread runtime.
async fn run_batch_inner<F: WorkerFleet>(
    state: &Arc<ServerState<F>>,
    dir: &Path,
    input_path: &Path,
) -> Result<(), ForgeError> {
    let queue = SqliteQueue::open(dir.join("ckpt.db"))?;
    let reject = dir.join("input.reject.jsonl");
    let stats = forge_shard::ingest_jsonl_with_rejects(&queue, input_path, Some(&reject)).await?;
    tracing::info!(
        hydrated = stats.hydrated,
        rejected = stats.rejected,
        "batch hydrated"
    );

    // Nothing hydrated (empty or fully-malformed input): it's already drained, so the
    // batch completes immediately without spinning up the fleet.
    if queue.counts().await?.total() == 0 {
        return Ok(());
    }

    let workers = state.fleet.build()?;
    let store = JsonlStore::new(dir.join("out.jsonl"));
    BatchRun::new(queue, workers, store)
        .with_config(state.run_config)
        .run()
        .await?;
    Ok(())
}

// ───────────────────────────── routing ─────────────────────────────

fn handle_request<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    req: tiny_http::Request,
    state: &Arc<ServerState<F>>,
) {
    let method = req.method().clone();
    let raw_url = req.url().to_string();
    let path = raw_url.split('?').next().unwrap_or(&raw_url).to_string();
    state.metrics.http_requests.fetch_add(1, Ordering::Relaxed);

    // Bind a correlation id for this request. Held for the whole handler, so every log
    // line it emits and every response it writes carry the same id — which is what lets
    // a customer quoting an id from a 500 be joined to the server-side detail.
    let _rid = forge_core::scope_request_id(forge_core::accept_request_id(
        header_value(req.headers(), "x-request-id").as_deref(),
    ));
    tracing::debug!(method = %method.as_str(), path, "request");

    // Liveness needs no auth — a load balancer holds no credential.
    //
    // It reports what a probe and a support ticket need (is this process serving, which
    // build is it, how long has it been up) and **not** the batch count it used to
    // return. That count is business telemetry: how much work a customer is running is
    // exactly the class of figure `/metrics` is gated on, and leaking it to any
    // unauthenticated caller made the gate on `/metrics` decorative. It is still
    // available there, behind the key.
    if method == tiny_http::Method::Get && path == "/v1/health" {
        let body = serde_json::json!({
            "status": "ok",
            "service": "forge-serve-batch",
            "build": forge_core::build::json(),
            "uptime_secs": state.started.elapsed().as_secs(),
        })
        .to_string();
        return respond_json(req, 200, body);
    }

    // Bearer auth on everything else (when a key is configured).
    //
    // `/metrics` is deliberately INSIDE the authenticated set, unlike `/v1/health`.
    // Health has to be reachable by a load balancer that holds no credential and it
    // leaks only a batch count; metrics expose per-status batch and item counts, which
    // is business telemetry. Prometheus supports bearer tokens, so the cost of keeping
    // it behind the key is a two-line scrape config.
    if let Some(expected) = &state.api_key {
        let auth = header_value(req.headers(), "authorization");
        if !bearer_matches(auth.as_deref(), expected) {
            state.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
            return respond_unauthorized(req);
        }
    }

    if method == tiny_http::Method::Get && path == "/metrics" {
        return serve_metrics(rt, req, state);
    }

    let m = &method;
    // Files.
    if *m == tiny_http::Method::Post && path == "/v1/files" {
        return upload_file(req, state);
    }
    if *m == tiny_http::Method::Get {
        if let Some(id) = path
            .strip_prefix("/v1/files/")
            .and_then(|r| r.strip_suffix("/content"))
        {
            return file_content(req, state, id);
        }
        if let Some(id) = path.strip_prefix("/v1/files/") {
            if !id.is_empty() {
                return file_object(req, state, id);
            }
        }
    }
    // Batches.
    match (m, path.as_str()) {
        (&tiny_http::Method::Post, "/v1/batches") => return create_batch(req, state),
        (&tiny_http::Method::Get, "/v1/batches") => return list_batches(rt, req, state),
        _ => {}
    }
    if let Some(rest) = path.strip_prefix("/v1/batches/") {
        if let Some(id) = rest.strip_suffix("/cancel") {
            if *m == tiny_http::Method::Post {
                return cancel_batch(rt, req, state, id);
            }
        } else if *m == tiny_http::Method::Get && !rest.is_empty() {
            return get_batch(rt, req, state, rest);
        }
    }

    state.metrics.http_errors.fetch_add(1, Ordering::Relaxed);
    respond_text(req, 404, "no such route");
}

// ───────────────────────────── file handlers ─────────────────────────────

fn upload_file<F: WorkerFleet>(mut req: tiny_http::Request, state: &Arc<ServerState<F>>) {
    // Optional `?filename=` (default `batch.jsonl`). The MVP accepts the raw JSONL as
    // the request body (see the crate-level note on the multipart compromise).
    let raw_url = req.url().to_string();
    let filename = query_param(&raw_url, "filename").unwrap_or_else(|| "batch.jsonl".to_string());

    let body = match read_body_capped(req.as_reader(), MAX_UPLOAD_BYTES) {
        Ok(Some(b)) => b,
        Ok(None) => return respond_text(req, 413, "upload body exceeds the size limit"),
        Err(_) => return respond_text(req, 400, "could not read upload body"),
    };

    let id = format!("file-{}", rand_hex(12));
    let path = state.data_dir.join("files").join(format!("{id}.jsonl"));
    if let Err(e) = std::fs::write(&path, &body) {
        return respond_internal(req, "persisting an upload", e);
    }

    let record = FileRecord {
        id: id.clone(),
        filename,
        bytes: body.len() as u64,
        created_at: now_ms(),
        purpose: "batch".to_string(),
        path,
        kind: FileKind::Input,
    };
    let object = record.to_object();
    {
        let mut reg = state.registry.lock().unwrap();
        reg.files.insert(id, record);
        state.metrics.files_uploaded.fetch_add(1, Ordering::Relaxed);
        state.persist_files(&reg.files);
    }
    respond_json(req, 200, serde_json::to_string(&object).unwrap_or_default());
}

fn file_object<F: WorkerFleet>(req: tiny_http::Request, state: &Arc<ServerState<F>>, id: &str) {
    let reg = state.registry.lock().unwrap();
    match reg.files.get(id) {
        Some(f) => respond_json(
            req,
            200,
            serde_json::to_string(&f.to_object()).unwrap_or_default(),
        ),
        None => respond_text(req, 404, "no such file"),
    }
}

fn file_content<F: WorkerFleet>(req: tiny_http::Request, state: &Arc<ServerState<F>>, id: &str) {
    let record = {
        let reg = state.registry.lock().unwrap();
        match reg.files.get(id) {
            Some(f) => f.clone(),
            None => return respond_text(req, 404, "no such file"),
        }
    };

    // Output/Error files are reshaped exactly once, at finalize time
    // (`reshape_results_once`), and `record.path` already points at that reshaped,
    // immutable sibling — so every kind streams its bytes as-is, byte-identical on
    // every GET (bounded RAM — from_file streams).
    match std::fs::File::open(&record.path) {
        Ok(f) => {
            let mut resp = tiny_http::Response::from_file(f).with_status_code(200);
            if record.kind != FileKind::Input {
                resp = resp.with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/jsonl"[..])
                        .expect("static header"),
                );
            }
            let _ = req.respond(resp);
        }
        Err(_) => respond_text(req, 404, "file bytes missing"),
    }
}

// ───────────────────────────── batch handlers ─────────────────────────────

fn create_batch<F: WorkerFleet>(mut req: tiny_http::Request, state: &Arc<ServerState<F>>) {
    let body = match read_body_capped(req.as_reader(), MAX_BATCH_REQUEST_BYTES) {
        Ok(Some(b)) => b,
        Ok(None) => return respond_text(req, 413, "request body exceeds the size limit"),
        Err(_) => return respond_text(req, 400, "unreadable request body"),
    };
    let request: CreateBatchRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return respond_text(req, 400, &format!("bad request body: {e}")),
    };

    // Resolve the referenced input file.
    let input_path = {
        let reg = state.registry.lock().unwrap();
        match reg.files.get(&request.input_file_id) {
            Some(f) => f.path.clone(),
            None => {
                return respond_text(
                    req,
                    404,
                    &format!("no such input_file_id: {}", request.input_file_id),
                )
            }
        }
    };

    let id = format!("batch-{}", rand_hex(12));
    let dir = state.data_dir.join("batches").join(&id);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return respond_internal(req, "creating a batch directory", e);
    }

    let now = now_ms();
    let record = BatchRecord {
        id: id.clone(),
        endpoint: request.endpoint.clone(),
        input_file_id: request.input_file_id.clone(),
        completion_window: request
            .completion_window
            .unwrap_or_else(|| "24h".to_string()),
        // We go straight to in_progress (a brief "validating" is elided; the queue is
        // hydrated by the run task we're about to spawn).
        status: S_IN_PROGRESS.to_string(),
        created_at: now,
        in_progress_at: Some(now),
        completed_at: None,
        cancelled_at: None,
        output_file_id: None,
        error_file_id: None,
    };
    persist_batch(&dir, &record);

    // Spawn the fan-out on its own OS thread + current-thread runtime (see
    // `run_supervisor` for why not `tokio::spawn`). The cancel flag is the stop handle.
    let cancel = Arc::new(AtomicBool::new(false));
    let task = {
        let state = Arc::clone(state);
        let id = id.clone();
        let dir = dir.clone();
        let cancel = Arc::clone(&cancel);
        match std::thread::Builder::new()
            .name(format!("forge-batch-{id}"))
            .spawn(move || run_supervisor(state, id, dir, input_path, cancel))
        {
            Ok(t) => Some(t),
            Err(e) => {
                // No supervisor means nothing will ever hydrate the queue or run the
                // fan-out — reporting `in_progress` here would strand the batch forever
                // with no way for the caller to detect it. Fail the request instead of
                // persisting an unrunnable batch.
                return respond_internal(req, "spawning a batch supervisor", e);
            }
        }
    };

    let object = record_to_object(&record, RequestCounts::default());
    {
        let mut reg = state.registry.lock().unwrap();
        reg.batches.insert(
            id,
            BatchEntry {
                record,
                dir,
                cancel,
                task,
                read_queue: None,
            },
        );
    }
    state
        .metrics
        .batches_created
        .fetch_add(1, Ordering::Relaxed);
    respond_json(req, 200, serde_json::to_string(&object).unwrap_or_default());
}

/// `GET /metrics` — Prometheus text exposition.
///
/// Why this exists: `serve-batch` is a long-running service deployed by the OSS Helm
/// chart, and until now it served exactly `/v1/health`. The documented way to collect
/// forge metrics was `forge status --prometheus` on a timer — a CLI read of a
/// checkpoint file — which is impossible in the shipped container, because the image
/// is `FROM scratch` and has no shell to run it from.
///
/// Both metric kinds are present on purpose:
///
/// - **Counters** (`_total`) are process-lifetime and monotonic, so `rate()` answers
///   "are we still completing batches?" — the question an alert actually asks.
/// - **Gauges** are current levels (how many batches are in progress, how many items
///   are pending right now). A level cannot be rated and a rate cannot be levelled;
///   exposing only gauges, which is what `status --prometheus` does, leaves the
///   throughput question unanswerable.
fn serve_metrics<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    req: tiny_http::Request,
    state: &Arc<ServerState<F>>,
) {
    use std::fmt::Write;

    // Snapshot ids and statuses under the lock, then read each batch's live counts
    // outside it — `read_counts` takes the same lock, and holding it across a queue
    // read would serialize every request behind a scrape.
    let (ids, files): (Vec<(String, String)>, usize) = {
        let reg = state.registry.lock().unwrap();
        (
            reg.batches
                .iter()
                .map(|(id, e)| (id.clone(), e.record.status.clone()))
                .collect(),
            reg.files.len(),
        )
    };

    let mut by_status: BTreeMap<&str, u64> = BTreeMap::new();
    for (_, status) in &ids {
        *by_status.entry(status.as_str()).or_default() += 1;
    }
    // Report every status the server can produce, including the ones at zero, so a
    // dashboard does not show a gap when nothing has failed yet.
    for st in [
        S_IN_PROGRESS,
        S_COMPLETED,
        S_CANCELLING,
        S_CANCELLED,
        S_FAILED,
    ] {
        by_status.entry(st).or_insert(0);
    }

    let mut items = forge_core::QueueCounts::default();
    for (id, status) in &ids {
        if status != S_IN_PROGRESS {
            continue; // finished batches' queues tell us nothing about current load
        }
        if let Some(c) = read_counts(rt, state, id) {
            items.pending += c.pending;
            items.leased += c.leased;
            items.done += c.done;
            items.dead += c.dead;
        }
    }

    let m = &state.metrics;
    let mut s = String::new();
    let _ = writeln!(
        s,
        "# HELP forge_batches Batches currently known, by status."
    );
    let _ = writeln!(s, "# TYPE forge_batches gauge");
    for (status, n) in &by_status {
        let _ = writeln!(s, "forge_batches{{status=\"{status}\"}} {n}");
    }
    let _ = writeln!(
        s,
        "# HELP forge_files Uploaded input/output files retained."
    );
    let _ = writeln!(s, "# TYPE forge_files gauge");
    let _ = writeln!(s, "forge_files {files}");
    let _ = writeln!(
        s,
        "# HELP forge_batch_items Items across in-progress batches, by state."
    );
    let _ = writeln!(s, "# TYPE forge_batch_items gauge");
    for (state_name, v) in [
        ("pending", items.pending),
        ("leased", items.leased),
        ("done", items.done),
        ("dead", items.dead),
    ] {
        let _ = writeln!(s, "forge_batch_items{{state=\"{state_name}\"}} {v}");
    }
    for (name, help, v) in [
        (
            "forge_batches_created_total",
            "Batches accepted since start.",
            m.batches_created.load(Ordering::Relaxed),
        ),
        (
            "forge_batches_completed_total",
            "Batches that reached `completed` since start.",
            m.batches_completed.load(Ordering::Relaxed),
        ),
        (
            "forge_batches_cancelled_total",
            "Batches cancelled since start.",
            m.batches_cancelled.load(Ordering::Relaxed),
        ),
        (
            "forge_batches_failed_total",
            "Batches that reached `failed` since start.",
            m.batches_failed.load(Ordering::Relaxed),
        ),
        (
            "forge_files_uploaded_total",
            "Files uploaded since start.",
            m.files_uploaded.load(Ordering::Relaxed),
        ),
        (
            "forge_http_requests_total",
            "HTTP requests served since start.",
            m.http_requests.load(Ordering::Relaxed),
        ),
        (
            "forge_http_errors_total",
            "HTTP requests answered 401 or 404 since start.",
            m.http_errors.load(Ordering::Relaxed),
        ),
    ] {
        let _ = writeln!(s, "# HELP {name} {help}");
        let _ = writeln!(s, "# TYPE {name} counter");
        let _ = writeln!(s, "{name} {v}");
    }

    let header = tiny_http::Header::from_bytes(
        &b"Content-Type"[..],
        &b"text/plain; version=0.0.4; charset=utf-8"[..],
    )
    .expect("static header");
    let _ = req.respond(tiny_http::Response::from_string(s).with_header(header));
}

/// Read the live queue counts for a batch, opening (and caching) a read handle to its
/// `ckpt.db` on first use. `None` if the queue can't be read (e.g. never hydrated).
fn read_counts<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    state: &Arc<ServerState<F>>,
    id: &str,
) -> Option<forge_core::QueueCounts> {
    let read_q = {
        let mut reg = state.registry.lock().unwrap();
        let entry = reg.batches.get_mut(id)?;
        if entry.read_queue.is_none() {
            let ckpt = entry.ckpt_path();
            if ckpt.exists() {
                entry.read_queue = SqliteQueue::open(&ckpt).ok().map(Arc::new);
            }
        }
        entry.read_queue.clone()
    };
    let q = read_q?;
    rt.block_on(q.counts()).ok()
}

fn get_batch<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    req: tiny_http::Request,
    state: &Arc<ServerState<F>>,
    id: &str,
) {
    // Snapshot status + whether a run task is still attached (this-process only).
    let (status, task_running, exists) = {
        let reg = state.registry.lock().unwrap();
        match reg.batches.get(id) {
            Some(e) => (e.record.status.clone(), e.task.is_some(), true),
            None => (String::new(), false, false),
        }
    };
    if !exists {
        return respond_text(req, 404, "no such batch");
    }

    let counts = read_counts(rt, state, id);
    let rc = counts
        .map(|c| RequestCounts {
            total: c.total(),
            completed: c.done,
            failed: c.dead,
        })
        .unwrap_or_default();

    // Restart recovery: a batch left `in_progress` with no running task (its process
    // died) whose queue is now drained is really finished — finalize it on read so its
    // results become fetchable. We require total > 0 so a not-yet-hydrated batch isn't
    // falsely completed.
    if status == S_IN_PROGRESS
        && !task_running
        && counts
            .map(|c| c.total() > 0 && c.is_drained())
            .unwrap_or(false)
    {
        finalize_completed(state, id);
    }

    let object = {
        let reg = state.registry.lock().unwrap();
        match reg.batches.get(id) {
            Some(e) => record_to_object(&e.record, rc),
            None => return respond_text(req, 404, "no such batch"),
        }
    };
    respond_json(req, 200, serde_json::to_string(&object).unwrap_or_default());
}

fn list_batches<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    req: tiny_http::Request,
    state: &Arc<ServerState<F>>,
) {
    // Snapshot the ids first, then read counts per id without holding the lock across
    // the async count reads.
    let ids: Vec<String> = {
        let reg = state.registry.lock().unwrap();
        reg.batches.keys().cloned().collect()
    };
    let mut data = Vec::with_capacity(ids.len());
    for id in &ids {
        let counts = read_counts(rt, state, id);
        let rc = counts
            .map(|c| RequestCounts {
                total: c.total(),
                completed: c.done,
                failed: c.dead,
            })
            .unwrap_or_default();
        let reg = state.registry.lock().unwrap();
        if let Some(e) = reg.batches.get(id) {
            data.push(record_to_object(&e.record, rc));
        }
    }
    // Newest first (a friendly default; OpenAI paginates, which the MVP does not).
    data.sort_by_key(|b| std::cmp::Reverse(b.created_at));
    let env = ListEnvelope::new(data);
    respond_json(req, 200, serde_json::to_string(&env).unwrap_or_default());
}

fn cancel_batch<F: WorkerFleet>(
    rt: &tokio::runtime::Handle,
    req: tiny_http::Request,
    state: &Arc<ServerState<F>>,
    id: &str,
) {
    {
        let mut reg = state.registry.lock().unwrap();
        let Some(entry) = reg.batches.get_mut(id) else {
            return respond_text(req, 404, "no such batch");
        };
        // Already terminal → return it unchanged (can't cancel a finished batch).
        if entry.record.status == S_IN_PROGRESS {
            // Flip the cooperative cancel flag; the supervisor drops the run future within
            // ~100ms. We can't cleanly drain BatchRun mid-flight (it owns its loop), so we
            // stop it: results already written stay; in-flight leased items are simply not
            // retried (their leases expire in the ckpt.db). We transition straight to
            // `cancelled` — the brief `cancelling` state is elided because the flag flip is
            // synchronous from the server's view — and stop reporting further progress.
            entry.cancel.store(true, Ordering::SeqCst);
            entry.record.status = S_CANCELLED.to_string();
            state
                .metrics
                .batches_cancelled
                .fetch_add(1, Ordering::Relaxed);
            entry.record.cancelled_at = Some(now_ms());
            persist_batch(&entry.dir, &entry.record);
        }
    }
    let _ = S_CANCELLING; // documented-but-elided intermediate state

    // Live counts, exactly like get_batch/list_batches — the cancelled batch's progress
    // at the moment of cancellation, not a hardcoded all-zero placeholder.
    let counts = read_counts(rt, state, id);
    let rc = counts
        .map(|c| RequestCounts {
            total: c.total(),
            completed: c.done,
            failed: c.dead,
        })
        .unwrap_or_default();
    let object = {
        let reg = state.registry.lock().unwrap();
        match reg.batches.get(id) {
            Some(e) => record_to_object(&e.record, rc),
            None => return respond_text(req, 404, "no such batch"),
        }
    };
    respond_json(req, 200, serde_json::to_string(&object).unwrap_or_default());
}

// ───────────────────────────── http helpers ─────────────────────────────

fn bearer_matches(auth: Option<&str>, expected: &str) -> bool {
    auth.and_then(|a| a.strip_prefix("Bearer "))
        .map(|t| constant_time_eq(t.trim().as_bytes(), expected.as_bytes()))
        .unwrap_or(false)
}

/// Constant-time byte comparison — this guards the server's only auth boundary, so a
/// length-and-short-circuit `==` would leak a timing side-channel on the API key. No new
/// dependency: XOR-accumulate every byte (padding the shorter side) so the comparison
/// always touches every byte of both inputs regardless of where they first differ.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len_diff = (a.len() != b.len()) as u8;
    let n = a.len().max(b.len());
    let mut diff = len_diff;
    for i in 0..n {
        diff |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

fn header_value(headers: &[tiny_http::Header], field: &str) -> Option<String> {
    headers
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(field))
        .map(|h| h.value.as_str().to_string())
}

/// Extract a query-string parameter value (`?k=v&...`) from a raw request URL.
fn query_param(raw_url: &str, key: &str) -> Option<String> {
    let (_, query) = raw_url.split_once('?')?;
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == key).then(|| v.to_string())
    })
}

fn json_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
        .expect("static header")
}

/// The `x-request-id` header for the request in scope, if one is bound.
///
/// Attached to EVERY response, not just errors: "quote the request id from the
/// response" only works as support advice if a successful-but-wrong response has one
/// too. Built from the sanitised id, so it cannot inject a header.
fn request_id_header() -> Option<tiny_http::Header> {
    let id = forge_core::current_request_id()?;
    tiny_http::Header::from_bytes(&b"x-request-id"[..], id.as_bytes()).ok()
}

fn respond_json(req: tiny_http::Request, code: u16, body: String) {
    let mut resp = tiny_http::Response::from_string(body)
        .with_header(json_header())
        .with_status_code(code);
    if let Some(h) = request_id_header() {
        resp.add_header(h);
    }
    let _ = req.respond(resp);
}

fn respond_text(req: tiny_http::Request, code: u16, msg: &str) {
    let mut resp = tiny_http::Response::from_string(msg.to_string()).with_status_code(code);
    if let Some(h) = request_id_header() {
        resp.add_header(h);
    }
    let _ = req.respond(resp);
}

/// Respond to an internal failure without describing the server.
///
/// The detail goes to the log against the request id; the caller gets the id and a
/// fixed sentence. `context` is a short static label (`"persisting upload"`), never
/// interpolated user or path data.
fn respond_internal(req: tiny_http::Request, context: &'static str, err: impl std::fmt::Display) {
    let id = forge_core::current_request_id().unwrap_or_else(forge_core::mint_request_id);
    tracing::error!(request_id = %id, context, error = %err, "request failed");
    let mut resp = tiny_http::Response::from_string(forge_core::internal_error_body(&id))
        .with_header(json_header())
        .with_status_code(500);
    if let Some(h) = request_id_header() {
        resp.add_header(h);
    }
    let _ = req.respond(resp);
}

fn respond_unauthorized(req: tiny_http::Request) {
    let hdr =
        tiny_http::Header::from_bytes(&b"WWW-Authenticate"[..], &b"Bearer"[..]).expect("static");
    let resp = tiny_http::Response::from_string("unauthorized")
        .with_header(hdr)
        .with_status_code(401);
    let _ = req.respond(resp);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_match_logic() {
        assert!(bearer_matches(Some("Bearer sk-123"), "sk-123"));
        assert!(!bearer_matches(Some("Bearer sk-123"), "sk-999"));
        assert!(!bearer_matches(Some("sk-123"), "sk-123")); // missing scheme
        assert!(!bearer_matches(None, "sk-123"));
    }

    #[test]
    fn constant_time_eq_matches_regular_equality() {
        assert!(constant_time_eq(b"sk-123", b"sk-123"));
        assert!(!constant_time_eq(b"sk-123", b"sk-999"));
        assert!(!constant_time_eq(b"short", b"much-longer"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn query_param_extraction() {
        assert_eq!(
            query_param("/v1/files?filename=my.jsonl", "filename"),
            Some("my.jsonl".to_string())
        );
        assert_eq!(query_param("/v1/files", "filename"), None);
        assert_eq!(query_param("/v1/files?a=1&b=2", "b"), Some("2".to_string()));
    }

    #[test]
    fn rand_hex_is_hex_of_expected_len() {
        let h = rand_hex(12);
        assert_eq!(h.len(), 24);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn read_body_capped_allows_exactly_the_limit() {
        let data = vec![b'x'; 10];
        let mut reader = data.as_slice();
        let out = read_body_capped(&mut reader, 10).unwrap();
        assert_eq!(out, Some(data));
    }

    #[test]
    fn read_body_capped_rejects_one_byte_over() {
        let data = vec![b'x'; 11];
        let mut reader = data.as_slice();
        let out = read_body_capped(&mut reader, 10).unwrap();
        assert_eq!(out, None, "a body one byte over the cap must be rejected");
    }
}
