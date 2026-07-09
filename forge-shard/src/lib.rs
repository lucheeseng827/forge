//! forge-shard — streaming JSONL ingest.
//!
//! Reads a (potentially 50M-line) input file **line by line**, parses each into an
//! [`Item`] keyed by `custom_id`, validates the id, and hydrates the [`Queue`] in
//! batches. The file never enters RAM whole.
//!
//! The streaming reader, parsing, `custom_id` validation, batched hydrate, and the
//! **parse-error dead-letter** sidecar (rejected lines preserved for repair, never
//! abort) are **implemented**. [`import_batch`] migrates an OpenAI / Anthropic /
//! Bedrock batch file into clean forge-native JSONL (the off-ramp from the closed
//! Batch APIs). A byte-offset [`Shard`](forge_core::Shard) index is recorded per
//! hydrated batch, so an interrupted ingest that is re-run **seeks** to the first
//! un-hydrated offset instead of rescanning — see [`ingest_jsonl`].

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use forge_core::{EndpointKind, ForgeError, Item, Queue, Shard};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

pub mod bucket;
pub mod verify;
pub use verify::{verify_completeness, VerifyConfig, VerifyReport};

/// How many parsed items to buffer before a single `enqueue` round trip.
const HYDRATE_BATCH: usize = 1_000;

/// Outcome of an ingest pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestStats {
    /// Non-empty lines read from the file.
    pub lines_read: u64,
    /// Rows newly inserted into the queue (idempotent: re-ingest counts 0).
    pub hydrated: u64,
    /// Lines rejected as malformed or with an invalid `custom_id`.
    pub rejected: u64,
}

/// A lazily-opened sidecar that preserves rejected input lines (malformed JSON or
/// an invalid `custom_id`) for repair. One JSON record per reject —
/// `{"line_no", "error", "input"}` — keeping the raw line so a 50M-line job can be
/// patched and re-ingested without re-deriving which records were dropped. The
/// file is created only when the first reject appears, so a clean run leaves no
/// empty sidecar behind. This is the ingest analogue of the store's dead-letter
/// JSONL: a parse error never has a `custom_id`, so it cannot ride the queue's
/// dead-letter path and needs its own sink.
struct RejectSink {
    path: PathBuf,
    file: Option<File>,
}

impl RejectSink {
    fn new(path: PathBuf) -> Self {
        Self { path, file: None }
    }

    async fn write(&mut self, line_no: u64, error: &str, raw: &str) -> Result<(), ForgeError> {
        if self.file.is_none() {
            if let Some(parent) = self.path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)
                    .await?,
            );
        }
        let rec = serde_json::json!({ "line_no": line_no, "error": error, "input": raw });
        let f = self.file.as_mut().unwrap();
        f.write_all(serde_json::to_string(&rec)?.as_bytes()).await?;
        f.write_all(b"\n").await?;
        f.flush().await?;
        Ok(())
    }
}

/// Best-effort "these two paths are the same file" check, to guard commands that
/// would truncate their own input (`File::create(out)` before reading `input`).
/// Catches an identical path spelling and — when both already exist — the same
/// canonical target (symlinks, `./`, `..`).
pub(crate) fn same_file(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    matches!(
        (a.canonicalize(), b.canonicalize()),
        (Ok(ca), Ok(cb)) if ca == cb
    )
}

/// Anthropic's `custom_id` charset: `^[a-zA-Z0-9_-]{1,64}$`. We adopt it verbatim
/// so a file from a closed batch API ingests unchanged.
pub fn valid_custom_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Parse and validate one JSONL line into an [`Item`].
pub fn parse_line(line: &str) -> Result<Item, ForgeError> {
    let item: Item = serde_json::from_str(line)?;
    if !valid_custom_id(&item.custom_id) {
        return Err(ForgeError::Ingest(format!(
            "invalid custom_id {:?} (expected ^[a-zA-Z0-9_-]{{1,64}}$)",
            item.custom_id
        )));
    }
    Ok(item)
}

/// Stream `path` line-by-line and hydrate `queue`. Idempotent: re-running over the
/// same file after a resume inserts nothing new (the queue dedups on `custom_id`).
///
/// Malformed lines are counted in [`IngestStats::rejected`] and skipped rather than
/// aborting the whole ingest — one bad line must not sink a 50M-line job. To also
/// preserve those lines for repair, use [`ingest_jsonl_with_rejects`].
///
/// A byte-offset [`Shard`] is recorded after each batch's `enqueue` commits, so an
/// interrupted ingest that is re-run **seeks** to the first un-hydrated offset (from
/// [`Queue::hydrated_through`]) instead of rescanning — see [`ingest_jsonl_with_rejects`].
pub async fn ingest_jsonl<Q: Queue>(
    queue: &Q,
    path: impl AsRef<Path>,
) -> Result<IngestStats, ForgeError> {
    ingest_jsonl_with_rejects(queue, path, None::<&Path>).await
}

/// Like [`ingest_jsonl`], but rejected lines are also appended to a parse-error
/// dead-letter sidecar at `reject_path` (one `{"line_no","error","input"}` record
/// each) so a malformed 50M-line input can be patched and re-ingested. Pass `None`
/// to skip the sidecar (rejects are still counted and logged).
///
/// **Seek-resume.** Before reading, this asks the queue how far a prior ingest durably
/// hydrated ([`Queue::hydrated_through`]) and seeks straight there, so re-running an
/// interrupted ingest skips the already-hydrated prefix. After each batch's `enqueue`
/// commits it records a [`Shard`] of the byte range just consumed. Recording *after*
/// the commit keeps the checkpoint conservative: the recorded offset is always ≤ what
/// is actually hydrated, so a seek can only ever re-read a little (which `enqueue`
/// dedups), never skip an un-hydrated line. A backend without a seek index no-ops both
/// calls and simply re-scans from the start — correct, just not optimized.
///
/// **Caveat — the seek assumes an unchanged input prefix.** The recorded offset is a byte
/// position into *this* file. A checkpoint offset **past the current end of file** (the
/// input was replaced/truncated *shorter*) is detected and falls back to a **full rescan**
/// from the top — so a shrunk input can't silently ingest nothing. But an input that was
/// *edited in place* to the same-or-greater length still lets the offset land mid-line and
/// resume from a garbled boundary; dedup on `custom_id` prevents *duplicate* hydration, not
/// skipped/corrupted lines. Resume must point at the same (append-only) input, or start
/// from a fresh checkpoint when the input changes.
pub async fn ingest_jsonl_with_rejects<Q: Queue>(
    queue: &Q,
    path: impl AsRef<Path>,
    reject_path: Option<impl AsRef<Path>>,
) -> Result<IngestStats, ForgeError> {
    let mut file = tokio::fs::File::open(path).await?;
    let (mut start_offset, mut start_line) = queue.hydrated_through().await?.unwrap_or((0, 0));
    if start_offset > 0 {
        // Only seek if the checkpoint offset is actually within *this* file. If the input
        // was replaced/truncated shorter than the recorded offset, seeking would land past
        // EOF and silently ingest nothing (a data-loss footgun); rescan from the top
        // instead — `enqueue` dedups any already-hydrated ids, so a full rescan is safe,
        // just slower.
        let len = file.metadata().await?.len();
        if start_offset <= len {
            file.seek(std::io::SeekFrom::Start(start_offset)).await?;
            tracing::info!(start_offset, start_line, "resuming ingest from checkpoint");
        } else {
            tracing::warn!(
                start_offset,
                file_len = len,
                "input is shorter than the checkpoint offset (replaced/truncated); \
                 rescanning from the start"
            );
            start_offset = 0;
            start_line = 0;
        }
    }

    let mut reader = BufReader::new(file);
    let mut rejects = reject_path.map(|p| RejectSink::new(p.as_ref().to_path_buf()));

    let mut stats = IngestStats::default();
    let mut batch: Vec<Item> = Vec::with_capacity(HYDRATE_BATCH);
    // Absolute positions (continue past a seek) so recorded Shards stay accurate.
    let mut line_no = start_line;
    let mut offset = start_offset;
    let mut batch_start_offset = start_offset;
    let mut batch_start_line = start_line;

    let mut line = String::new();
    loop {
        line.clear();
        // `read_line` (vs `.lines()`) exposes the byte count, so we track the exact
        // offset — and it always lands on a `\n` boundary, safe to seek back to.
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF
        }
        offset += n as u64;
        line_no += 1;
        // `read_line` keeps the trailing newline; strip it so parsing and the reject
        // sidecar see exactly the content `.lines()` used to yield.
        let content = line.trim_end_matches(['\r', '\n']);
        if content.trim().is_empty() {
            continue;
        }
        stats.lines_read += 1;
        match parse_line(content) {
            Ok(item) => {
                batch.push(item);
                if batch.len() >= HYDRATE_BATCH {
                    let count = batch.len() as u64;
                    stats.hydrated += queue.enqueue(&batch).await?;
                    batch.clear();
                    checkpoint_shard(
                        queue,
                        batch_start_offset,
                        offset,
                        batch_start_line,
                        line_no,
                        count,
                    )
                    .await?;
                    batch_start_offset = offset;
                    batch_start_line = line_no;
                }
            }
            Err(err) => {
                stats.rejected += 1;
                // line_no pinpoints the offending record; the sidecar keeps the raw
                // line so it can be fixed and re-ingested.
                tracing::warn!(line_no, error = %err, "rejected input line");
                if let Some(sink) = rejects.as_mut() {
                    sink.write(line_no, &err.to_string(), content).await?;
                }
            }
        }
    }
    if !batch.is_empty() {
        let count = batch.len() as u64;
        stats.hydrated += queue.enqueue(&batch).await?;
        checkpoint_shard(
            queue,
            batch_start_offset,
            offset,
            batch_start_line,
            line_no,
            count,
        )
        .await?;
    }
    Ok(stats)
}

/// Record the byte range a just-committed batch consumed as a [`Shard`] (the ingest
/// checkpoint the next run seeks past). Best-effort: a queue with no seek index no-ops.
async fn checkpoint_shard<Q: Queue>(
    queue: &Q,
    byte_start: u64,
    byte_end: u64,
    line_start: u64,
    line_end: u64,
    lines_hydrated: u64,
) -> Result<(), ForgeError> {
    queue
        .record_shard(&Shard {
            shard_id: 0, // the backend assigns its own id (append-only checkpoints)
            byte_offset_start: byte_start,
            byte_offset_end: byte_end,
            line_start,
            line_end,
            lines_hydrated,
        })
        .await
}

/// Outcome of an [`import_batch`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImportStats {
    /// Non-empty lines read from the source file.
    pub lines_read: u64,
    /// Lines normalized and written to the forge-native output.
    pub written: u64,
    /// Lines rejected (unusable shape / invalid `custom_id`).
    pub rejected: u64,
    /// Lines dropped because their `custom_id` already appeared (the closed Batch
    /// APIs require unique ids; forge would otherwise dedup them on hydrate).
    pub duplicates: u64,
}

/// A single batch line normalized to forge-native fields.
struct Normalized {
    custom_id: String,
    /// The clean `{custom_id, method, url, body}` record to emit.
    record: serde_json::Value,
}

/// Infer the request path from the body shape when the source line omits a `url`
/// (Anthropic Message Batches and Bedrock are single-endpoint and carry none).
fn infer_url(body: &serde_json::Value, default_endpoint: EndpointKind) -> &'static str {
    if body.get("messages").is_some() {
        EndpointKind::Chat.default_path()
    } else if body.get("input").is_some() {
        EndpointKind::Embeddings.default_path()
    } else if body.get("prompt").is_some() {
        EndpointKind::Completions.default_path()
    } else {
        default_endpoint.default_path()
    }
}

/// Normalize one closed-Batch-API line into forge-native fields. Accepts all three
/// shapes by sniffing field names:
/// - **id:** `custom_id` (OpenAI / Anthropic) or `recordId` (Bedrock).
/// - **body:** `body` (OpenAI), `params` (Anthropic Message Batches), or
///   `modelInput` (Bedrock).
/// - **url:** explicit `url` (OpenAI), else inferred from the body shape, else the
///   `default_endpoint` path.
fn normalize_line(line: &str, default_endpoint: EndpointKind) -> Result<Normalized, ForgeError> {
    let v: serde_json::Value = serde_json::from_str(line)?;

    let custom_id = v
        .get("custom_id")
        .or_else(|| v.get("recordId"))
        .and_then(|x| x.as_str())
        .ok_or_else(|| ForgeError::Ingest("missing custom_id / recordId".into()))?;
    if !valid_custom_id(custom_id) {
        return Err(ForgeError::Ingest(format!(
            "invalid custom_id {custom_id:?} (expected ^[a-zA-Z0-9_-]{{1,64}}$)"
        )));
    }

    let body = v
        .get("body")
        .or_else(|| v.get("params"))
        .or_else(|| v.get("modelInput"))
        .ok_or_else(|| ForgeError::Ingest("missing body / params / modelInput".into()))?;
    if !body.is_object() {
        return Err(ForgeError::Ingest(
            "body / params / modelInput is not a JSON object".into(),
        ));
    }

    let method = v.get("method").and_then(|x| x.as_str()).unwrap_or("POST");
    let url = v
        .get("url")
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| infer_url(body, default_endpoint).to_string());

    let record = serde_json::json!({
        "custom_id": custom_id,
        "method": method,
        "url": url,
        "body": body,
    });
    Ok(Normalized {
        custom_id: custom_id.to_string(),
        record,
    })
}

/// Migrate an OpenAI / Anthropic / Bedrock batch JSONL into a clean forge-native
/// JSONL (`custom_id` / `method` / `url` / `body`) ready for `forge run`. Streams
/// line-by-line (never holds the file in RAM). Unusable lines and duplicate
/// `custom_id`s go to the reject sidecar at `reject_path` (if `Some`) with the raw
/// line preserved; everything else is normalized and written to `out`.
///
/// The dedup set is bounded by the count of unique ids — fine at v1 scale; a 50M
/// run would want a probabilistic set instead.
pub async fn import_batch(
    input: impl AsRef<Path>,
    out: impl AsRef<Path>,
    reject_path: Option<impl AsRef<Path>>,
    default_endpoint: EndpointKind,
) -> Result<ImportStats, ForgeError> {
    let input = input.as_ref();
    // Guard the destructive footgun: `File::create(out)` truncates before the read
    // loop finishes, so importing a file onto itself would silently wipe the source.
    if same_file(input, out.as_ref()) {
        return Err(ForgeError::Ingest(format!(
            "refusing to import {} onto itself (--input and --out are the same file)",
            input.display()
        )));
    }
    let file = File::open(input).await?;
    let mut lines = BufReader::new(file).lines();
    let mut rejects = reject_path.map(|p| RejectSink::new(p.as_ref().to_path_buf()));

    let out = out.as_ref();
    if let Some(parent) = out.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    let mut writer = BufWriter::new(File::create(out).await?);

    let mut seen: HashSet<String> = HashSet::new();
    let mut stats = ImportStats::default();
    let mut line_no: u64 = 0;

    while let Some(line) = lines.next_line().await? {
        line_no += 1;
        if line.trim().is_empty() {
            continue;
        }
        stats.lines_read += 1;
        match normalize_line(&line, default_endpoint) {
            Ok(norm) => {
                if !seen.insert(norm.custom_id) {
                    stats.duplicates += 1;
                    if let Some(sink) = rejects.as_mut() {
                        sink.write(line_no, "duplicate custom_id", &line).await?;
                    }
                    continue;
                }
                writer
                    .write_all(serde_json::to_string(&norm.record)?.as_bytes())
                    .await?;
                writer.write_all(b"\n").await?;
                stats.written += 1;
            }
            Err(err) => {
                stats.rejected += 1;
                tracing::warn!(line_no, error = %err, "rejected import line");
                if let Some(sink) = rejects.as_mut() {
                    sink.write(line_no, &err.to_string(), &line).await?;
                }
            }
        }
    }
    writer.flush().await?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_id_charset() {
        assert!(valid_custom_id("req-001"));
        assert!(valid_custom_id("a_B-9"));
        assert!(!valid_custom_id("")); // too short
        assert!(!valid_custom_id("has space"));
        assert!(!valid_custom_id("emoji😀"));
        assert!(!valid_custom_id(&"x".repeat(65))); // too long
    }

    #[test]
    fn parse_good_line() {
        let line = r#"{"custom_id":"req-1","body":{"input":"hello"}}"#;
        let item = parse_line(line).unwrap();
        assert_eq!(item.custom_id, "req-1");
    }

    #[test]
    fn parse_rejects_bad_id() {
        let line = r#"{"custom_id":"bad id","body":{}}"#;
        assert!(parse_line(line).is_err());
    }

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("forge-shard-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn ingest_routes_rejects_to_sidecar_and_hydrates_good_lines() {
        use forge_queue::SqliteQueue;

        let dir = tmp("rejects");
        let input = dir.join("in.jsonl");
        let reject = dir.join("out.jsonl.reject.jsonl");
        std::fs::write(
            &input,
            // good · malformed JSON · invalid custom_id · blank (skipped, not a reject)
            "{\"custom_id\":\"req-1\",\"body\":{\"input\":\"hi\"}}\n\
             {not json}\n\
             {\"custom_id\":\"bad id\",\"body\":{}}\n\
             \n",
        )
        .unwrap();

        let queue = SqliteQueue::open_in_memory().unwrap();
        let stats = ingest_jsonl_with_rejects(&queue, &input, Some(&reject))
            .await
            .unwrap();

        assert_eq!(stats.lines_read, 3, "blank line is skipped, not counted");
        assert_eq!(stats.hydrated, 1, "only the one valid line hydrates");
        assert_eq!(stats.rejected, 2, "malformed JSON + bad custom_id");
        assert_eq!(queue.counts().await.unwrap().pending, 1);

        let dead = std::fs::read_to_string(&reject).unwrap();
        assert_eq!(dead.lines().count(), 2, "one record per rejected line");
        // Each record carries the line number, an error, and the raw input for repair.
        let first: serde_json::Value = serde_json::from_str(dead.lines().next().unwrap()).unwrap();
        assert_eq!(first["line_no"], 2);
        assert_eq!(first["input"], "{not json}");
        assert!(first["error"].as_str().unwrap().contains("json"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn item_lines(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("{{\"custom_id\":\"req-{i:04}\",\"body\":{{\"input\":\"hi\"}}}}"))
            .collect()
    }

    #[tokio::test]
    async fn reingest_of_fully_hydrated_input_seeks_to_eof() {
        use forge_queue::SqliteQueue;
        let dir = tmp("seek-eof");
        let input = dir.join("in.jsonl");
        std::fs::write(&input, item_lines(20).join("\n") + "\n").unwrap();

        let queue = SqliteQueue::open_in_memory().unwrap();
        let first = ingest_jsonl(&queue, &input).await.unwrap();
        assert_eq!(first.hydrated, 20);
        assert_eq!(first.lines_read, 20);

        // The ingest recorded a Shard high-water mark at EOF (byte size, line 20).
        let size = std::fs::metadata(&input).unwrap().len();
        assert_eq!(queue.hydrated_through().await.unwrap(), Some((size, 20)));

        // Re-running the same ingest seeks straight to EOF and reads nothing — vs the
        // pre-seek behavior of rescanning all 20 lines.
        let second = ingest_jsonl(&queue, &input).await.unwrap();
        assert_eq!(
            second.lines_read, 0,
            "re-ingest seeks past the hydrated prefix"
        );
        assert_eq!(second.hydrated, 0);
        assert_eq!(queue.counts().await.unwrap().pending, 20, "no duplicates");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn interrupted_ingest_resumes_from_the_recorded_offset() {
        use forge_queue::SqliteQueue;
        let dir = tmp("seek-partial");
        let full = dir.join("full.jsonl");
        let partial = dir.join("partial.jsonl");
        let lines = item_lines(20);
        // `partial` is the exact byte-prefix an interrupted ingest of the first 8 lines
        // would have consumed, so the recorded offset lands on line 8's boundary in `full`.
        std::fs::write(&full, lines.join("\n") + "\n").unwrap();
        std::fs::write(&partial, lines[..8].join("\n") + "\n").unwrap();

        let queue = SqliteQueue::open_in_memory().unwrap();
        let a = ingest_jsonl(&queue, &partial).await.unwrap(); // the "interrupted" pass
        assert_eq!(a.hydrated, 8);
        assert_eq!(queue.counts().await.unwrap().pending, 8);

        // Re-run over the FULL file: ingest seeks past the 8 already-hydrated lines and
        // reads only the remaining 12.
        let b = ingest_jsonl(&queue, &full).await.unwrap();
        assert_eq!(b.lines_read, 12, "only the un-hydrated tail is read");
        assert_eq!(b.hydrated, 12);
        assert_eq!(
            queue.counts().await.unwrap().pending,
            20,
            "all 20 present, none duplicated"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_shorter_replaced_input_rescans_instead_of_seeking_past_eof() {
        use forge_queue::SqliteQueue;
        let dir = tmp("seek-shrink");
        let big = dir.join("big.jsonl");
        let small = dir.join("small.jsonl");
        // `big` (20 lines) records a checkpoint at its EOF offset.
        std::fs::write(&big, item_lines(20).join("\n") + "\n").unwrap();
        // `small` has 5 DIFFERENT ids and is byte-shorter than big's recorded offset.
        let small_lines: Vec<String> = (0..5)
            .map(|i| format!("{{\"custom_id\":\"s{i}\",\"body\":{{}}}}"))
            .collect();
        std::fs::write(&small, small_lines.join("\n") + "\n").unwrap();

        let queue = SqliteQueue::open_in_memory().unwrap();
        assert_eq!(ingest_jsonl(&queue, &big).await.unwrap().hydrated, 20);

        // Re-ingest over the SHORTER file: the recorded offset (big's EOF) is past small's
        // EOF. The guard rescans from the top instead of seeking past EOF and reading
        // nothing — so the 5 new ids are hydrated, not silently dropped.
        let b = ingest_jsonl(&queue, &small).await.unwrap();
        assert_eq!(
            b.lines_read, 5,
            "rescanned the whole shorter file, not zero"
        );
        assert_eq!(
            b.hydrated, 5,
            "the new ids were hydrated, not silently dropped"
        );
        assert_eq!(queue.counts().await.unwrap().pending, 25);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn clean_ingest_leaves_no_sidecar() {
        use forge_queue::SqliteQueue;

        let dir = tmp("clean");
        let input = dir.join("in.jsonl");
        let reject = dir.join("out.jsonl.reject.jsonl");
        std::fs::write(&input, "{\"custom_id\":\"req-1\",\"body\":{}}\n").unwrap();

        let queue = SqliteQueue::open_in_memory().unwrap();
        let stats = ingest_jsonl_with_rejects(&queue, &input, Some(&reject))
            .await
            .unwrap();

        assert_eq!(stats.rejected, 0);
        assert!(!reject.exists(), "no reject file when nothing is rejected");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn read_lines(p: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn import_normalizes_all_three_shapes() {
        let dir = tmp("import");
        let input = dir.join("batch.jsonl");
        let out = dir.join("forge.jsonl");
        let reject = dir.join("forge.jsonl.reject.jsonl");
        std::fs::write(
            &input,
            // OpenAI (native) · Anthropic (custom_id+params, no url) ·
            // Bedrock (recordId+modelInput) · bad (no body) · dup of the first id
            "{\"custom_id\":\"a\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"messages\":[]}}\n\
             {\"custom_id\":\"b\",\"params\":{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}}\n\
             {\"recordId\":\"c\",\"modelInput\":{\"input\":\"embed me\"}}\n\
             {\"custom_id\":\"d\",\"note\":\"no body here\"}\n\
             {\"custom_id\":\"a\",\"body\":{\"messages\":[]}}\n",
        )
        .unwrap();

        let stats = import_batch(&input, &out, Some(&reject), EndpointKind::Chat)
            .await
            .unwrap();
        assert_eq!(stats.lines_read, 5);
        assert_eq!(stats.written, 3, "a, b, c");
        assert_eq!(stats.rejected, 1, "d has no body");
        assert_eq!(stats.duplicates, 1, "the second a");

        let out_lines = read_lines(&out);
        assert_eq!(out_lines.len(), 3);
        // Anthropic: params → body, url inferred from messages → chat.
        let b = out_lines.iter().find(|v| v["custom_id"] == "b").unwrap();
        assert_eq!(b["url"], "/v1/chat/completions");
        assert_eq!(b["method"], "POST");
        assert!(b["body"]["messages"].is_array());
        // Bedrock: recordId → custom_id, modelInput → body, url inferred from input → embeddings.
        let c = out_lines.iter().find(|v| v["custom_id"] == "c").unwrap();
        assert_eq!(c["url"], "/v1/embeddings");
        assert_eq!(c["body"]["input"], "embed me");

        // The reject sidecar holds the bad line and the duplicate, with reasons.
        let rejects = read_lines(&reject);
        assert_eq!(rejects.len(), 2);
        assert!(rejects
            .iter()
            .any(|r| r["error"].as_str().unwrap().contains("body")));
        assert!(rejects.iter().any(|r| r["error"] == "duplicate custom_id"));

        // The output is forge-native and re-parses cleanly via the normal path.
        for v in &out_lines {
            parse_line(&v.to_string()).unwrap();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn import_rejects_invalid_custom_id() {
        let dir = tmp("import-badid");
        let input = dir.join("b.jsonl");
        let out = dir.join("o.jsonl");
        std::fs::write(&input, "{\"custom_id\":\"has space\",\"body\":{}}\n").unwrap();
        let stats = import_batch(&input, &out, None::<&Path>, EndpointKind::Chat)
            .await
            .unwrap();
        assert_eq!(stats.written, 0);
        assert_eq!(stats.rejected, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
