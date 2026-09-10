//! forge-store — the result sink, keyed by `custom_id`, order-independent.
//!
//! The default is a local append-only JSONL file plus a sibling dead-letter JSONL.
//! Writes are **idempotent**: an in-memory emitted-id set (seeded by scanning the
//! existing files on first use) makes a re-run after an expired lease a no-op, so
//! at-least-once execution yields an exactly-once *effect*. A `custom_id` is
//! terminal exactly once — as a success (`put`) **or** a dead-letter, never both —
//! so a single emitted set guards both sinks. `object_store` (S3/GCS/Azure) is the
//! optional object-store backend behind the same [`ResultStore`] trait.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use forge_core::{ForgeError, ItemResult, ResultStore, TokenUsage, UsageTotals};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

// Object-store backend (S3/GCS/Azure), behind the off-by-default
// `object_store` feature so the cloud SDKs never reach the lean musl binary.
#[cfg(feature = "object_store")]
mod object_store_backend;
#[cfg(feature = "object_store")]
pub use object_store_backend::{objstore_from_out, objstore_open_run, open_input_stream, ObjStore};

// Columnar Parquet output, behind the off-by-default `parquet` feature so the
// arrow/parquet stack never reaches the lean musl binary.
#[cfg(feature = "parquet")]
mod parquet_store;
#[cfg(feature = "parquet")]
pub use parquet_store::{forge_schema, jsonl_to_parquet, ParquetOpts, ParquetStore};

/// Append-only local JSONL result sink + a sibling dead-letter file.
pub struct JsonlStore {
    out_path: PathBuf,
    dead_path: PathBuf,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Every `custom_id` already written (to either sink) — the dedup key set.
    emitted: HashSet<String>,
    /// Whether the existing files have been scanned into `emitted` yet.
    loaded: bool,
    /// Lazily-opened append handles.
    out: Option<File>,
    dead: Option<File>,
}

impl JsonlStore {
    /// Results go to `out_path`; dead-letters go to `<out_path>.dead.jsonl`.
    pub fn new(out_path: impl AsRef<Path>) -> Self {
        let out_path = out_path.as_ref().to_path_buf();
        let dead_path = dead_letter_path(&out_path);
        Self {
            out_path,
            dead_path,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Results file path.
    pub fn out_path(&self) -> &Path {
        &self.out_path
    }

    /// Dead-letter file path.
    pub fn dead_path(&self) -> &Path {
        &self.dead_path
    }
}

/// `results.jsonl` → `results.jsonl.dead.jsonl`.
fn dead_letter_path(out: &Path) -> PathBuf {
    let mut s = out.as_os_str().to_os_string();
    s.push(".dead.jsonl");
    PathBuf::from(s)
}

/// Stream a results JSONL and sum its `usage` into [`UsageTotals`] — the input to
/// the cost-arbitrage report (`forge cost`). Counts each result line (anything with
/// a `custom_id`) as one item; a line without a parseable `usage` contributes zero
/// tokens. A missing file is an empty total. Never holds the file in RAM.
pub async fn sum_usage(path: impl AsRef<Path>) -> Result<UsageTotals, ForgeError> {
    Ok(sum_usage_from(path, 0).await?.totals)
}

/// What [`sum_usage_from`] read: the usage, where it stopped, and what it could not
/// account for.
///
/// The counters beyond `totals` exist because a usage-accounting window has to be able
/// to state what it does *not* know. `sum_usage` folds an unreadable line and a line
/// with no usage into the same silent zero; a report built on that would claim a token
/// count for a million items without ever saying that four of them were unreadable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageRead {
    pub totals: UsageTotals,
    /// Byte offset this read began at.
    pub start_offset: u64,
    /// Byte offset immediately after the last **newline-terminated** line consumed.
    ///
    /// Never the file length. If the writer is mid-append, the trailing fragment is
    /// not consumed and this stops before it, so the record that completes that line
    /// is read by the *next* call rather than being split across two — which would
    /// bill it never.
    pub next_offset: u64,
    /// Complete lines consumed (blank lines excluded).
    pub lines_read: u64,
    /// Lines that were not valid JSON, or carried no `custom_id`. Counted, not billed.
    pub lines_unreadable: u64,
    /// Result lines carrying no parseable `usage` object. Billed as zero tokens, which
    /// is what `sum_usage` has always done — but now it is a stated number.
    pub items_without_usage: u64,
    /// True if the file ended in an unterminated fragment, which is a writer mid-append
    /// (or a crash-torn tail). Its bytes are deliberately left for the next read.
    pub ended_mid_line: bool,
}

/// [`sum_usage`], resumable: read a results JSONL from `start_offset` and report both
/// the usage and exactly where it stopped.
///
/// This is what makes a second invoice over a growing results file bill only the new
/// records. The offset is a genuine total order over billable evidence because the
/// results file is strictly append-only.
///
/// **`next_offset` lands only ever on a line boundary.** Setting it to the file length
/// would be the bug this exists to prevent: a coordinator appending concurrently leaves
/// a partial line, and a window that closed at the file length would resume mid-record,
/// so the record completing that line is billed by neither window.
pub async fn sum_usage_from(
    path: impl AsRef<Path>,
    start_offset: u64,
) -> Result<UsageRead, ForgeError> {
    sum_usage_range(path, start_offset, None).await
}

/// [`sum_usage_from`], bounded above: read `[start_offset, end_offset)`.
///
/// The bounded form exists so an already-billed prefix can be re-summed and compared
/// against what an invoice recorded — the check that catches a results file which was
/// rotated, truncated or restored, where the offset alone still points *somewhere*.
pub async fn sum_usage_range(
    path: impl AsRef<Path>,
    start_offset: u64,
    end_offset: Option<u64>,
) -> Result<UsageRead, ForgeError> {
    let mut read = UsageRead {
        start_offset,
        next_offset: start_offset,
        ..Default::default()
    };
    let mut file = match File::open(path.as_ref()).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(read),
        Err(e) => return Err(e.into()),
    };
    if start_offset > 0 {
        let len = file.metadata().await?.len();
        if start_offset > len {
            // The file is shorter than where we left off — rotated, truncated or
            // restored. Refusing beats reading from a plausible-looking offset in
            // unrelated content and producing a number nobody can challenge.
            return Err(ForgeError::Store(format!(
                "results file {} is {len} bytes but the last recorded offset is \
                 {start_offset} — it was truncated, rotated or restored; the read \
                 cannot continue from an offset that no longer exists",
                path.as_ref().display()
            )));
        }
        file.seek(std::io::SeekFrom::Start(start_offset)).await?;
    }

    let mut reader = BufReader::new(file);
    let mut offset = start_offset;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        // `read_until` (not `lines()`) because it keeps the terminator: that is the
        // only way to tell a complete line from the file's unterminated tail, and
        // `lines()` yields both identically.
        if let Some(end) = end_offset {
            if offset >= end {
                break; // reached the requested upper bound
            }
        }
        let n = reader.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            break; // EOF
        }
        if buf.last() != Some(&b'\n') {
            // Unterminated tail: the writer is mid-append. Leave its bytes for the
            // next read and do NOT advance past them.
            read.ended_mid_line = true;
            break;
        }
        offset += n as u64;
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim();
        if line.is_empty() {
            read.next_offset = offset;
            continue;
        }
        read.lines_read += 1;
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(v) if v.get("custom_id").is_some() => {
                // A result line with no parseable `usage` still counts as one ITEM
                // worth zero tokens — that is what `sum_usage` has always done, and
                // this must stay byte-for-byte identical to it. The difference is
                // that the fact is now reported instead of vanishing.
                let usage = v
                    .get("usage")
                    .and_then(|u| serde_json::from_value::<TokenUsage>(u.clone()).ok());
                if usage.is_none() {
                    read.items_without_usage += 1;
                }
                read.totals.add(&usage.unwrap_or_default());
            }
            // Not a result line, or not JSON at all.
            _ => read.lines_unreadable += 1,
        }
        read.next_offset = offset;
    }
    Ok(read)
}

/// Collect the `custom_id` of every line already in `path` (missing file = empty).
async fn scan_ids(path: &Path, set: &mut HashSet<String>) -> Result<(), ForgeError> {
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let mut lines = BufReader::new(file).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            if let Some(id) = v.get("custom_id").and_then(|x| x.as_str()) {
                set.insert(id.to_string());
            }
        }
    }
    Ok(())
}

/// What a [`prune_dead_letters`] pass removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruneReport {
    /// Dead-letter records removed from the file.
    pub removed: u64,
    /// Dead-letter records left in place.
    pub kept: u64,
    /// Requested ids that were not present in the dead-letter file. Not an error —
    /// a requeue is driven by the *queue*, and the two can legitimately disagree
    /// (a previous prune already ran, or the item died before the store saw it) —
    /// but the caller states the number rather than letting it vanish.
    pub not_found: Vec<String>,
    /// Lines that were not valid JSON, or carried no `custom_id`. **Preserved
    /// verbatim**: a line this function cannot understand is not a line it may
    /// throw away.
    pub lines_unreadable: u64,
}

/// Remove `ids` from a dead-letter JSONL, atomically.
///
/// **This is the whole reason a requeue is safe.** [`ensure_loaded`] seeds the
/// emitted-id set from the results file *and* the dead-letter file, and both [`put`]
/// and [`dead_letter`] short-circuit on it — so an item put back on the queue while
/// its dead record still exists would be re-leased, re-run against a real GPU, and
/// have its fresh result **silently discarded**. Pruning the dead record is what
/// makes the id emittable again.
///
/// Written to a temp file beside the target and `rename`d over it, so a crash leaves
/// either the old file or the new one and never a half-written dead-letter ledger.
/// The unterminated tail of a torn file is carried across as its own line, exactly as
/// the scanners already treat it.
///
/// A missing dead-letter file is not an error: nothing has dead-lettered yet, so
/// there is nothing to prune and the report is empty.
pub async fn prune_dead_letters(
    dead_path: impl AsRef<Path>,
    ids: &HashSet<String>,
) -> Result<PruneReport, ForgeError> {
    let dead_path = dead_path.as_ref();
    let mut report = PruneReport::default();
    if ids.is_empty() {
        return Ok(report);
    }
    let raw = match tokio::fs::read(dead_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Nothing has dead-lettered yet, so there is nothing to prune — but every
            // requested id genuinely was not found, and saying otherwise would let a
            // caller read "absent file" as "all removed".
            report.not_found = ids.iter().cloned().collect();
            report.not_found.sort_unstable();
            return Ok(report);
        }
        Err(e) => return Err(e.into()),
    };

    let mut found: HashSet<String> = HashSet::new();
    let mut kept_bytes: Vec<u8> = Vec::with_capacity(raw.len());
    for line in raw.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let text = String::from_utf8_lossy(line);
        let id = serde_json::from_str::<serde_json::Value>(text.trim())
            .ok()
            .and_then(|v| {
                v.get("custom_id")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
            });
        match id {
            Some(id) if ids.contains(&id) => {
                report.removed += 1;
                found.insert(id);
            }
            Some(_) => {
                report.kept += 1;
                kept_bytes.extend_from_slice(line);
                kept_bytes.push(b'\n');
            }
            None => {
                // Unparseable, or no custom_id: keep it. This function removes records
                // it positively identified; it does not tidy a ledger it cannot read.
                report.lines_unreadable += 1;
                kept_bytes.extend_from_slice(line);
                kept_bytes.push(b'\n');
            }
        }
    }
    report.not_found = ids.difference(&found).cloned().collect();
    report.not_found.sort_unstable();

    if report.removed == 0 {
        // Nothing to do — and rewriting the file anyway would risk a good ledger for
        // no gain.
        return Ok(report);
    }

    // Temp beside the target so the rename is same-filesystem (and therefore atomic).
    // PID **and** a per-call counter: two prunes of the same file inside one process
    // would otherwise pick the same temp path and truncate each other's work, and a
    // reused PID does the same across processes.
    static PRUNE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = PRUNE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dead_path.with_extension(format!("prune-{}-{seq}.tmp", std::process::id()));
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)
        .await?;
    f.write_all(&kept_bytes).await?;
    // fsync before the rename: a rename that lands ahead of the data would leave a
    // truncated dead-letter file after a power cut, silently discarding records that
    // were never requeued.
    f.sync_data().await?;
    drop(f);
    tokio::fs::rename(&tmp, dead_path).await?;
    Ok(report)
}

/// Seed the emitted set from both existing files, once.
async fn ensure_loaded(out: &Path, dead: &Path, inner: &mut Inner) -> Result<(), ForgeError> {
    if inner.loaded {
        return Ok(());
    }
    scan_ids(out, &mut inner.emitted).await?;
    scan_ids(dead, &mut inner.emitted).await?;
    inner.loaded = true;
    Ok(())
}

/// True if `path` exists, is non-empty, and its last byte is not `\n` — i.e. a
/// previous coordinator was killed mid-line (a torn write). Appending a new record
/// after such a file without terminating the fragment would concatenate the record
/// onto it, corrupting an otherwise-good result.
async fn ends_mid_line(path: &Path) -> Result<bool, ForgeError> {
    let mut f = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let len = f.seek(std::io::SeekFrom::End(0)).await?;
    if len == 0 {
        return Ok(false);
    }
    f.seek(std::io::SeekFrom::End(-1)).await?;
    let mut last = [0u8; 1];
    f.read_exact(&mut last).await?;
    Ok(last[0] != b'\n')
}

/// Append one JSONL line, opening the handle on first write.
async fn append_line(handle: &mut Option<File>, path: &Path, line: &str) -> Result<(), ForgeError> {
    if handle.is_none() {
        // Crash-safety: if the file ends mid-line from a SIGKILL, terminate that
        // fragment first so this record can't be glued onto it. The fragment stays
        // as its own unparseable line (skipped on the next scan); every `custom_id`
        // still ends with exactly one parseable result.
        let torn = ends_mid_line(path).await?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        if torn {
            f.write_all(b"\n").await?;
        }
        *handle = Some(f);
    }
    let f = handle.as_mut().unwrap();
    f.write_all(line.as_bytes()).await?;
    f.write_all(b"\n").await?;
    f.flush().await?;
    Ok(())
}

impl ResultStore for JsonlStore {
    async fn put(&self, result: &ItemResult) -> Result<(), ForgeError> {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        ensure_loaded(&self.out_path, &self.dead_path, inner).await?;
        if inner.emitted.contains(&result.custom_id) {
            return Ok(()); // idempotent / dedup-on-resume
        }
        let line = serde_json::to_string(result)?;
        append_line(&mut inner.out, &self.out_path, &line).await?;
        inner.emitted.insert(result.custom_id.clone());
        Ok(())
    }

    async fn dead_letter(&self, result: &ItemResult) -> Result<(), ForgeError> {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        ensure_loaded(&self.out_path, &self.dead_path, inner).await?;
        if inner.emitted.contains(&result.custom_id) {
            return Ok(());
        }
        let line = serde_json::to_string(result)?;
        append_line(&mut inner.dead, &self.dead_path, &line).await?;
        inner.emitted.insert(result.custom_id.clone());
        Ok(())
    }

    async fn emitted_ids(&self) -> Result<HashSet<String>, ForgeError> {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard;
        ensure_loaded(&self.out_path, &self.dead_path, inner).await?;
        Ok(inner.emitted.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch DIRECTORY (the module's `tmp` returns a file path).
    fn pdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("forge-prune-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn failed_result(id: &str, msg: &str) -> ItemResult {
        ItemResult {
            custom_id: id.into(),
            response: None,
            error: Some(forge_core::ItemError {
                code: "retries_exhausted".into(),
                message: msg.into(),
            }),
            usage: TokenUsage::default(),
            worker_id: "w0".into(),
            latency_ms: 1,
            attempt: 3,
            completed_at: 0,
        }
    }

    fn dead_line(id: &str, msg: &str) -> String {
        format!(
            "{{\"custom_id\":\"{id}\",\"response\":null,\"error\":{{\"message\":\"{msg}\"}},\
              \"usage\":{{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}},\
              \"worker_id\":\"w\",\"latency_ms\":1,\"attempt\":3,\"completed_at\":0}}"
        )
    }

    #[tokio::test]
    async fn pruning_a_dead_letter_makes_the_id_emittable_again() {
        // The whole point. `ensure_loaded` seeds the emitted set from the dead file, so
        // an item requeued while its dead record stands would re-run against a real GPU
        // and have the result SILENTLY dropped by `put`'s short-circuit.
        let dir = pdir("prune-emittable");
        let out = dir.join("results.jsonl");
        let store = JsonlStore::new(&out);
        let dead = store.dead_path().to_path_buf();

        store
            .dead_letter(&failed_result("a", "boom"))
            .await
            .unwrap();
        store
            .dead_letter(&failed_result("b", "boom"))
            .await
            .unwrap();
        assert_eq!(store.emitted_ids().await.unwrap().len(), 2);

        let ids: HashSet<String> = ["a".to_string()].into_iter().collect();
        let rep = prune_dead_letters(&dead, &ids).await.unwrap();
        assert_eq!(rep.removed, 1);
        assert_eq!(rep.kept, 1);
        assert!(rep.not_found.is_empty());

        // A FRESH store (the requeued run's coordinator) no longer knows about "a",
        // so its result is written rather than discarded — while "b" stays guarded.
        let reopened = JsonlStore::new(&out);
        let ids = reopened.emitted_ids().await.unwrap();
        assert!(!ids.contains("a"), "the pruned id must be emittable again");
        assert!(
            ids.contains("b"),
            "an untouched dead-letter still guards its id"
        );
        reopened.put(&ok_result("a")).await.unwrap();
        assert!(std::fs::read_to_string(&out)
            .unwrap()
            .contains("\"custom_id\":\"a\""));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn pruning_never_discards_a_line_it_cannot_read() {
        // A dead-letter file is the record of what a customer's job failed to produce.
        // A line this function cannot parse is not a line it may throw away.
        let dir = pdir("prune-unreadable");
        let dead = dir.join("results.jsonl.dead.jsonl");
        std::fs::write(
            &dead,
            format!(
                "{}\n{{not json}}\n{{\"no\":\"custom_id\"}}\n{}\n",
                dead_line("a", "boom"),
                dead_line("b", "boom")
            ),
        )
        .unwrap();

        let ids: HashSet<String> = ["a".to_string()].into_iter().collect();
        let rep = prune_dead_letters(&dead, &ids).await.unwrap();
        assert_eq!(rep.removed, 1);
        assert_eq!(rep.lines_unreadable, 2);
        let left = std::fs::read_to_string(&dead).unwrap();
        assert!(left.contains("{not json}"), "kept verbatim");
        assert!(left.contains("\"no\":\"custom_id\""), "kept verbatim");
        assert!(left.contains("\"custom_id\":\"b\""));
        assert!(!left.contains("\"custom_id\":\"a\""));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn pruning_is_idempotent_and_reports_ids_it_did_not_find() {
        // A requeue is driven by the QUEUE, so the two can legitimately disagree — a
        // previous prune already ran, or the item died before the store saw it. That is
        // not an error, but the number is stated rather than vanishing.
        let dir = pdir("prune-idem");
        let dead = dir.join("d.jsonl");
        std::fs::write(&dead, format!("{}\n", dead_line("a", "boom"))).unwrap();

        let ids: HashSet<String> = ["a", "ghost"].iter().map(|s| s.to_string()).collect();
        let first = prune_dead_letters(&dead, &ids).await.unwrap();
        assert_eq!(first.removed, 1);
        assert_eq!(first.not_found, vec!["ghost".to_string()]);

        let second = prune_dead_letters(&dead, &ids).await.unwrap();
        assert_eq!(second.removed, 0, "a second pass removes nothing");
        assert_eq!(
            second.not_found,
            vec!["a".to_string(), "ghost".to_string()],
            "and now reports both as absent"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn pruning_a_torn_file_keeps_the_fragment_and_terminates_it() {
        // A SIGKILL mid-append leaves an unterminated tail. The prune rewrites the file,
        // so it must carry that fragment across as its own line — the same treatment
        // every other scanner already gives it — rather than gluing the next record onto
        // it or dropping it.
        let dir = pdir("prune-torn");
        let dead = dir.join("d.jsonl");
        std::fs::write(
            &dead,
            format!(
                "{}\n{}\n{{\"custom_id\":\"c\",\"resp",
                dead_line("a", "x"),
                dead_line("b", "x")
            ),
        )
        .unwrap();

        let ids: HashSet<String> = ["a".to_string()].into_iter().collect();
        let rep = prune_dead_letters(&dead, &ids).await.unwrap();
        assert_eq!(rep.removed, 1);
        assert_eq!(rep.lines_unreadable, 1, "the fragment counts as unreadable");
        let left = std::fs::read_to_string(&dead).unwrap();
        assert!(
            left.contains("\"custom_id\":\"c\",\"resp"),
            "fragment preserved"
        );
        assert!(
            left.ends_with('\n'),
            "and terminated, so the next append is clean"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn pruning_nothing_leaves_the_file_byte_identical() {
        let dir = pdir("prune-noop");
        let dead = dir.join("d.jsonl");
        let body = format!("{}\n", dead_line("a", "boom"));
        std::fs::write(&dead, &body).unwrap();

        let none: HashSet<String> = HashSet::new();
        assert_eq!(prune_dead_letters(&dead, &none).await.unwrap().removed, 0);
        let miss: HashSet<String> = ["zzz".to_string()].into_iter().collect();
        assert_eq!(prune_dead_letters(&dead, &miss).await.unwrap().removed, 0);
        assert_eq!(std::fs::read_to_string(&dead).unwrap(), body);
        // No temp file left behind either.
        let strays: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(strays.is_empty(), "temp files must not survive");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn pruning_a_missing_dead_file_is_not_an_error() {
        let dir = pdir("prune-absent");
        let ids: HashSet<String> = ["a".to_string()].into_iter().collect();
        let rep = prune_dead_letters(dir.join("nope.jsonl"), &ids)
            .await
            .unwrap();
        assert_eq!(rep.removed, 0);
        assert_eq!(rep.not_found, vec!["a".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    use forge_core::{ItemError, ItemResponse, TokenUsage};
    use serde_json::json;

    fn tmp(name: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("forge-store-{}-{name}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(dead_letter_path(&p));
        p
    }

    fn ok_result(id: &str) -> ItemResult {
        ItemResult {
            custom_id: id.into(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: None,
                body: json!({"choices": []}),
            }),
            error: None,
            usage: TokenUsage {
                prompt_tokens: 3,
                completion_tokens: 1,
                total_tokens: 4,
                ..Default::default()
            },
            worker_id: "w0".into(),
            latency_ms: 5,
            attempt: 1,
            completed_at: 0,
        }
    }

    #[test]
    fn dead_letter_path_is_sibling() {
        let s = JsonlStore::new("results.jsonl");
        assert_eq!(s.out_path(), Path::new("results.jsonl"));
        assert_eq!(s.dead_path(), Path::new("results.jsonl.dead.jsonl"));
    }

    #[tokio::test]
    async fn put_is_idempotent_and_dedups() {
        let p = tmp("idem");
        let s = JsonlStore::new(&p);
        s.put(&ok_result("a")).await.unwrap();
        s.put(&ok_result("a")).await.unwrap(); // duplicate → skipped
        s.put(&ok_result("b")).await.unwrap();

        assert_eq!(s.emitted_ids().await.unwrap().len(), 2);
        let content = std::fs::read_to_string(&p).unwrap();
        assert_eq!(
            content.lines().count(),
            2,
            "duplicate must not append a line"
        );
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn sum_usage_totals_tokens_over_results() {
        let p = tmp("usage");
        let s = JsonlStore::new(&p);
        s.put(&ok_result("a")).await.unwrap(); // usage 3/1
        s.put(&ok_result("b")).await.unwrap(); // usage 3/1
                                               // A torn fragment (no custom_id) must not count.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            writeln!(f, "{{\"oops\": true}}").unwrap();
        }

        let totals = sum_usage(&p).await.unwrap();
        assert_eq!(totals.items, 2);
        assert_eq!(totals.prompt_tokens, 6);
        assert_eq!(totals.completion_tokens, 2);
        assert_eq!(totals.total_tokens(), 8);

        // Missing file → empty totals.
        let empty = sum_usage(std::env::temp_dir().join("forge-nope-xyz.jsonl"))
            .await
            .unwrap();
        assert_eq!(empty, UsageTotals::default());
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn torn_last_line_does_not_corrupt_next_record() {
        // Simulate a SIGKILL mid-write: the file ends with a half-written record and
        // NO trailing newline. A good prior record precedes it.
        let p = tmp("torn");
        let good = serde_json::to_string(&ok_result("a")).unwrap();
        std::fs::write(&p, format!("{good}\n{{\"custom_id\":\"b\",\"resp")).unwrap();

        // On resume, "a" is emitted (dedup), "b"'s torn fragment is unparseable so
        // "b" re-runs and is written cleanly.
        let s = JsonlStore::new(&p);
        assert!(s.emitted_ids().await.unwrap().contains("a"));
        s.put(&ok_result("b")).await.unwrap();

        // Every custom_id has exactly one *parseable* result; the fragment is not
        // glued onto b's clean record.
        let content = std::fs::read_to_string(&p).unwrap();
        let mut ids: Vec<String> = content
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| {
                v.get("custom_id")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn resume_dedups_from_existing_file() {
        let p = tmp("resume");
        // A prior run already emitted "a".
        std::fs::write(
            &p,
            format!("{}\n", serde_json::to_string(&ok_result("a")).unwrap()),
        )
        .unwrap();

        let s = JsonlStore::new(&p);
        s.put(&ok_result("a")).await.unwrap(); // seen in the file → skipped
        s.put(&ok_result("c")).await.unwrap();

        let content = std::fs::read_to_string(&p).unwrap();
        assert_eq!(content.lines().count(), 2); // pre-existing "a" + new "c"
        let ids = s.emitted_ids().await.unwrap();
        assert!(ids.contains("a") && ids.contains("c"));
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn dead_letter_writes_sibling_and_blocks_later_success() {
        let p = tmp("dead");
        let s = JsonlStore::new(&p);
        let mut r = ok_result("x");
        r.response = None;
        r.error = Some(ItemError {
            code: "boom".into(),
            message: "always 500".into(),
        });
        s.dead_letter(&r).await.unwrap();

        // A dead-lettered id is terminal: a later success for the same id is a no-op.
        s.put(&ok_result("x")).await.unwrap();

        assert!(
            !p.exists() || std::fs::read_to_string(&p).unwrap().lines().count() == 0,
            "no success line should be written for a dead-lettered id"
        );
        let dead = std::fs::read_to_string(dead_letter_path(&p)).unwrap();
        assert_eq!(dead.lines().count(), 1);
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(dead_letter_path(&p));
    }
}

#[cfg(test)]
mod usage_read_tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("forge-usageread-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn line(id: &str, p: u64, c: u64) -> String {
        format!(
            r#"{{"custom_id":"{id}","response":{{"status_code":200,"body":{{}}}},"usage":{{"prompt_tokens":{p},"completion_tokens":{c},"total_tokens":{}}},"worker_id":"w","latency_ms":1,"attempt":1,"completed_at":0}}"#,
            p + c
        )
    }

    /// The property the whole watermark rests on: consecutive windows partition the
    /// file's usage exactly, with no record billed twice and none billed never.
    #[tokio::test]
    async fn consecutive_windows_partition_the_file_exactly() {
        let d = dir("partition");
        let f = d.join("results.jsonl");
        let first: String = (0..5)
            .map(|i| line(&format!("a{i}"), 10, 2) + "\n")
            .collect();
        std::fs::write(&f, &first).unwrap();

        let w1 = sum_usage_from(&f, 0).await.unwrap();
        assert_eq!(w1.totals.items, 5);
        assert_eq!(w1.totals.prompt_tokens, 50);
        assert_eq!(w1.next_offset, first.len() as u64);

        // Append more, as a running job does.
        let second: String = (0..3)
            .map(|i| line(&format!("b{i}"), 100, 7) + "\n")
            .collect();
        std::fs::write(&f, format!("{first}{second}")).unwrap();

        let w2 = sum_usage_from(&f, w1.next_offset).await.unwrap();
        assert_eq!(w2.totals.items, 3, "only the NEW records");
        assert_eq!(w2.totals.prompt_tokens, 300);

        // Disjoint, and together exactly the whole file.
        let whole = sum_usage(&f).await.unwrap();
        assert_eq!(w1.totals.items + w2.totals.items, whole.items);
        assert_eq!(
            w1.totals.prompt_tokens + w2.totals.prompt_tokens,
            whole.prompt_tokens
        );
        assert_eq!(
            w1.totals.completion_tokens + w2.totals.completion_tokens,
            whole.completion_tokens
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A window must never close mid-record. If it did, the record completing that
    /// line would be split across two windows and billed by neither.
    #[tokio::test]
    async fn an_unterminated_tail_is_left_for_the_next_read() {
        let d = dir("torn");
        let f = d.join("results.jsonl");
        let complete = line("a", 10, 1) + "\n";
        let partial = r#"{"custom_id":"b","usage":{"prompt_"#; // writer mid-append
        std::fs::write(&f, format!("{complete}{partial}")).unwrap();

        let w = sum_usage_from(&f, 0).await.unwrap();
        assert!(w.ended_mid_line, "the fragment must be recognised");
        assert_eq!(
            w.next_offset,
            complete.len() as u64,
            "must stop after the last COMPLETE line, never at the file length"
        );
        assert_eq!(w.totals.items, 1);

        // The writer finishes the line; the next window bills it exactly once.
        std::fs::write(&f, format!("{complete}{}\n", line("b", 20, 3))).unwrap();
        let w2 = sum_usage_from(&f, w.next_offset).await.unwrap();
        assert_eq!(w2.totals.items, 1);
        assert_eq!(
            w2.totals.prompt_tokens, 20,
            "the completed record, billed once"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A truncated/rotated/restored file must be refused, not read from a plausible
    /// offset in unrelated content.
    #[tokio::test]
    async fn an_offset_past_the_end_is_refused() {
        let d = dir("short");
        let f = d.join("results.jsonl");
        std::fs::write(&f, line("a", 1, 1) + "\n").unwrap();
        let err = sum_usage_from(&f, 10_000).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("truncated, rotated or restored"), "got: {msg}");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// `sum_usage` must stay byte-for-byte what it was — `forge cost` and the meter
    /// path both depend on it, including its treatment of a usage-less line as one
    /// item worth zero tokens.
    #[tokio::test]
    async fn sum_usage_behaviour_is_unchanged_including_usage_less_lines() {
        let d = dir("parity");
        let f = d.join("results.jsonl");
        let body = format!(
            "{}\n{}\n{}\n{}\n",
            line("a", 10, 2),
            r#"{"custom_id":"no-usage","response":{"status_code":200,"body":{}}}"#,
            "{not json at all}",
            line("b", 5, 1),
        );
        std::fs::write(&f, body).unwrap();

        let totals = sum_usage(&f).await.unwrap();
        // Three result lines (the unparseable one is not one), the usage-less line
        // counting as an item worth zero.
        assert_eq!(totals.items, 3);
        assert_eq!(totals.prompt_tokens, 15);
        assert_eq!(totals.completion_tokens, 3);

        let read = sum_usage_from(&f, 0).await.unwrap();
        assert_eq!(read.totals, totals, "the two must not diverge");
        // And the read STATES what the fold silently absorbed.
        assert_eq!(read.items_without_usage, 1);
        assert_eq!(read.lines_unreadable, 1);
        assert_eq!(read.lines_read, 4);
        let _ = std::fs::remove_dir_all(&d);
    }
}
