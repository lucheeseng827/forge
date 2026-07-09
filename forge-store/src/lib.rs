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
pub use object_store_backend::{objstore_from_out, objstore_open_run, ObjStore};

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
    let mut totals = UsageTotals::default();
    let file = match File::open(path.as_ref()).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(totals),
        Err(e) => return Err(e.into()),
    };
    let mut lines = BufReader::new(file).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if v.get("custom_id").is_none() {
            continue; // not a result line (e.g. a crash-torn fragment)
        }
        let usage = v
            .get("usage")
            .and_then(|u| serde_json::from_value::<TokenUsage>(u.clone()).ok())
            .unwrap_or_default();
        totals.add(&usage);
    }
    Ok(totals)
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
