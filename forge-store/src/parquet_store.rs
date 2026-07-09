//! Parquet columnar result output (feature `parquet`).
//!
//! For embedding / large-N runs, a row-per-`custom_id` **columnar** file is far
//! cheaper to store and scan than the JSONL path. Two entry points, both built on
//! `parquet::arrow::ArrowWriter` over one fixed [`forge_schema`]:
//!
//! - [`jsonl_to_parquet`] — the **exporter** (the MVP): stream a results JSONL and
//!   write Parquet, flushing a row group every `row_group_rows`, so a 50M-row file
//!   never materializes more than one row group in RAM. This is the doctrine-clean
//!   path: run normally (resumable JSONL / object store), then export once at the end.
//! - [`ParquetStore`] — a **write-once** `ResultStore` for an embedder that wants
//!   columnar output directly from a single run. Parquet has no append/footer-rewrite,
//!   so it is **not** a resumable checkpoint (its dedup is in-process only); a
//!   resumable run should use `JsonlStore` and `jsonl_to_parquet`. `finalize()` **must**
//!   be called to write the footers, or the file is unreadable.
//!
//! Off by default: the arrow/parquet stack (~30 crates) compiles only with the
//! `parquet` feature, never into the lean `forge` musl binary. Snappy (`snap`, pure
//! Rust) is the codec, so even a feature-on build stays musl-static (no zstd/brotli C).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use forge_core::{ForgeError, ItemResult, ResultStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::io::AsyncBufReadExt;
use tokio::sync::Mutex;

fn pe(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Store(e.to_string())
}

/// Best-effort "same file" guard (identical spelling, or same canonical target when
/// both exist), so `jsonl_to_parquet` never truncates its own input.
fn same_file(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    matches!(
        (a.canonicalize(), b.canonicalize()),
        (Ok(ca), Ok(cb)) if ca == cb
    )
}

/// The fixed, flat columnar schema — one row per `custom_id`. Nullable columns carry
/// the success xor dead-letter split (`status_code`/`body_json` null on a dead-letter;
/// `error_*` null on a success).
pub fn forge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("custom_id", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, true),
        Field::new("body_json", DataType::Utf8, true),
        Field::new("error_code", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("prompt_tokens", DataType::Int64, false),
        Field::new("completion_tokens", DataType::Int64, false),
        Field::new("total_tokens", DataType::Int64, false),
        Field::new("worker_id", DataType::Utf8, false),
        Field::new("latency_ms", DataType::Int64, false),
        Field::new("attempt", DataType::Int64, false),
        Field::new("completed_at", DataType::Int64, false),
    ]))
}

/// Tunables for Parquet writing.
#[derive(Debug, Clone)]
pub struct ParquetOpts {
    /// Rows buffered before a row group is cut/flushed (bounds RAM at scale).
    pub row_group_rows: usize,
    /// Column compression. Default Snappy (pure-Rust `snap`).
    pub compression: Compression,
}

impl Default for ParquetOpts {
    fn default() -> Self {
        Self {
            row_group_rows: 50_000,
            compression: Compression::SNAPPY,
        }
    }
}

/// Column-oriented row accumulator: each result appends one element to every column
/// vector; [`drain_to_batch`](RowBuffer::drain_to_batch) turns the lot into one
/// [`RecordBatch`] and empties the buffers.
#[derive(Default)]
struct RowBuffer {
    custom_id: Vec<String>,
    status_code: Vec<Option<i32>>,
    body_json: Vec<Option<String>>,
    error_code: Vec<Option<String>>,
    error_message: Vec<Option<String>>,
    prompt_tokens: Vec<i64>,
    completion_tokens: Vec<i64>,
    total_tokens: Vec<i64>,
    worker_id: Vec<String>,
    latency_ms: Vec<i64>,
    attempt: Vec<i64>,
    completed_at: Vec<i64>,
}

impl RowBuffer {
    fn push(&mut self, r: &ItemResult) {
        self.custom_id.push(r.custom_id.clone());
        self.status_code
            .push(r.response.as_ref().map(|x| x.status_code as i32));
        self.body_json
            .push(r.response.as_ref().map(|x| x.body.to_string()));
        self.error_code
            .push(r.error.as_ref().map(|e| e.code.clone()));
        self.error_message
            .push(r.error.as_ref().map(|e| e.message.clone()));
        self.prompt_tokens.push(r.usage.prompt_tokens as i64);
        self.completion_tokens
            .push(r.usage.completion_tokens as i64);
        self.total_tokens.push(r.usage.total_tokens as i64);
        self.worker_id.push(r.worker_id.clone());
        self.latency_ms.push(r.latency_ms as i64);
        self.attempt.push(r.attempt as i64);
        self.completed_at.push(r.completed_at);
    }

    fn len(&self) -> usize {
        self.custom_id.len()
    }

    fn drain_to_batch(&mut self, schema: &SchemaRef) -> Result<RecordBatch, ForgeError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter(
                std::mem::take(&mut self.custom_id).into_iter().map(Some),
            )),
            Arc::new(Int32Array::from(std::mem::take(&mut self.status_code))),
            Arc::new(StringArray::from_iter(std::mem::take(&mut self.body_json))),
            Arc::new(StringArray::from_iter(std::mem::take(&mut self.error_code))),
            Arc::new(StringArray::from_iter(std::mem::take(
                &mut self.error_message,
            ))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.prompt_tokens))),
            Arc::new(Int64Array::from(std::mem::take(
                &mut self.completion_tokens,
            ))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.total_tokens))),
            Arc::new(StringArray::from_iter(
                std::mem::take(&mut self.worker_id).into_iter().map(Some),
            )),
            Arc::new(Int64Array::from(std::mem::take(&mut self.latency_ms))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.attempt))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.completed_at))),
        ];
        RecordBatch::try_new(schema.clone(), columns).map_err(pe)
    }
}

fn writer_props(opts: &ParquetOpts) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(opts.compression)
        .set_max_row_group_size(opts.row_group_rows.max(1))
        .build()
}

/// Stream a results JSONL into Parquet, flushing a row group every
/// `opts.row_group_rows` rows (never holds the whole file in RAM). Torn/unparseable
/// lines are skipped (like `sum_usage`). Returns the number of rows written.
pub async fn jsonl_to_parquet(
    input: impl AsRef<Path>,
    out: impl AsRef<Path>,
    opts: ParquetOpts,
) -> Result<u64, ForgeError> {
    let input: &Path = input.as_ref();
    let out: &Path = out.as_ref();
    // `File::create(out)` truncates before the read loop starts, so exporting a file
    // onto itself would wipe the source before a single row is read.
    if same_file(input, out) {
        return Err(ForgeError::Store(format!(
            "refusing to export {} onto itself (input and out are the same file)",
            input.display()
        )));
    }
    let schema = forge_schema();
    let file = std::fs::File::create(out)?;
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(writer_props(&opts))).map_err(pe)?;

    let mut buf = RowBuffer::default();
    let mut rows = 0u64;
    let mut lines = tokio::io::BufReader::new(tokio::fs::File::open(input).await?).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(r) = serde_json::from_str::<ItemResult>(&line) else {
            continue; // torn crash fragment — skip, never abort
        };
        buf.push(&r);
        rows += 1;
        if buf.len() >= opts.row_group_rows {
            let batch = buf.drain_to_batch(&schema)?;
            writer.write(&batch).map_err(pe)?;
        }
    }
    if buf.len() > 0 {
        let batch = buf.drain_to_batch(&schema)?;
        writer.write(&batch).map_err(pe)?;
    }
    writer.close().map_err(pe)?; // writes the footer — required for a readable file
    Ok(rows)
}

struct ParquetInner {
    emitted: HashSet<String>,
    writer: Option<ArrowWriter<std::fs::File>>,
    buf: RowBuffer,
    finalized: bool,
}

/// A **write-once** Parquet [`ResultStore`]. Buffers a row per terminal result and
/// flushes row groups by `row_group_rows`; [`finalize`](ParquetStore::finalize) writes
/// the footer. Dedup is **in-process only** — Parquet can't be reopened/appended, so
/// this is not a resumable checkpoint (use `JsonlStore` + [`jsonl_to_parquet`] for
/// that). One file; dead-letters are columnar rows with null `status_code`/`body_json`
/// and populated `error_*`.
pub struct ParquetStore {
    schema: SchemaRef,
    opts: ParquetOpts,
    out_path: PathBuf,
    inner: Mutex<ParquetInner>,
}

impl ParquetStore {
    pub fn new(out_path: impl AsRef<Path>) -> Self {
        Self::with_opts(out_path, ParquetOpts::default())
    }

    pub fn with_opts(out_path: impl AsRef<Path>, opts: ParquetOpts) -> Self {
        Self {
            schema: forge_schema(),
            opts,
            out_path: out_path.as_ref().to_path_buf(),
            inner: Mutex::new(ParquetInner {
                emitted: HashSet::new(),
                writer: None,
                buf: RowBuffer::default(),
                finalized: false,
            }),
        }
    }

    async fn append(&self, r: &ItemResult) -> Result<(), ForgeError> {
        let mut inner = self.inner.lock().await;
        if inner.finalized {
            return Err(ForgeError::Store("ParquetStore already finalized".into()));
        }
        if !inner.emitted.insert(r.custom_id.clone()) {
            return Ok(()); // in-process dedup — terminal once
        }
        if inner.writer.is_none() {
            let file = std::fs::File::create(&self.out_path)?;
            inner.writer = Some(
                ArrowWriter::try_new(file, self.schema.clone(), Some(writer_props(&self.opts)))
                    .map_err(pe)?,
            );
        }
        inner.buf.push(r);
        if inner.buf.len() >= self.opts.row_group_rows {
            let batch = inner.buf.drain_to_batch(&self.schema)?;
            inner.writer.as_mut().unwrap().write(&batch).map_err(pe)?;
        }
        Ok(())
    }

    /// Flush the trailing rows and write the Parquet footer. **Must** be called or the
    /// file has no footer and is unreadable. Idempotent.
    pub async fn finalize(&self) -> Result<(), ForgeError> {
        let mut inner = self.inner.lock().await;
        if inner.finalized {
            return Ok(());
        }
        inner.finalized = true;
        if inner.buf.len() > 0 {
            let batch = inner.buf.drain_to_batch(&self.schema)?;
            inner.writer.as_mut().unwrap().write(&batch).map_err(pe)?;
        }
        if let Some(writer) = inner.writer.take() {
            writer.close().map_err(pe)?;
        }
        Ok(())
    }
}

impl ResultStore for ParquetStore {
    async fn put(&self, result: &ItemResult) -> Result<(), ForgeError> {
        self.append(result).await
    }

    async fn dead_letter(&self, result: &ItemResult) -> Result<(), ForgeError> {
        self.append(result).await
    }

    async fn emitted_ids(&self) -> Result<HashSet<String>, ForgeError> {
        // In-process only (write-once sink); a fresh store starts empty by design.
        Ok(self.inner.lock().await.emitted.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use forge_core::{ItemError, ItemResponse, TokenUsage};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn ok_result(id: &str) -> ItemResult {
        ItemResult {
            custom_id: id.into(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: None,
                body: serde_json::json!({"choices": [{"message": {"content": "ok"}}]}),
            }),
            error: None,
            usage: TokenUsage {
                prompt_tokens: 3,
                completion_tokens: 4,
                total_tokens: 7,
                ..Default::default()
            },
            worker_id: "gpu1".into(),
            latency_ms: 5,
            attempt: 1,
            completed_at: 42,
        }
    }

    fn dead_result(id: &str) -> ItemResult {
        let mut r = ok_result(id);
        r.response = None;
        r.error = Some(ItemError {
            code: "worker_error".into(),
            message: "boom".into(),
        });
        r
    }

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("forge-store-pq-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn read_all(path: &Path) -> Vec<RecordBatch> {
        let file = std::fs::File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.map(|b| b.unwrap()).collect()
    }

    fn col_str<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
        let i = b.schema().index_of(name).unwrap();
        b.column(i).as_any().downcast_ref::<StringArray>().unwrap()
    }

    #[tokio::test]
    async fn exporter_roundtrips_rows_and_columns() {
        let dir = tmp("export");
        let input = dir.join("results.jsonl");
        let out = dir.join("results.parquet");
        let lines: Vec<String> = ["a", "b", "c"]
            .iter()
            .map(|id| serde_json::to_string(&ok_result(id)).unwrap())
            .collect();
        std::fs::write(&input, lines.join("\n") + "\n").unwrap();

        let rows = jsonl_to_parquet(&input, &out, ParquetOpts::default())
            .await
            .unwrap();
        assert_eq!(rows, 3);

        let batches = read_all(&out);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        let ids: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let c = col_str(b, "custom_id");
                (0..c.len())
                    .map(|i| c.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        // token columns survived.
        let tot = {
            let b = &batches[0];
            let i = b.schema().index_of("total_tokens").unwrap();
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(tot, 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn dead_letter_row_has_null_status_and_populated_error() {
        let dir = tmp("dead");
        let out = dir.join("o.parquet");
        let s = ParquetStore::new(&out);
        s.dead_letter(&dead_result("x")).await.unwrap();
        s.finalize().await.unwrap();

        let batches = read_all(&out);
        let b = &batches[0];
        let sc = b.column(b.schema().index_of("status_code").unwrap());
        assert!(sc.is_null(0), "status_code null on a dead-letter");
        let body = col_str(b, "body_json");
        assert!(body.is_null(0), "body_json null on a dead-letter");
        assert_eq!(col_str(b, "error_code").value(0), "worker_error");
        assert_eq!(col_str(b, "error_message").value(0), "boom");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn store_put_is_idempotent_within_process() {
        let dir = tmp("idem");
        let out = dir.join("o.parquet");
        let s = ParquetStore::new(&out);
        s.put(&ok_result("a")).await.unwrap();
        s.put(&ok_result("a")).await.unwrap(); // dup — no-op
        s.put(&ok_result("b")).await.unwrap();
        s.finalize().await.unwrap();

        let total: usize = read_all(&out).iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn bounded_memory_flushes_row_groups() {
        let dir = tmp("rowgroups");
        let input = dir.join("r.jsonl");
        let out = dir.join("r.parquet");
        let lines: Vec<String> = (0..5)
            .map(|i| serde_json::to_string(&ok_result(&format!("id{i}"))).unwrap())
            .collect();
        std::fs::write(&input, lines.join("\n") + "\n").unwrap();

        let opts = ParquetOpts {
            row_group_rows: 2,
            ..Default::default()
        };
        jsonl_to_parquet(&input, &out, opts).await.unwrap();

        let file = std::fs::File::open(&out).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        // 5 rows, 2 per group → 3 row groups (2+2+1): proof of streaming flush.
        assert_eq!(builder.metadata().num_row_groups(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
