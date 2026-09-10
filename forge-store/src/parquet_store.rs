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

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
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
    forge_schema_with_embedding(None)
}

/// The schema, optionally carrying a typed `embedding` column.
///
/// Embeddings-at-scale is the marquee batch workload and the reason Parquet output
/// exists at all — but until now the vector landed inside `body_json` as text, so
/// loading it into a warehouse or a vector DB meant re-parsing JSON per row and
/// re-typing the array. A `FixedSizeList<Float32, D>` column is directly loadable and
/// is the layout vector stores expect.
///
/// `dim` is `None` for non-embedding runs, which produces exactly the previous schema
/// — a chat run's output is byte-compatible with what it was before.
pub fn forge_schema_with_embedding(dim: Option<i32>) -> SchemaRef {
    let mut fields = vec![
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
    ];
    if let Some(d) = dim {
        // Nullable: a dead-lettered item in an embeddings run has no vector, and a
        // null is the honest representation of that — not a zero vector, which would
        // silently poison a nearest-neighbour index.
        fields.push(Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), d),
            true,
        ));
    }
    Arc::new(Schema::new(fields))
}

/// Pull the embedding vector out of an OpenAI-compatible `/v1/embeddings` response
/// body: `{"data":[{"embedding":[...]}]}`. `None` for any other shape, which is how a
/// chat run and a dead-lettered item both end up without one.
pub(crate) fn extract_embedding(body: &serde_json::Value) -> Option<Vec<f32>> {
    let arr = body
        .get("data")?
        .as_array()?
        .first()?
        .get("embedding")?
        .as_array()?;
    arr.iter()
        .map(|v| v.as_f64().map(|f| f as f32))
        .collect::<Option<Vec<f32>>>()
        .filter(|v| !v.is_empty())
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
    /// One optional vector per row; `None` for a row with no embedding (a dead-letter,
    /// or a non-embedding response). Empty when the run has no embedding column.
    embedding: Vec<Option<Vec<f32>>>,
}

impl RowBuffer {
    /// Append one result. `dim` is the run's embedding width, or `None` when the
    /// schema has no embedding column — passing it here (rather than inferring per
    /// row) is what keeps every column the same length as `custom_id`.
    fn push(&mut self, r: &ItemResult, dim: Option<i32>) {
        if dim.is_some() {
            self.embedding.push(
                r.response
                    .as_ref()
                    .and_then(|resp| extract_embedding(&resp.body)),
            );
        }
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
        let mut columns: Vec<ArrayRef> = vec![
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
        // The embedding column exists iff the schema declares it, and its width is
        // whatever the schema says — a row whose vector disagrees is a hard error
        // rather than a silently truncated or zero-padded vector, which would corrupt
        // a nearest-neighbour index in a way nobody would notice until query time.
        if let Some(field) = schema.fields().iter().find(|f| f.name() == "embedding") {
            let DataType::FixedSizeList(_, dim) = field.data_type() else {
                return Err(ForgeError::Store(
                    "embedding column is not a FixedSizeList".into(),
                ));
            };
            let dim = *dim;
            // `with_field` so the builder's inner field matches the schema's exactly.
            // The default builder marks the inner values nullable; the schema says a
            // value inside a vector is never null, which is the accurate statement —
            // and RecordBatch::try_new compares the two structurally, so they have to
            // agree rather than one being quietly relaxed.
            let inner = match field.data_type() {
                DataType::FixedSizeList(f, _) => Arc::clone(f),
                _ => unreachable!("matched FixedSizeList above"),
            };
            let mut b = FixedSizeListBuilder::new(Float32Builder::new(), dim).with_field(inner);
            for v in std::mem::take(&mut self.embedding) {
                match v {
                    Some(vec) => {
                        if vec.len() as i32 != dim {
                            return Err(ForgeError::Store(format!(
                                "embedding width {} does not match the run's dimension {dim} \
                                 — a results file mixing embedding models cannot be written \
                                 as one typed column",
                                vec.len()
                            )));
                        }
                        b.values().append_slice(&vec);
                        b.append(true);
                    }
                    None => {
                        // No vector for this row (a dead-letter, say). Append the
                        // required number of placeholder values, then mark the LIST
                        // null so readers see null rather than zeros.
                        b.values().append_slice(&vec![0.0; dim as usize]);
                        b.append(false);
                    }
                }
            }
            columns.push(Arc::new(b.finish()) as ArrayRef);
        }
        RecordBatch::try_new(schema.clone(), columns).map_err(pe)
    }
}

/// The declared embedding width of a schema, or `None` when it carries no vector column.
fn embedding_dim(schema: &SchemaRef) -> Option<i32> {
    schema.fields().iter().find_map(|f| match f.data_type() {
        DataType::FixedSizeList(_, d) if f.name() == "embedding" => Some(*d),
        _ => None,
    })
}

fn writer_props(opts: &ParquetOpts) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(opts.compression)
        .set_max_row_group_size(opts.row_group_rows.max(1))
        .build()
}

/// Read the embedding width from the first result line that carries one.
///
/// Bounded: it stops at the first vector, which for an embeddings run is line 1. A
/// results file with no embeddings anywhere is read once and yields `None`, which
/// reproduces the previous schema exactly.
async fn first_embedding_dim(input: &Path) -> Result<Option<i32>, ForgeError> {
    // Bounded on BOTH sides. Stopping at the first vector makes an embeddings run cost
    // one line, but a chat export has no vector anywhere — so this used to read to EOF
    // and `jsonl_to_parquet` then reopened and read the same file again. That is a full
    // second scan on the common case, which the old comment described as "read once"
    // (true of this function, not of what the export pays).
    const PEEK_LINES: usize = 64;
    let mut seen = 0usize;
    let mut lines = tokio::io::BufReader::new(tokio::fs::File::open(input).await?).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        seen += 1;
        if seen > PEEK_LINES {
            return Ok(None);
        }
        let Ok(r) = serde_json::from_str::<ItemResult>(&line) else {
            continue;
        };
        if let Some(v) = r.response.as_ref().and_then(|x| extract_embedding(&x.body)) {
            return Ok(Some(v.len() as i32));
        }
    }
    Ok(None)
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
    // The vector width is a property of the run, so it is read from the first result
    // that carries one — a single-line peek, not a pre-pass over the file, which would
    // cost a second full read at 50M rows. A run mixing embedding models is rejected
    // later by `drain_to_batch` rather than silently truncated.
    let dim = first_embedding_dim(input).await?;
    let schema = forge_schema_with_embedding(dim);
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
        buf.push(&r, dim);
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
    /// Fixed on the FIRST result, not at construction: whether the run produces
    /// embeddings — and at what width — is only knowable once a response has been
    /// seen. The writer is already created lazily for the same reason, so this costs
    /// no extra bookkeeping.
    schema: Option<SchemaRef>,
}

/// A **write-once** Parquet [`ResultStore`]. Buffers a row per terminal result and
/// flushes row groups by `row_group_rows`; [`finalize`](ParquetStore::finalize) writes
/// the footer. Dedup is **in-process only** — Parquet can't be reopened/appended, so
/// this is not a resumable checkpoint (use `JsonlStore` + [`jsonl_to_parquet`] for
/// that). One file; dead-letters are columnar rows with null `status_code`/`body_json`
/// and populated `error_*`.
pub struct ParquetStore {
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
            opts,
            out_path: out_path.as_ref().to_path_buf(),
            inner: Mutex::new(ParquetInner {
                emitted: HashSet::new(),
                writer: None,
                buf: RowBuffer::default(),
                finalized: false,
                schema: None,
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
        if inner.schema.is_none() {
            // First result decides the shape: an embeddings response gives the run a
            // typed vector column of that width; anything else keeps the previous
            // schema exactly.
            let dim = r
                .response
                .as_ref()
                .and_then(|resp| extract_embedding(&resp.body))
                .map(|v| v.len() as i32);
            inner.schema = Some(forge_schema_with_embedding(dim));
        }
        let schema = inner.schema.clone().expect("schema fixed above");
        let dim = embedding_dim(&schema);
        if inner.writer.is_none() {
            let file = std::fs::File::create(&self.out_path)?;
            inner.writer = Some(
                ArrowWriter::try_new(file, schema.clone(), Some(writer_props(&self.opts)))
                    .map_err(pe)?,
            );
        }
        inner.buf.push(r, dim);
        if inner.buf.len() >= self.opts.row_group_rows {
            let batch = inner.buf.drain_to_batch(&schema)?;
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
            let schema = inner
                .schema
                .clone()
                .expect("buffered rows imply a fixed schema");
            let batch = inner.buf.drain_to_batch(&schema)?;
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
    fn embedding_result(id: &str, dim: usize) -> ItemResult {
        ItemResult {
            custom_id: id.into(),
            response: Some(ItemResponse {
                status_code: 200,
                request_id: None,
                body: serde_json::json!({
                    "object": "list",
                    "data": [{"object": "embedding", "index": 0,
                              "embedding": (0..dim).map(|i| i as f64 / 10.0).collect::<Vec<_>>()}],
                    "model": "bge-m3",
                }),
            }),
            error: None,
            usage: TokenUsage::default(),
            worker_id: "w0".into(),
            latency_ms: 1,
            attempt: 1,
            completed_at: 0,
        }
    }

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("forge-emb-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn an_embeddings_run_gets_a_typed_vector_column() {
        let d = tmpdir("typed");
        let jsonl = d.join("res.jsonl");
        let body: String = (0..3)
            .map(|i| serde_json::to_string(&embedding_result(&format!("e{i}"), 4)).unwrap() + "\n")
            .collect();
        std::fs::write(&jsonl, body).unwrap();

        let out = d.join("out.parquet");
        let rows = jsonl_to_parquet(&jsonl, &out, ParquetOpts::default())
            .await
            .unwrap();
        assert_eq!(rows, 3);

        // Read it back and assert the column is a real FixedSizeList<Float32, 4> —
        // the point of the feature is that a loader does not have to re-parse JSON.
        let f = std::fs::File::open(&out).unwrap();
        let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        let field = b
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == "embedding")
            .expect("an embeddings run must produce an embedding column")
            .clone();
        match field.data_type() {
            DataType::FixedSizeList(inner, dim) => {
                assert_eq!(*dim, 4, "width comes from the data");
                assert_eq!(inner.data_type(), &DataType::Float32);
            }
            other => panic!("expected FixedSizeList<Float32,4>, got {other:?}"),
        }

        let mut reader = b.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col = batch
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .expect("typed vector column");
        assert_eq!(col.len(), 3);
        assert!(col.is_valid(0));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn a_chat_run_is_unchanged_and_a_dead_letter_is_null_not_zeros() {
        let d = tmpdir("mixed");

        // No embeddings anywhere → exactly the previous schema, no new column.
        let chat = d.join("chat.jsonl");
        std::fs::write(
            &chat,
            serde_json::to_string(&ok_result("c1")).unwrap() + "\n",
        )
        .unwrap();
        let chat_out = d.join("chat.parquet");
        jsonl_to_parquet(&chat, &chat_out, ParquetOpts::default())
            .await
            .unwrap();
        let f = std::fs::File::open(&chat_out).unwrap();
        let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
        assert!(
            !b.schema().fields().iter().any(|f| f.name() == "embedding"),
            "a non-embedding run must keep the original schema"
        );

        // An embeddings run containing a dead-letter: the failed row must read back
        // NULL, never a zero vector — zeros would silently poison a similarity index.
        let mixed = d.join("mixed.jsonl");
        let mut body = serde_json::to_string(&embedding_result("e0", 3)).unwrap() + "\n";
        let mut dead = ok_result("d1");
        dead.response = None;
        dead.error = Some(forge_core::ItemError {
            code: "worker_error".into(),
            message: "boom".into(),
        });
        body.push_str(&(serde_json::to_string(&dead).unwrap() + "\n"));
        std::fs::write(&mixed, body).unwrap();

        let mixed_out = d.join("mixed.parquet");
        jsonl_to_parquet(&mixed, &mixed_out, ParquetOpts::default())
            .await
            .unwrap();
        let f = std::fs::File::open(&mixed_out).unwrap();
        let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col = batch
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap();
        assert!(col.is_valid(0), "the embedding row has a vector");
        assert!(col.is_null(1), "the dead-letter must be NULL, not zeros");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[tokio::test]
    async fn mixing_embedding_widths_is_an_error_not_a_silent_truncation() {
        // A results file mixing two embedding models cannot become one typed column.
        // Truncating or padding would corrupt a vector index invisibly.
        let d = tmpdir("mixed-dims");
        let jsonl = d.join("res.jsonl");
        let body = serde_json::to_string(&embedding_result("a", 4)).unwrap()
            + "\n"
            + &serde_json::to_string(&embedding_result("b", 8)).unwrap()
            + "\n";
        std::fs::write(&jsonl, body).unwrap();
        let err = jsonl_to_parquet(&jsonl, &d.join("o.parquet"), ParquetOpts::default())
            .await
            .expect_err("mixed widths must not be written");
        assert!(
            err.to_string().contains("does not match"),
            "unhelpful error: {err}"
        );
        let _ = std::fs::remove_dir_all(&d);
    }
}
