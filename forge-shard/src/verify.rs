//! verify — bounded-memory **completeness** check.
//!
//! `forge verify` confirms that every input `custom_id` has a *terminal* entry — a
//! result line **or** a dead-letter line — and reports `missing` / `extra` /
//! `duplicate` counts, optionally spilling the missing ids to a sidecar. The whole
//! point is that it does this for a **50M-line** job **without holding all ids in
//! RAM**: it is an **exact external sort-merge set-difference**, not a Bloom filter.
//!
//! Why exact, not Bloom: a false-positive Bloom hit would mark a genuinely-missing id
//! as present — a *silent completeness lie*. (The ROADMAP's "is a Bloom filter
//! enough?" question is about the *resume* emitted-id skip path, which tolerates a
//! confirming read; a correctness verifier cannot.) So ids stream through bounded
//! in-RAM run buffers that sort + spill to temp files, then a streaming k-way merge
//! walks the two sorted id streams in a single linear pass. RAM is `O(run_capacity)`,
//! independent of the 50M.
//!
//! **Report-only, by doctrine.** `verify` never re-queues the gaps it finds — that is
//! the existing `forge sweep` / `Queue::reap` job. Crossing input ids with results to
//! *find and re-run* gaps would tempt a fan-in/join shape; `verify` stays strictly:
//! read input ids, read emitted ids, diff, report. Any re-run is a separate explicit
//! `forge run` / `resume` / `sweep` the operator invokes. It opens files read-only and
//! never touches the queue.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use forge_core::ForgeError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines};

use crate::valid_custom_id;

/// Default ids buffered in RAM before a sorted run is spilled to a temp file. ~1M
/// short ids ≈ a few tens of MB; at 50M that's ~50 runs (a ~50-entry merge heap).
const DEFAULT_RUN_CAPACITY: usize = 1_000_000;

/// Bound on how many missing ids the report keeps in memory as a sample (the full
/// list goes to the sidecar when one is configured).
const MISSING_SAMPLE_CAP: usize = 100;

/// Process-unique sequence so concurrent verifies (e.g. parallel tests) get distinct
/// temp dirs without `Math.random`.
static VERIFY_SEQ: AtomicU64 = AtomicU64::new(0);

/// Knobs for a completeness sweep.
#[derive(Debug, Clone)]
pub struct VerifyConfig {
    /// Ids per in-RAM run buffer before spilling. Smaller = less RAM, more temp runs.
    pub run_capacity: usize,
    /// Optional sidecar that receives every missing `custom_id` (one per line),
    /// lazily created only if at least one id is missing.
    pub missing_out: Option<PathBuf>,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        Self {
            run_capacity: DEFAULT_RUN_CAPACITY,
            missing_out: None,
        }
    }
}

/// The completeness tally. `missing == 0` ⇒ every input id has a terminal result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// Distinct input `custom_id`s.
    pub input_ids: u64,
    /// Distinct emitted ids (results file + dead-letter sibling).
    pub emitted_ids: u64,
    /// Input ids with **no** terminal result — the completeness gap.
    pub missing: u64,
    /// Emitted ids **not** present in the input (e.g. wrong file pair / stray result).
    pub extra: u64,
    /// Input ids that appeared more than once (count of the surplus occurrences).
    pub duplicate_input: u64,
    /// Input lines skipped because their `custom_id` was absent/unparseable/invalid.
    pub rejected_input: u64,
    /// A bounded sample of missing ids (first [`MISSING_SAMPLE_CAP`]); the full set,
    /// if requested, is in the sidecar.
    pub missing_sample: Vec<String>,
    /// External-sort runs spilled across both streams (0 ⇒ everything fit one buffer).
    /// Exposed so a caller/test can confirm the bounded-memory path actually engaged.
    pub runs_spilled: usize,
}

impl VerifyReport {
    /// True iff nothing is missing — the run is provably complete.
    pub fn is_complete(&self) -> bool {
        self.missing == 0
    }
}

/// Extract the `custom_id` (Bedrock `recordId` accepted) from one JSONL line.
/// Returns `None` for a torn/unparseable line or one lacking the field — those are
/// skipped, never counted as content.
fn id_of_line(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    v.get("custom_id")
        .or_else(|| v.get("recordId"))
        .and_then(|x| x.as_str())
        .map(str::to_string)
}

/// A bounded buffer that sorts and spills sorted run files — the external-sort
/// front half. RAM stays `O(capacity)`; `runs` is the list of sorted spill files.
struct RunSpiller {
    dir: PathBuf,
    prefix: &'static str,
    capacity: usize,
    buf: Vec<String>,
    runs: Vec<PathBuf>,
}

impl RunSpiller {
    fn new(dir: PathBuf, prefix: &'static str, capacity: usize) -> Self {
        Self {
            dir,
            prefix,
            capacity: capacity.max(1),
            buf: Vec::new(),
            runs: Vec::new(),
        }
    }

    async fn push(&mut self, id: String) -> Result<(), ForgeError> {
        self.buf.push(id);
        if self.buf.len() >= self.capacity {
            self.spill().await?;
        }
        Ok(())
    }

    async fn spill(&mut self) -> Result<(), ForgeError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.buf.sort_unstable();
        let path = self
            .dir
            .join(format!("{}-{}.run", self.prefix, self.runs.len()));
        let mut w = BufWriter::new(File::create(&path).await?);
        for id in &self.buf {
            w.write_all(id.as_bytes()).await?;
            w.write_all(b"\n").await?;
        }
        w.flush().await?;
        self.runs.push(path);
        self.buf.clear();
        Ok(())
    }

    /// Flush the tail and return the sorted run files.
    async fn finish(mut self) -> Result<Vec<PathBuf>, ForgeError> {
        self.spill().await?;
        Ok(self.runs)
    }
}

/// Streaming k-way merge over sorted run files → a globally sorted id stream (with
/// duplicates, which are adjacent because the inputs are sorted).
struct KwayMerge {
    readers: Vec<Lines<BufReader<File>>>,
    heap: BinaryHeap<Reverse<(String, usize)>>,
}

impl KwayMerge {
    async fn open(runs: &[PathBuf]) -> Result<Self, ForgeError> {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        for (i, p) in runs.iter().enumerate() {
            let mut lines = BufReader::new(File::open(p).await?).lines();
            if let Some(first) = lines.next_line().await? {
                heap.push(Reverse((first, i)));
            }
            readers.push(lines);
        }
        Ok(Self { readers, heap })
    }

    /// The next id in sorted order (duplicates included), or `None` at the end.
    async fn next_raw(&mut self) -> Result<Option<String>, ForgeError> {
        let Some(Reverse((id, i))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(next) = self.readers[i].next_line().await? {
            self.heap.push(Reverse((next, i)));
        }
        Ok(Some(id))
    }
}

/// A deduping view over a [`KwayMerge`]: yields each distinct id once (in sorted
/// order) while tallying total vs unique (the duplicate count falls out).
struct UniqueStream {
    merge: KwayMerge,
    held: Option<String>,
    total: u64,
    unique: u64,
}

impl UniqueStream {
    async fn open(runs: &[PathBuf]) -> Result<Self, ForgeError> {
        Ok(Self {
            merge: KwayMerge::open(runs).await?,
            held: None,
            total: 0,
            unique: 0,
        })
    }

    /// The next distinct id, collapsing (and counting) adjacent duplicates.
    async fn next_unique(&mut self) -> Result<Option<String>, ForgeError> {
        let current = match self.held.take() {
            Some(x) => x,
            None => match self.merge.next_raw().await? {
                Some(x) => x,
                None => return Ok(None),
            },
        };
        self.total += 1;
        loop {
            match self.merge.next_raw().await? {
                Some(next) if next == current => self.total += 1, // a duplicate
                Some(next) => {
                    self.held = Some(next); // first id of the next group
                    break;
                }
                None => break, // end of stream
            }
        }
        self.unique += 1;
        Ok(Some(current))
    }
}

/// A lazily-opened sidecar of missing ids (one per line) — created only if at least
/// one id is missing, so a clean verify leaves nothing behind.
struct MissingSink {
    path: PathBuf,
    file: Option<File>,
}

impl MissingSink {
    fn new(path: PathBuf) -> Self {
        Self { path, file: None }
    }

    async fn write(&mut self, id: &str) -> Result<(), ForgeError> {
        if self.file.is_none() {
            if let Some(parent) = self.path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&self.path)
                    .await?,
            );
        }
        let f = self.file.as_mut().unwrap();
        f.write_all(id.as_bytes()).await?;
        f.write_all(b"\n").await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ForgeError> {
        if let Some(f) = self.file.as_mut() {
            f.flush().await?;
        }
        Ok(())
    }
}

/// Stream one file's ids into `spiller`. `validate` true ⇒ count + skip ids that
/// fail [`valid_custom_id`] (the input pass); false ⇒ silently skip unparseable
/// lines (the emitted pass, where torn crash fragments are expected). Returns the
/// number of rejected lines.
async fn spill_ids(
    path: &Path,
    spiller: &mut RunSpiller,
    validate: bool,
) -> Result<u64, ForgeError> {
    let mut lines = BufReader::new(File::open(path).await?).lines();
    let mut rejected = 0u64;
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match id_of_line(&line) {
            Some(id) if !validate || valid_custom_id(&id) => spiller.push(id).await?,
            _ => {
                if validate {
                    rejected += 1;
                }
            }
        }
    }
    Ok(rejected)
}

/// Verify that every `custom_id` in `input` has a terminal entry in `results` (or its
/// `dead` dead-letter sibling), in bounded memory. See the module docs.
pub async fn verify_completeness(
    input: impl AsRef<Path>,
    results: impl AsRef<Path>,
    dead: Option<impl AsRef<Path>>,
    cfg: &VerifyConfig,
) -> Result<VerifyReport, ForgeError> {
    let seq = VERIFY_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("forge-verify-{}-{}", std::process::id(), seq));
    tokio::fs::create_dir_all(&dir).await?;

    // Front half: stream both sides into bounded, sorted, spilled runs.
    let mut input_spiller = RunSpiller::new(dir.clone(), "in", cfg.run_capacity);
    let rejected_input = spill_ids(input.as_ref(), &mut input_spiller, true).await?;

    let mut emitted_spiller = RunSpiller::new(dir.clone(), "out", cfg.run_capacity);
    spill_ids(results.as_ref(), &mut emitted_spiller, false).await?;
    if let Some(d) = dead.as_ref() {
        let d = d.as_ref();
        // Distinguish "no dead-letter file" (Ok(false)) from an I/O error (Err): a
        // permission/stat failure must not be read as "no gaps" and undercount
        // completeness — propagate it.
        if tokio::fs::try_exists(d).await? {
            spill_ids(d, &mut emitted_spiller, false).await?;
        }
    }

    let input_runs = input_spiller.finish().await?;
    let emitted_runs = emitted_spiller.finish().await?;
    let runs_spilled = input_runs.len() + emitted_runs.len();

    // Back half: walk the two sorted, deduped streams in lockstep — a linear
    // set-difference. input<emitted ⇒ missing; input>emitted ⇒ extra; equal ⇒ ok.
    let mut a = UniqueStream::open(&input_runs).await?;
    let mut b = UniqueStream::open(&emitted_runs).await?;
    let mut sink = cfg.missing_out.clone().map(MissingSink::new);

    let mut report = VerifyReport {
        runs_spilled,
        ..Default::default()
    };
    let record_missing = |report: &mut VerifyReport, id: String| {
        report.missing += 1;
        if report.missing_sample.len() < MISSING_SAMPLE_CAP {
            report.missing_sample.push(id);
        }
    };

    let mut cur_a = a.next_unique().await?;
    let mut cur_b = b.next_unique().await?;
    loop {
        match (cur_a.as_deref(), cur_b.as_deref()) {
            (None, None) => break,
            (Some(x), None) => {
                if let Some(s) = sink.as_mut() {
                    s.write(x).await?;
                }
                record_missing(&mut report, x.to_string());
                cur_a = a.next_unique().await?;
            }
            (None, Some(_)) => {
                report.extra += 1;
                cur_b = b.next_unique().await?;
            }
            (Some(x), Some(y)) => match x.cmp(y) {
                std::cmp::Ordering::Less => {
                    if let Some(s) = sink.as_mut() {
                        s.write(x).await?;
                    }
                    record_missing(&mut report, x.to_string());
                    cur_a = a.next_unique().await?;
                }
                std::cmp::Ordering::Greater => {
                    report.extra += 1;
                    cur_b = b.next_unique().await?;
                }
                std::cmp::Ordering::Equal => {
                    cur_a = a.next_unique().await?;
                    cur_b = b.next_unique().await?;
                }
            },
        }
    }

    if let Some(s) = sink.as_mut() {
        s.flush().await?;
    }

    report.input_ids = a.unique;
    report.emitted_ids = b.unique;
    report.duplicate_input = a.total - a.unique;
    report.rejected_input = rejected_input;

    // Best-effort cleanup of the spill dir (read-only verifier; a killed process just
    // leaves a process-id-named temp dir).
    let _ = tokio::fs::remove_dir_all(&dir).await;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("forge-verify-t-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn write(path: &Path, lines: &[&str]) {
        std::fs::write(path, lines.join("\n") + "\n").unwrap();
    }

    fn input_line(id: &str) -> String {
        format!(r#"{{"custom_id":"{id}","method":"POST","url":"","body":{{}}}}"#)
    }
    fn result_line(id: &str) -> String {
        format!(r#"{{"custom_id":"{id}","response":{{"status_code":200,"body":{{}}}}}}"#)
    }

    #[tokio::test]
    async fn reports_missing_extra_duplicate() {
        let dir = tmp("mixed");
        let input = dir.join("in.jsonl");
        let results = dir.join("out.jsonl");
        let dead = dir.join("out.jsonl.dead.jsonl");
        // input a,b,c,c(dup),d ; results a,z(extra) ; dead b → missing c,d ; extra z
        write(
            &input,
            &[
                &input_line("a"),
                &input_line("b"),
                &input_line("c"),
                &input_line("c"),
                &input_line("d"),
            ],
        );
        write(&results, &[&result_line("a"), &result_line("z")]);
        write(&dead, &[&result_line("b")]);

        let rep = verify_completeness(&input, &results, Some(&dead), &VerifyConfig::default())
            .await
            .unwrap();

        assert_eq!(rep.input_ids, 4, "a,b,c,d distinct");
        assert_eq!(rep.duplicate_input, 1, "the second c");
        assert_eq!(rep.emitted_ids, 3, "a,b,z");
        assert_eq!(rep.missing, 2, "c and d");
        assert_eq!(rep.extra, 1, "z");
        assert!(!rep.is_complete());
        let mut sample = rep.missing_sample.clone();
        sample.sort();
        assert_eq!(sample, vec!["c".to_string(), "d".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn complete_run_is_zero_missing_and_writes_no_sidecar() {
        let dir = tmp("complete");
        let input = dir.join("in.jsonl");
        let results = dir.join("out.jsonl");
        let dead = dir.join("out.jsonl.dead.jsonl");
        let missing_out = dir.join("missing.txt");
        // a,b done; c dead-lettered → all terminal.
        write(
            &input,
            &[&input_line("a"), &input_line("b"), &input_line("c")],
        );
        write(&results, &[&result_line("a"), &result_line("b")]);
        write(&dead, &[&result_line("c")]);

        let cfg = VerifyConfig {
            missing_out: Some(missing_out.clone()),
            ..Default::default()
        };
        let rep = verify_completeness(&input, &results, Some(&dead), &cfg)
            .await
            .unwrap();
        assert!(rep.is_complete());
        assert_eq!(rep.missing, 0);
        assert_eq!(rep.extra, 0);
        assert!(!missing_out.exists(), "no sidecar when nothing is missing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_sidecar_lists_every_gap() {
        let dir = tmp("sidecar");
        let input = dir.join("in.jsonl");
        let results = dir.join("out.jsonl");
        let missing_out = dir.join("missing.txt");
        write(
            &input,
            &[&input_line("a"), &input_line("b"), &input_line("c")],
        );
        write(&results, &[&result_line("a")]);

        let cfg = VerifyConfig {
            missing_out: Some(missing_out.clone()),
            ..Default::default()
        };
        let rep = verify_completeness(&input, &results, None::<&Path>, &cfg)
            .await
            .unwrap();
        assert_eq!(rep.missing, 2);
        let mut got: Vec<String> = std::fs::read_to_string(&missing_out)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        got.sort();
        assert_eq!(got, vec!["b".to_string(), "c".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_invalid_input_id_without_aborting() {
        let dir = tmp("reject");
        let input = dir.join("in.jsonl");
        let results = dir.join("out.jsonl");
        // valid a · invalid id (space) · torn/unparseable line · valid b
        std::fs::write(
            &input,
            format!(
                "{}\n{}\n{{not json}}\n{}\n",
                input_line("a"),
                r#"{"custom_id":"has space","body":{}}"#,
                input_line("b")
            ),
        )
        .unwrap();
        write(&results, &[&result_line("a"), &result_line("b")]);

        let rep = verify_completeness(&input, &results, None::<&Path>, &VerifyConfig::default())
            .await
            .unwrap();
        assert_eq!(rep.rejected_input, 2, "bad id + torn line");
        assert_eq!(rep.input_ids, 2, "only a,b counted");
        assert!(rep.is_complete(), "the two valid ids are both present");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn bounded_memory_spills_and_matches_single_buffer() {
        // The structural proof: with a tiny run_capacity over many ids, the spiller
        // MUST flush multiple runs (the buffer is provably capped), and the result is
        // identical to a single-buffer run — correctness independent of buffer size.
        let dir = tmp("bounded");
        let input = dir.join("in.jsonl");
        let results = dir.join("out.jsonl");
        let n = 5_000;
        let in_lines: Vec<String> = (0..n).map(|i| input_line(&format!("id{i:05}"))).collect();
        // Emit all but the last 3 → 3 missing.
        let out_lines: Vec<String> = (0..n - 3)
            .map(|i| result_line(&format!("id{i:05}")))
            .collect();
        std::fs::write(&input, in_lines.join("\n") + "\n").unwrap();
        std::fs::write(&results, out_lines.join("\n") + "\n").unwrap();

        let tiny = VerifyConfig {
            run_capacity: 4,
            missing_out: None,
        };
        let small = verify_completeness(&input, &results, None::<&Path>, &tiny)
            .await
            .unwrap();
        let big = verify_completeness(
            &input,
            &results,
            None::<&Path>,
            &VerifyConfig::default(), // single huge buffer
        )
        .await
        .unwrap();

        assert!(
            small.runs_spilled > 2,
            "tiny capacity must spill many runs, got {}",
            small.runs_spilled
        );
        assert_eq!(
            big.runs_spilled, 2,
            "default capacity fits each side in one run"
        );
        assert_eq!(small.input_ids, n as u64);
        assert_eq!(small.missing, 3);
        assert_eq!(
            small.missing, big.missing,
            "result independent of buffer size"
        );
        assert_eq!(small.input_ids, big.input_ids);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
