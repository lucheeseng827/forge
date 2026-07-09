//! Prefix-bucket reordering — a bounded-RAM preprocessing pass that groups a JSONL by
//! shared prompt prefix so the downstream fan-out keeps an engine's **automatic prefix
//! cache** hot.
//!
//! vLLM/SGLang cache the KV of a shared prompt prefix; when consecutive requests to one
//! engine share a `(model, system-prompt)`, the prefill is reused and throughput jumps
//! (client-side reordering has been measured elsewhere at up to ~50% wall-clock on
//! long prefix-heavy jobs). forge
//! doesn't schedule per-token, but it *can* reorder the input so prefix-similar items are
//! contiguous in the queue — then the lease pull hands a run of them to one worker.
//!
//! Bounded RAM by construction (forge's ethos): each line is hashed by its
//! `(model, system-prompt-prefix)` into one of `buckets` temp files in a single pass,
//! then the buckets are concatenated in order. Only `buckets` file writers are open at
//! once — never the corpus. Exact-prefix matches always land together; a hash collision
//! merely co-locates two unrelated prefixes, which is harmless (never a correctness
//! issue, at worst a missed cache hit). Malformed lines can't be keyed, so they sort into
//! a final bucket and still flow through to the normal reject path at ingest.

use std::path::{Path, PathBuf};

use forge_core::ForgeError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

/// How many leading characters of the system (or first-user) prompt feed the bucket key.
/// Long enough that distinct system prompts separate; short enough that a shared
/// preamble with a varying tail still buckets together (prefix caching only needs the
/// *shared prefix* to match).
const PREFIX_CHARS: usize = 256;

/// Outcome of a bucketing pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketStats {
    pub lines: u64,
    pub buckets_used: usize,
}

/// The bucket key of one raw JSONL line: `(model, first PREFIX_CHARS of the system prompt,
/// else the first user message)`. `None` when the line can't be parsed enough to key it —
/// those go to a trailing catch-all bucket and are handled by the normal ingest reject
/// path, never dropped here.
fn bucket_key(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let body = v.get("body")?;
    let model = body.get("model").and_then(|m| m.as_str()).unwrap_or("");

    // Prefer the system message; fall back to the first user message; else the raw
    // `input` (embeddings/completions). Whatever forms the shared prefix.
    let mut prefix = String::new();
    if let Some(msgs) = body.get("messages").and_then(|m| m.as_array()) {
        if let Some(sys) = msgs
            .iter()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("system"))
            .or_else(|| msgs.first())
        {
            if let Some(c) = sys.get("content").and_then(|c| c.as_str()) {
                prefix = c.chars().take(PREFIX_CHARS).collect();
            }
        }
    } else if let Some(input) = body.get("input").and_then(|i| i.as_str()) {
        prefix = input.chars().take(PREFIX_CHARS).collect();
    } else if let Some(prompt) = body.get("prompt").and_then(|p| p.as_str()) {
        prefix = prompt.chars().take(PREFIX_CHARS).collect();
    }
    Some(format!("{model}\u{0}{prefix}"))
}

/// FNV-1a of a key → bucket index. Deterministic, dependency-free.
fn bucket_of(key: &str, buckets: usize) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % buckets as u64) as usize
}

/// Upper bound on `buckets`, well below common `ulimit -n` floors (e.g. 1024) even
/// accounting for stdio + the input/output file handles. Matches the value forge-cli
/// actually passes today; a public caller cannot silently request enough open temp
/// writers to trip `EMFILE`.
const MAX_BUCKETS: usize = 256;

/// Reorder `input` into `output` so prefix-similar lines are contiguous, bounded to
/// `buckets` open temp writers. `buckets` is clamped to `[1, MAX_BUCKETS]`. Writers are
/// opened lazily (only buckets a line actually hashes into ever get a file), and the temp
/// files live beside `output` (`<output>.pbkt.<n>`) — removed on both success and failure,
/// so a mid-pass error never strands `.pbkt.*` files next to the output.
pub async fn bucket_jsonl(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    buckets: usize,
) -> Result<BucketStats, ForgeError> {
    let buckets = buckets.clamp(1, MAX_BUCKETS);
    let output = output.as_ref();
    // One extra bucket at the end for un-keyable (malformed) lines, kept in file order.
    let n = buckets + 1;
    let unkeyable = buckets;

    let tmp_path = |i: usize| -> PathBuf {
        let mut p = output.as_os_str().to_owned();
        p.push(format!(".pbkt.{i}"));
        PathBuf::from(p)
    };

    let result = bucket_jsonl_inner(input.as_ref(), output, buckets, n, unkeyable, &tmp_path).await;
    if result.is_err() {
        for i in 0..n {
            let _ = tokio::fs::remove_file(tmp_path(i)).await;
        }
    }
    result
}

async fn bucket_jsonl_inner(
    input: &Path,
    output: &Path,
    buckets: usize,
    n: usize,
    unkeyable: usize,
    tmp_path: &dyn Fn(usize) -> PathBuf,
) -> Result<BucketStats, ForgeError> {
    // Lazily opened: a bucket only gets a file (and an fd) once a line actually hashes
    // into it, so a corpus with fewer distinct prefixes than `buckets` never opens `n`
    // writers at once.
    let mut writers: Vec<Option<BufWriter<File>>> = (0..n).map(|_| None).collect();

    let reader = BufReader::new(File::open(input).await.map_err(io_err)?);
    let mut lines_reader = reader.lines();
    let mut lines = 0u64;
    let mut used = vec![false; n];
    while let Some(line) = lines_reader.next_line().await.map_err(io_err)? {
        if line.trim().is_empty() {
            continue;
        }
        let idx = bucket_key(&line).map_or(unkeyable, |k| bucket_of(&k, buckets));
        if writers[idx].is_none() {
            let f = File::create(tmp_path(idx)).await.map_err(io_err)?;
            writers[idx] = Some(BufWriter::new(f));
        }
        let w = writers[idx].as_mut().expect("just created above");
        w.write_all(line.as_bytes()).await.map_err(io_err)?;
        w.write_all(b"\n").await.map_err(io_err)?;
        used[idx] = true;
        lines += 1;
    }
    for w in writers.iter_mut().flatten() {
        w.flush().await.map_err(io_err)?;
    }
    drop(writers);

    // Concatenate buckets in order into the output (keyed buckets first, un-keyable last).
    // Only buckets a line actually landed in (`used`) were ever created on disk.
    let mut out = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output)
            .await
            .map_err(io_err)?,
    );
    for (i, &is_used) in used.iter().enumerate().take(n) {
        if !is_used {
            continue;
        }
        let mut src = File::open(tmp_path(i)).await.map_err(io_err)?;
        tokio::io::copy(&mut src, &mut out).await.map_err(io_err)?;
    }
    out.flush().await.map_err(io_err)?;
    for (i, &is_used) in used.iter().enumerate().take(n) {
        if is_used {
            let _ = tokio::fs::remove_file(tmp_path(i)).await;
        }
    }

    Ok(BucketStats {
        lines,
        buckets_used: used.iter().filter(|&&u| u).count(),
    })
}

fn io_err(e: std::io::Error) -> ForgeError {
    ForgeError::Ingest(format!("prefix-bucket io: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn buckets_group_shared_prefixes_contiguously() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.jsonl");
        let output = dir.path().join("out.jsonl");
        // Two system prompts A and B, interleaved in the input.
        let mut body = String::new();
        for i in 0..6 {
            let sys = if i % 2 == 0 { "SYSTEM-A" } else { "SYSTEM-B" };
            body.push_str(&format!(
                "{{\"custom_id\":\"req-{i}\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{{\"model\":\"m\",\"messages\":[{{\"role\":\"system\",\"content\":\"{sys}\"}},{{\"role\":\"user\",\"content\":\"q{i}\"}}]}}}}\n"
            ));
        }
        std::fs::write(&input, body).unwrap();

        let stats = bucket_jsonl(&input, &output, 64).await.unwrap();
        assert_eq!(stats.lines, 6);

        // In the output, all SYSTEM-A lines are contiguous and all SYSTEM-B lines are
        // contiguous (no interleaving) — that is the prefix-cache-friendly ordering.
        let out = std::fs::read_to_string(&output).unwrap();
        let sys_seq: Vec<&str> = out
            .lines()
            .map(|l| if l.contains("SYSTEM-A") { "A" } else { "B" })
            .collect();
        // Count transitions between A and B; a grouped ordering has exactly 1.
        let transitions = sys_seq.windows(2).filter(|w| w[0] != w[1]).count();
        assert_eq!(transitions, 1, "prefixes must be grouped, got {sys_seq:?}");
        // No line lost.
        assert_eq!(out.lines().count(), 6);
    }

    #[tokio::test]
    async fn malformed_lines_survive_to_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.jsonl");
        let output = dir.path().join("out.jsonl");
        std::fs::write(
            &input,
            "not json\n{\"custom_id\":\"ok\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"m\",\"messages\":[]}}\n",
        )
        .unwrap();
        let stats = bucket_jsonl(&input, &output, 16).await.unwrap();
        assert_eq!(stats.lines, 2);
        let out = std::fs::read_to_string(&output).unwrap();
        assert!(
            out.contains("not json"),
            "malformed line must not be dropped"
        );
    }
}
