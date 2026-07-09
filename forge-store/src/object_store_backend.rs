//! object_store-backed result sink (feature `object_store`) — the at-scale backend
//! for S3 / GCS / Azure (and `LocalFileSystem` / `InMemory`).
//!
//! Cloud object stores have **no append**, so the JSONL store's single growing file
//! doesn't translate. Instead each terminal result is its own immutable object —
//! `{prefix}/done/{custom_id}.json` or `{prefix}/dead/{custom_id}.json` — written with
//! a **conditional `PutMode::Create`** (write-if-absent). That makes the write
//! idempotent: a re-run after an expired lease re-PUTs the same key and the store
//! returns `AlreadyExists`, which we treat as a harmless no-op — the same
//! **exactly-once *effect*** the JSONL store gets from its emitted-id set.
//!
//! `emitted_ids()` must stay **O(emitted)** on resume (never list/re-read the whole
//! result prefix, which would be O(all results) at 50M). So every emitted id also
//! drops a tiny marker object under a separate `{prefix}/_manifest/` prefix; resume
//! lists only that prefix to rebuild the set. One small extra PUT per id buys an
//! O(emitted) resume that doesn't touch the bulky result objects.
//!
//! This backend is **off by default** (the `object_store` feature): the cloud SDKs
//! never compile into the lean `forge` musl binary. It implements the same
//! `ResultStore` trait as `JsonlStore`, so the embeddable loop is unchanged.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use forge_core::{ForgeError, ItemResult, ResultStore, UsageTotals};
use futures_util::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

/// Which terminal sub-prefix a result lands in. A `custom_id` is terminal in exactly
/// one (success xor dead-letter), guarded by the shared emitted set — the same
/// invariant the JSONL store holds across its two files.
#[derive(Clone, Copy)]
enum Kind {
    Done,
    Dead,
}

impl Kind {
    fn dir(self) -> &'static str {
        match self {
            Kind::Done => "done",
            Kind::Dead => "dead",
        }
    }
}

fn se(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Store(e.to_string())
}

/// An [`ObjectStore`]-backed [`ResultStore`]. Generic over the concrete store, so the
/// same idempotency logic runs over `InMemory` / `LocalFileSystem` / `AmazonS3` /
/// `GoogleCloudStorage` / `MicrosoftAzure` unchanged.
pub struct ObjStore {
    store: Arc<dyn ObjectStore>,
    /// Job root, e.g. `results/{job}`.
    prefix: String,
    inner: Mutex<ObjInner>,
}

#[derive(Default)]
struct ObjInner {
    emitted: HashSet<String>,
    loaded: bool,
}

impl ObjStore {
    /// Build over an already-constructed store, rooted at `results/{job}`.
    pub fn new(store: Arc<dyn ObjectStore>, job: &str) -> Self {
        Self {
            store,
            prefix: format!("results/{job}"),
            inner: Mutex::new(ObjInner::default()),
        }
    }

    /// Parse `s3://bucket/prefix` (or `gs://` / `az://` / `file://` / `memory://`) into
    /// a store + base prefix via [`object_store::parse_url_opts`]; `opts` carries
    /// creds/region (e.g. from the environment). The job root becomes
    /// `{base}/results/{job}`.
    pub fn from_url(
        url: &url::Url,
        opts: impl IntoIterator<Item = (String, String)>,
        job: &str,
    ) -> Result<Self, ForgeError> {
        let (store, base) = object_store::parse_url_opts(url, opts).map_err(se)?;
        let base = base.to_string();
        let prefix = if base.is_empty() {
            format!("results/{job}")
        } else {
            format!("{base}/results/{job}")
        };
        Ok(Self {
            store: Arc::from(store),
            prefix,
            inner: Mutex::new(ObjInner::default()),
        })
    }

    /// Construct rooted at an already-known `{base}/results/{job}` (from run discovery).
    fn at_job(store: Arc<dyn ObjectStore>, base: &str, job: &str) -> Self {
        let prefix = if base.is_empty() {
            format!("results/{job}")
        } else {
            format!("{base}/results/{job}")
        };
        Self {
            store,
            prefix,
            inner: Mutex::new(ObjInner::default()),
        }
    }

    /// Sum captured token usage across every `done/` result object (streaming, one
    /// object at a time — RAM independent of the result count). The object-store analog
    /// of [`crate::sum_usage`] over the local results JSONL: dead-lettered items carry no
    /// usage, so like the JSONL path this counts only successful results.
    pub async fn sum_usage(&self) -> Result<UsageTotals, ForgeError> {
        let prefix = ObjPath::from(format!("{}/{}", self.prefix, Kind::Done.dir()));
        let mut stream = self.store.list(Some(&prefix));
        let mut totals = UsageTotals::default();
        while let Some(meta) = stream.try_next().await.map_err(se)? {
            let bytes = self
                .store
                .get_opts(&meta.location, GetOptions::default())
                .await
                .map_err(se)?
                .bytes()
                .await
                .map_err(se)?;
            // A stray non-result object under the prefix is skipped, mirroring the JSONL
            // path's tolerance of a torn line.
            if let Ok(r) = serde_json::from_slice::<ItemResult>(&bytes) {
                totals.add(&r.usage);
            }
        }
        Ok(totals)
    }

    /// Stream the terminal ids — every `_manifest/` marker, which is dropped for both a
    /// `done` and a `dead` result — to `dest` as `{"custom_id":"…"}` lines (bounded RAM).
    /// `forge verify` then runs its existing exact, bounded-RAM completeness sweep over
    /// that file, so a 50M-id run never materializes the id set. Returns the id count.
    pub async fn dump_emitted_ids(&self, dest: &Path) -> Result<u64, ForgeError> {
        let mut stream = self.store.list(Some(&self.manifest_prefix()));
        let mut w = BufWriter::new(tokio::fs::File::create(dest).await?);
        let mut n = 0u64;
        while let Some(meta) = stream.try_next().await.map_err(se)? {
            if let Some(id) = meta.location.filename() {
                // custom_id is validated `^[a-zA-Z0-9_-]{1,64}$`, so it needs no escaping.
                w.write_all(format!("{{\"custom_id\":\"{id}\"}}\n").as_bytes())
                    .await?;
                n += 1;
            }
        }
        w.flush().await?;
        Ok(n)
    }

    fn result_path(&self, custom_id: &str, kind: Kind) -> ObjPath {
        ObjPath::from(format!("{}/{}/{custom_id}.json", self.prefix, kind.dir()))
    }

    fn manifest_marker(&self, custom_id: &str) -> ObjPath {
        ObjPath::from(format!("{}/_manifest/{custom_id}", self.prefix))
    }

    fn manifest_prefix(&self) -> ObjPath {
        ObjPath::from(format!("{}/_manifest", self.prefix))
    }

    /// Load the emitted-id set from the `_manifest/` prefix exactly once. Lists only
    /// that prefix (O(emitted)), never the `done/`/`dead/` result objects.
    async fn ensure_loaded(&self, inner: &mut ObjInner) -> Result<(), ForgeError> {
        if inner.loaded {
            return Ok(());
        }
        let prefix = self.manifest_prefix();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.try_next().await.map_err(se)? {
            if let Some(name) = meta.location.filename() {
                inner.emitted.insert(name.to_string());
            }
        }
        inner.loaded = true;
        Ok(())
    }

    /// Conditional create. `Ok(true)` = newly written, `Ok(false)` = already existed
    /// (the idempotent no-op).
    async fn put_if_absent(&self, path: &ObjPath, body: Vec<u8>) -> Result<bool, ForgeError> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(path, body.into(), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(se(e)),
        }
    }

    /// Shared write path for both terminal kinds: store-the-object, drop the manifest
    /// marker, remember the id.
    async fn emit(&self, r: &ItemResult, kind: Kind) -> Result<(), ForgeError> {
        let mut inner = self.inner.lock().await;
        self.ensure_loaded(&mut inner).await?;
        if inner.emitted.contains(&r.custom_id) {
            return Ok(()); // already terminal — idempotent no-op
        }
        let body = serde_json::to_vec(r)?;
        // Conditional create makes this safe even if two workers race the same id: one
        // writes, the other sees AlreadyExists. The result object is the source of
        // truth; the marker is the resume index.
        self.put_if_absent(&self.result_path(&r.custom_id, kind), body)
            .await?;
        self.put_if_absent(&self.manifest_marker(&r.custom_id), Vec::new())
            .await?;
        inner.emitted.insert(r.custom_id.clone());
        Ok(())
    }
}

/// Open an [`ObjStore`] from a `forge run --out` URL — `s3://` / `gs://` / `az://`
/// (cloud) or `file://` / `memory://` (local/in-process). Cloud credentials and region
/// are read from the process environment: the standard `AWS_*` / `GOOGLE_*` / `AZURE_*`
/// variables, matching object_store's own `from_env` conventions. Only those prefixes
/// are forwarded — object_store also accepts bare aliases (`region`, `token`, `bucket`),
/// so passing the whole environment could misread an unrelated variable. Results land
/// under `{url}/results/{job}`.
pub fn objstore_from_out(out: &str, job: &str) -> Result<ObjStore, ForgeError> {
    let url = parse_out_url(out)?;
    ObjStore::from_url(&url, scheme_env_opts(&url), job)
}

/// Cloud config options harvested from the environment. Only the `AWS_*` / `GOOGLE_*` /
/// `AZURE_*` prefixes are forwarded — object_store also accepts bare aliases (`region`,
/// `token`, `bucket`), so passing the whole environment could misread an unrelated var.
fn env_opts() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(k, _)| {
            let k = k.to_ascii_uppercase();
            k.starts_with("AWS_") || k.starts_with("GOOGLE_") || k.starts_with("AZURE_")
        })
        .collect()
}

/// Cloud env options, but **only** for cloud schemes. The `file://` and `memory://`
/// builders **reject unknown configuration keys**, so forwarding `AWS_*`/`GOOGLE_*`/
/// `AZURE_*` to a local store would make a local run fail whenever cloud creds happen to
/// be exported. Deny-listing the two local schemes (rather than allow-listing cloud ones)
/// keeps any real cloud scheme working.
fn scheme_env_opts(url: &url::Url) -> Vec<(String, String)> {
    match url.scheme() {
        "file" | "memory" => Vec::new(),
        _ => env_opts(),
    }
}

fn parse_out_url(out: &str) -> Result<url::Url, ForgeError> {
    url::Url::parse(out)
        .map_err(|e| ForgeError::Config(format!("invalid object-store URL {out:?}: {e}")))
}

/// Open the **single** run written under `{out}/results/` (the layout `forge run --out
/// <out>` produces), for read-back by `forge cost` / `forge verify`. The caller passes
/// the same `--out` URL; this discovers the one job beneath it. Errors if there are zero
/// runs (nothing written yet) or more than one (an ambiguous shared prefix — point the
/// URL at a single run).
pub async fn objstore_open_run(out: &str) -> Result<ObjStore, ForgeError> {
    let url = parse_out_url(out)?;
    let (store, base) = object_store::parse_url_opts(&url, scheme_env_opts(&url)).map_err(se)?;
    let store: Arc<dyn ObjectStore> = Arc::from(store);
    let base = base.to_string();
    let results_prefix = if base.is_empty() {
        "results".to_string()
    } else {
        format!("{base}/results")
    };
    let listed = store
        .list_with_delimiter(Some(&ObjPath::from(results_prefix.clone())))
        .await
        .map_err(se)?;
    let jobs: Vec<String> = listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().map(str::to_string))
        .collect();
    match jobs.as_slice() {
        [job] => Ok(ObjStore::at_job(store, &base, job)),
        [] => Err(ForgeError::Store(format!(
            "no forge results found under {out} (expected {results_prefix}/<job>/)"
        ))),
        many => Err(ForgeError::Store(format!(
            "multiple runs under {out}: {many:?}; point --results at a single run"
        ))),
    }
}

impl ResultStore for ObjStore {
    async fn put(&self, result: &ItemResult) -> Result<(), ForgeError> {
        self.emit(result, Kind::Done).await
    }

    async fn dead_letter(&self, result: &ItemResult) -> Result<(), ForgeError> {
        self.emit(result, Kind::Dead).await
    }

    async fn emitted_ids(&self) -> Result<HashSet<String>, ForgeError> {
        let mut inner = self.inner.lock().await;
        self.ensure_loaded(&mut inner).await?;
        Ok(inner.emitted.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_core::{ItemResponse, TokenUsage};
    use object_store::memory::InMemory;

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
            completed_at: 0,
        }
    }

    fn dead_result(id: &str) -> ItemResult {
        let mut r = ok_result(id);
        r.response = None;
        r.error = Some(forge_core::ItemError {
            code: "worker_error".into(),
            message: "boom".into(),
        });
        r
    }

    async fn count_prefix(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
        let p = ObjPath::from(prefix.to_string());
        let mut stream = store.list(Some(&p));
        let mut n = 0;
        while stream.try_next().await.unwrap().is_some() {
            n += 1;
        }
        n
    }

    #[tokio::test]
    async fn put_is_idempotent_and_dedups() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ObjStore::new(Arc::clone(&backing), "job1");

        s.put(&ok_result("a")).await.unwrap();
        s.put(&ok_result("a")).await.unwrap(); // duplicate — no-op
        s.put(&ok_result("b")).await.unwrap();

        let emitted = s.emitted_ids().await.unwrap();
        assert_eq!(emitted.len(), 2);
        assert!(emitted.contains("a") && emitted.contains("b"));
        // Exactly one object for `a` under done/ (conditional-create dedup).
        assert_eq!(count_prefix(&backing, "results/job1/done").await, 2);
    }

    #[tokio::test]
    async fn resume_reads_manifest_not_the_result_objects() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Emit 3 ids through the store (3 manifest markers), then write 1000 stray
        // result objects directly — emitted_ids must reflect the 3, not the 1000.
        {
            let s = ObjStore::new(Arc::clone(&backing), "job2");
            for id in ["x", "y", "z"] {
                s.put(&ok_result(id)).await.unwrap();
            }
        }
        for i in 0..1000 {
            let p = ObjPath::from(format!("results/job2/done/stray{i:04}.json"));
            backing
                .put_opts(&p, Vec::new().into(), PutOptions::default())
                .await
                .unwrap();
        }

        // A fresh store over the same backing rebuilds the emitted set from the
        // manifest only.
        let s2 = ObjStore::new(Arc::clone(&backing), "job2");
        let emitted = s2.emitted_ids().await.unwrap();
        assert_eq!(
            emitted.len(),
            3,
            "O(emitted) from the manifest, not O(objects)"
        );
        assert!(emitted.contains("x") && emitted.contains("y") && emitted.contains("z"));
    }

    #[tokio::test]
    async fn dead_letter_is_terminal_and_blocks_a_later_success() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ObjStore::new(Arc::clone(&backing), "job3");

        s.dead_letter(&dead_result("p")).await.unwrap();
        s.put(&ok_result("p")).await.unwrap(); // already terminal — no-op

        assert_eq!(count_prefix(&backing, "results/job3/dead").await, 1);
        assert_eq!(count_prefix(&backing, "results/job3/done").await, 0);
        assert_eq!(s.emitted_ids().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_writes_of_same_id_emit_once() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = Arc::new(ObjStore::new(Arc::clone(&backing), "job4"));
        let (s1, s2) = (Arc::clone(&s), Arc::clone(&s));
        let r1 = tokio::spawn(async move { s1.put(&ok_result("dup")).await });
        let r2 = tokio::spawn(async move { s2.put(&ok_result("dup")).await });
        r1.await.unwrap().unwrap();
        r2.await.unwrap().unwrap();

        assert_eq!(count_prefix(&backing, "results/job4/done").await, 1);
        assert_eq!(s.emitted_ids().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn local_filesystem_parity() {
        use object_store::local::LocalFileSystem;
        let dir = std::env::temp_dir().join(format!("forge-objstore-{}-fs", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let backing: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap());

        {
            let s = ObjStore::new(Arc::clone(&backing), "j");
            s.put(&ok_result("a")).await.unwrap();
            s.put(&ok_result("a")).await.unwrap();
            s.dead_letter(&dead_result("b")).await.unwrap();
        }
        let s2 = ObjStore::new(Arc::clone(&backing), "j");
        let emitted = s2.emitted_ids().await.unwrap();
        assert_eq!(emitted.len(), 2);
        assert!(emitted.contains("a") && emitted.contains("b"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn objstore_from_out_opens_and_writes() {
        // memory:// needs no creds/network; the helper parses the URL and returns a
        // working store rooted at results/{job}.
        let s = objstore_from_out("memory:///", "jobx").unwrap();
        s.put(&ok_result("a")).await.unwrap();
        s.put(&ok_result("a")).await.unwrap(); // idempotent
        assert_eq!(s.emitted_ids().await.unwrap().len(), 1);
        assert!(objstore_from_out("not a url", "j").is_err());
    }

    #[tokio::test]
    async fn sum_usage_counts_done_and_dump_lists_all_terminal_ids() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ObjStore::new(Arc::clone(&backing), "jobc");
        s.put(&ok_result("a")).await.unwrap(); // usage 7
        s.put(&ok_result("b")).await.unwrap(); // usage 7
        s.dead_letter(&dead_result("c")).await.unwrap(); // terminal, but not a `done`

        // cost: only the two successful results count (dead-letters carry no usage).
        assert_eq!(s.sum_usage().await.unwrap().total_tokens(), 14);

        // verify: the manifest has a marker for every terminal id (done AND dead).
        let dest = std::env::temp_dir().join(format!("forge-dump-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&dest);
        assert_eq!(s.dump_emitted_ids(&dest).await.unwrap(), 3);
        let dumped = std::fs::read_to_string(&dest).unwrap();
        assert_eq!(dumped.lines().count(), 3);
        assert!(dumped.contains("\"custom_id\":\"c\"")); // the dead id is terminal too
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn open_run_discovers_the_single_job() {
        let dir = std::env::temp_dir().join(format!("forge-openrun-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("file://{}", dir.display());

        // Write a run the way `forge run --out <url>` does.
        {
            let s = objstore_from_out(&url, "myjob").unwrap();
            s.put(&ok_result("a")).await.unwrap();
            s.put(&ok_result("b")).await.unwrap();
            s.dead_letter(&dead_result("c")).await.unwrap();
        }

        // cost / verify pass the same URL; discovery finds the one job under results/.
        let run = objstore_open_run(&url).await.unwrap();
        assert_eq!(run.sum_usage().await.unwrap().total_tokens(), 14);
        let dest = dir.join("ids.jsonl");
        assert_eq!(run.dump_emitted_ids(&dest).await.unwrap(), 3);

        // A URL with no run under it is a clear error, not a silent empty result.
        let empty =
            std::env::temp_dir().join(format!("forge-openrun-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&empty);
        std::fs::create_dir_all(&empty).unwrap();
        assert!(objstore_open_run(&format!("file://{}", empty.display()))
            .await
            .is_err());

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&empty);
    }

    #[test]
    fn from_url_parses_each_scheme() {
        // Non-network schemes resolve to a store + prefix without any I/O.
        let mem = url::Url::parse("memory:///").unwrap();
        let s = ObjStore::from_url(&mem, std::iter::empty::<(String, String)>(), "j").unwrap();
        assert_eq!(s.prefix, "results/j");

        let file = url::Url::parse("file:///tmp/forge-out").unwrap();
        let s = ObjStore::from_url(&file, std::iter::empty::<(String, String)>(), "j").unwrap();
        assert!(s.prefix.ends_with("results/j"));
    }
}
