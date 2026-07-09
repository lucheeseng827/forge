//! Domain types. The JSONL contract is the **OpenAI Batch contract verbatim**:
//! one independent request per line keyed by a caller-supplied `custom_id`; output
//! is a matched JSONL keyed by the same id, order NOT guaranteed. Anthropic's
//! `custom_id` and Bedrock's `recordId` are accepted as aliases.

use serde::{Deserialize, Serialize};

use crate::state::ItemState;

/// Which OpenAI-compatible endpoint a worker serves. Every item in one run hits
/// the same kind of endpoint — there is no per-item "task type" that branches
/// behavior (that would be a dagron `Executor`, not a forge `Worker`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    #[default]
    Chat,
    Completions,
    Embeddings,
}

impl EndpointKind {
    /// Default request path for this endpoint kind.
    pub fn default_path(self) -> &'static str {
        match self {
            EndpointKind::Chat => "/v1/chat/completions",
            EndpointKind::Completions => "/v1/completions",
            EndpointKind::Embeddings => "/v1/embeddings",
        }
    }

    /// Best-effort inverse of [`default_path`](Self::default_path): classify an
    /// item's request `path` so per-item response validation matches the *item's*
    /// endpoint even when one run carries a mix (the OpenAI Batch contract allows a
    /// per-line `url`). `None` when the path doesn't look like any known endpoint;
    /// callers fall back to the run-level default. Order matters — the chat suffix
    /// is checked before the bare `/completions` one.
    pub fn from_path(path: &str) -> Option<EndpointKind> {
        if path.contains("/embeddings") {
            Some(EndpointKind::Embeddings)
        } else if path.contains("/chat/completions") {
            Some(EndpointKind::Chat)
        } else if path.contains("/completions") {
            Some(EndpointKind::Completions)
        } else {
            None
        }
    }
}

/// A hint about the engine behind a worker. Affects only the health-probe shape
/// and which concurrency knob the operator set — never the wire contract, which
/// is OpenAI-compatible regardless. A router URL is just another worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EngineHint {
    #[default]
    Vllm,
    Sglang,
    LlamaCpp,
    Router,
}

impl EngineHint {
    /// The engine's queue-depth metric endpoint for **load-aware dispatch** (B3), or
    /// `None` when there's no standard one to read. A read-only side channel: a
    /// failure here only leaves the worker's load unknown, never unready. A `Router`
    /// self-balances, so forge treats it as load-unknown (round-robin) rather than
    /// second-guessing it.
    ///
    /// - vLLM: `/metrics` (Prometheus text; gauge `vllm:num_requests_waiting`)
    /// - SGLang: `/get_server_info` (JSON; `internal_states[*].num_waiting_reqs`)
    /// - llama.cpp: `/slots` (JSON array; count of `is_processing == true`)
    pub fn metrics_path(self) -> Option<&'static str> {
        match self {
            EngineHint::Vllm => Some("/metrics"),
            EngineHint::Sglang => Some("/get_server_info"),
            EngineHint::LlamaCpp => Some("/slots"),
            EngineHint::Router => None,
        }
    }
}

/// Per-request token accounting, captured free from every OpenAI-compatible
/// `usage` object. llama.cpp's native `tokens_evaluated`/`tokens_predicted` map
/// into the same shape. The invoiceable tokens-per-spot-dollar metric.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    /// Input tokens served from the provider's prompt cache — a **subset** of
    /// `prompt_tokens`, billed at a discount (from `prompt_tokens_details.cached_tokens`).
    /// `0` when the engine reports no cache detail.
    #[serde(default)]
    pub cached_tokens: u64,
    /// Reasoning tokens — a **subset** of `completion_tokens` on reasoning models
    /// (from `completion_tokens_details.reasoning_tokens`). Surfaced so the cost ledger
    /// can show how much output was reasoning; not double-counted in totals.
    #[serde(default)]
    pub reasoning_tokens: u64,
}

impl TokenUsage {
    /// Parse an OpenAI-compatible `usage` object, including the nested
    /// `prompt_tokens_details.cached_tokens` / `completion_tokens_details.reasoning_tokens`
    /// that a flat `serde` derive misses. Used at the point forge captures usage from a
    /// raw engine body (forge's own re-serialized result lines already carry the flat
    /// fields, so reading those back needs only the derive).
    pub fn from_openai_usage(v: &serde_json::Value) -> Self {
        let get = |k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
        let nested = |parent: &str, child: &str| {
            v.get(parent)
                .and_then(|d| d.get(child))
                .and_then(|x| x.as_u64())
                .unwrap_or(0)
        };
        TokenUsage {
            prompt_tokens: get("prompt_tokens"),
            completion_tokens: get("completion_tokens"),
            total_tokens: get("total_tokens"),
            cached_tokens: nested("prompt_tokens_details", "cached_tokens"),
            reasoning_tokens: nested("completion_tokens_details", "reasoning_tokens"),
        }
    }
}

/// The declared capability of one black-box OpenAI-compatible endpoint.
///
/// `concurrency_limit` MUST mirror the engine's own cap (vLLM `--max-num-seqs`,
/// SGLang `--max-running-requests`, llama-server `--parallel`) so forge never
/// oversubscribes a worker into an OOM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSpec {
    pub worker_id: String,
    pub base_url: String,
    pub endpoint_kind: EndpointKind,
    /// = the engine's max concurrent sequences. The per-worker in-flight semaphore
    /// width.
    pub concurrency_limit: usize,
    pub engine_hint: EngineHint,
    pub health_path: String,
    /// Runtime readiness, updated by the health probe. Not part of the
    /// declared identity.
    #[serde(default)]
    pub ready: bool,
    /// Optional **global request-rate ceiling** in requests/second (feature-gated,
    /// ROADMAP §2 `governor`). `None` = unlimited (the default). `forge-core` only
    /// carries the number — the GCRA limiter itself lives in `forge-worker` behind an
    /// opt-in `governor` feature, so the lean default binary never compiles it. Kept
    /// `skip_serializing_if` so existing spec JSON stays byte-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_per_second: Option<u32>,
}

impl WorkerSpec {
    /// A worker with sane defaults: `concurrency_limit = 256`, `engine_hint = vLLM`,
    /// `health_path = "/health"`. Override with the builder methods.
    pub fn new(
        worker_id: impl Into<String>,
        base_url: impl Into<String>,
        endpoint_kind: EndpointKind,
    ) -> Self {
        Self {
            worker_id: worker_id.into(),
            base_url: base_url.into(),
            endpoint_kind,
            concurrency_limit: 256,
            engine_hint: EngineHint::default(),
            health_path: "/health".to_string(),
            ready: false,
            rate_limit_per_second: None,
        }
    }

    /// Set the in-flight cap (= the engine's own concurrency knob).
    pub fn concurrency(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }

    /// Set an optional global request-rate ceiling (req/sec). Only takes effect when
    /// `forge-worker` is built with its `governor` feature; otherwise it's inert
    /// metadata. Orthogonal to [`concurrency`](Self::concurrency): that caps in-flight
    /// requests, this caps the emission *rate*.
    pub fn rate_limit(mut self, per_second: u32) -> Self {
        self.rate_limit_per_second = Some(per_second);
        self
    }

    /// Set the engine hint (health-probe shape only).
    pub fn engine_hint(mut self, hint: EngineHint) -> Self {
        self.engine_hint = hint;
        self
    }

    /// Override the health-probe path.
    pub fn health_path(mut self, path: impl Into<String>) -> Self {
        self.health_path = path.into();
        self
    }

    /// The full request URL for this worker, given the item's `url` (or the
    /// endpoint default when the item left it blank).
    pub fn request_url(&self, item_url: &str) -> String {
        let path = if item_url.is_empty() {
            self.endpoint_kind.default_path()
        } else {
            item_url
        };
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

fn default_method() -> String {
    "POST".to_string()
}

/// The atomic unit of fan-out: one inference request plus its queue bookkeeping.
///
/// Deserializes directly from an input JSONL line — the runtime fields
/// (`status`, `attempts`, `leased_*`, `last_error`) default in, so an OpenAI /
/// Anthropic / Bedrock batch line parses as-is. **No `depends_on`, no fan-in.**
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    /// Identity, idempotency/dedup key, and resume key, all at once.
    /// Accepts Bedrock's `recordId` as an alias (Anthropic already uses `custom_id`).
    #[serde(alias = "recordId")]
    pub custom_id: String,
    #[serde(default = "default_method")]
    pub method: String,
    /// Request path; empty → the worker's endpoint default.
    #[serde(default)]
    pub url: String,
    /// The request payload, passed through to the engine verbatim.
    pub body: serde_json::Value,

    #[serde(default)]
    pub status: ItemState,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leased_until: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leased_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// The success body of an [`ItemResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemResponse {
    pub status_code: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub body: serde_json::Value,
}

/// The error body of an [`ItemResult`] (terminal failure after retries).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemError {
    pub code: String,
    pub message: String,
}

/// What gets written to the store, keyed by `custom_id`, order-independent —
/// the OpenAI Batch *output* contract. Persisted to the store **before** the item
/// is acked `Done` (the exactly-once-effect ordering).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemResult {
    pub custom_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<ItemResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ItemError>,
    pub usage: TokenUsage,
    pub worker_id: String,
    pub latency_ms: u64,
    pub attempt: u32,
    pub completed_at: i64,
}

impl ItemResult {
    /// Build a terminal-failure result for an item whose retries are exhausted.
    pub fn failed(item: &Item, worker_id: impl Into<String>, error: &crate::ForgeError) -> Self {
        Self {
            custom_id: item.custom_id.clone(),
            response: None,
            error: Some(ItemError {
                code: "worker_error".to_string(),
                message: error.to_string(),
            }),
            usage: TokenUsage::default(),
            worker_id: worker_id.into(),
            latency_ms: 0,
            attempt: item.attempts,
            completed_at: crate::now_ms(),
        }
    }

    /// True when this result represents a successful response (not a dead-letter).
    pub fn is_success(&self) -> bool {
        self.response.is_some() && self.error.is_none()
    }
}

/// Coarse run status. Authoritative per-item progress lives in the queue rows;
/// this is metadata for `forge status` / the (later) control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    #[default]
    Running,
    Done,
    Failed,
    Paused,
}

/// Aggregate progress for a run. **No dependency/graph field** — the dagron
/// boundary in the type system.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobTotals {
    pub items_total: u64,
    pub items_done: u64,
    pub items_dead: u64,
    pub tokens_prompt: u64,
    pub tokens_completion: u64,
}

impl JobTotals {
    pub fn tokens_total(&self) -> u64 {
        self.tokens_prompt + self.tokens_completion
    }

    /// Fold a successful result's usage into the totals.
    pub fn record_done(&mut self, usage: &TokenUsage) {
        self.items_done += 1;
        self.tokens_prompt += usage.prompt_tokens;
        self.tokens_completion += usage.completion_tokens;
    }
}

/// Run metadata. Progress is authoritative in the queue; this records the shape of
/// the run (where input/output live, which workers, which endpoint kind).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub job_id: String,
    pub input_uri: String,
    pub endpoint_kind: EndpointKind,
    pub worker_specs: Vec<WorkerSpec>,
    pub output_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub created_at: i64,
    #[serde(default)]
    pub status: JobStatus,
    #[serde(default)]
    pub totals: JobTotals,
}

/// A contiguous **line-range** of the input, for streaming/resume bookkeeping
/// ONLY. It has no successors and no executor choice — explicitly *not* a dagron
/// task. Lets a resumed run seek to the first un-hydrated offset instead of
/// rescanning a 50M-line file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shard {
    pub shard_id: u64,
    pub byte_offset_start: u64,
    pub byte_offset_end: u64,
    pub line_start: u64,
    pub line_end: u64,
    pub lines_hydrated: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_openai_batch_line() {
        let line = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[]}}"#;
        let item: Item = serde_json::from_str(line).unwrap();
        assert_eq!(item.custom_id, "req-1");
        assert_eq!(item.status, ItemState::Pending);
        assert_eq!(item.attempts, 0);
    }

    #[test]
    fn accepts_bedrock_record_id_alias() {
        let line = r#"{"recordId":"rec-9","body":{"input":"hi"}}"#;
        let item: Item = serde_json::from_str(line).unwrap();
        assert_eq!(item.custom_id, "rec-9");
        assert_eq!(item.method, "POST"); // defaulted
    }

    #[test]
    fn worker_request_url_uses_endpoint_default_when_blank() {
        let w = WorkerSpec::new("gpu1", "http://gpu1:8000/", EndpointKind::Chat).concurrency(128);
        assert_eq!(w.concurrency_limit, 128);
        assert_eq!(w.request_url(""), "http://gpu1:8000/v1/chat/completions");
        assert_eq!(
            w.request_url("/v1/embeddings"),
            "http://gpu1:8000/v1/embeddings"
        );
    }

    #[test]
    fn endpoint_from_path_classifies() {
        assert_eq!(
            EndpointKind::from_path("/v1/embeddings"),
            Some(EndpointKind::Embeddings)
        );
        assert_eq!(
            EndpointKind::from_path("/v1/chat/completions"),
            Some(EndpointKind::Chat)
        );
        assert_eq!(
            EndpointKind::from_path("/v1/completions"),
            Some(EndpointKind::Completions)
        );
        assert_eq!(EndpointKind::from_path(""), None); // blank → caller's default
        assert_eq!(EndpointKind::from_path("/v1/rerank"), None);
    }

    #[test]
    fn token_totals_fold() {
        let mut t = JobTotals::default();
        t.record_done(&TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 3,
            total_tokens: 13,
            ..Default::default()
        });
        t.record_done(&TokenUsage {
            prompt_tokens: 5,
            completion_tokens: 2,
            total_tokens: 7,
            ..Default::default()
        });
        assert_eq!(t.items_done, 2);
        assert_eq!(t.tokens_total(), 20);
    }
}
