//! The OpenAI Batch API wire shapes — field names and object envelopes kept
//! **exact** so unmodified OpenAI SDK code deserializes them.
//!
//! These are pure data-transfer objects: the runtime truth lives in the forge queue
//! (progress) and the batch registry (status/paths); a [`BatchObject`] is built fresh
//! on each response from that truth. Nullable fields (`output_file_id`,
//! `in_progress_at`, …) are serialized as JSON `null` when absent — **not** omitted —
//! exactly as the real API renders them, so a client that reads them by key is happy.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::rand_hex;

/// The OpenAI **File object** returned by `POST /v1/files` and `GET /v1/files/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObject {
    pub id: String,
    /// Always `"file"`.
    pub object: String,
    pub bytes: u64,
    pub created_at: i64,
    pub filename: String,
    /// `"batch"` for an uploaded input; `"batch_output"` / `"batch_error"` for the
    /// files that back a finished batch's results.
    pub purpose: String,
}

/// The `{total, completed, failed}` progress triple embedded in a [`BatchObject`].
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RequestCounts {
    pub total: u64,
    pub completed: u64,
    pub failed: u64,
}

/// The OpenAI **Batch object**. Nullable fields stay present-as-`null` until the
/// batch reaches the relevant lifecycle point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchObject {
    pub id: String,
    /// Always `"batch"`.
    pub object: String,
    pub endpoint: String,
    pub input_file_id: String,
    pub completion_window: String,
    /// `validating` → `in_progress` → `completed`, or `cancelling` → `cancelled`,
    /// or `failed`.
    pub status: String,
    pub output_file_id: Option<String>,
    pub error_file_id: Option<String>,
    pub created_at: i64,
    pub in_progress_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub cancelled_at: Option<i64>,
    pub request_counts: RequestCounts,
}

/// The list envelope OpenAI wraps collections in (`GET /v1/batches`).
#[derive(Debug, Clone, Serialize)]
pub struct ListEnvelope<T> {
    /// Always `"list"`.
    pub object: String,
    pub data: Vec<T>,
}

impl<T> ListEnvelope<T> {
    pub fn new(data: Vec<T>) -> Self {
        Self {
            object: "list".to_string(),
            data,
        }
    }
}

/// The `POST /v1/batches` request body. `metadata` and any extra fields are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateBatchRequest {
    pub input_file_id: String,
    pub endpoint: String,
    #[serde(default)]
    pub completion_window: Option<String>,
}

/// One line of a batch **output** file, in the OpenAI batch-output shape. A success
/// carries `response` (and `error: null`); a failure carries `error` (and
/// `response: null`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputLine {
    /// `batch_req_<hex>` — the per-request id OpenAI assigns to an output row.
    pub id: String,
    pub custom_id: String,
    pub response: Option<OutputResponse>,
    pub error: Option<Value>,
}

/// The `response` object nested in an [`OutputLine`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputResponse {
    pub status_code: u16,
    pub request_id: String,
    pub body: Value,
}

/// Reshape one forge result line (from `out.jsonl` **or** its `.dead.jsonl` sibling)
/// into the OpenAI batch-output line shape. The forge success line carries a
/// `response` object; the dead-letter line carries an `error` object — the presence
/// of one or the other is the discriminator, so a single pass reshapes either file.
///
/// Returns `None` for a line with no `custom_id` (e.g. a crash-torn fragment), which
/// the caller skips — matching how the store's own scans ignore such lines.
pub fn reshape_result_line(line: &str) -> Option<OutputLine> {
    let v: Value = serde_json::from_str(line).ok()?;
    let custom_id = v.get("custom_id")?.as_str()?.to_string();
    let id = format!("batch_req_{}", rand_hex(12));

    if let Some(resp) = v.get("response").filter(|r| !r.is_null()) {
        let status_code = resp
            .get("status_code")
            .and_then(Value::as_u64)
            .unwrap_or(200) as u16;
        // Prefer the engine's own request id if forge captured one; otherwise synthesize
        // a stable-shaped one so the field is never missing.
        let request_id = resp
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("req_{}", rand_hex(12)));
        let body = resp.get("body").cloned().unwrap_or(Value::Null);
        Some(OutputLine {
            id,
            custom_id,
            response: Some(OutputResponse {
                status_code,
                request_id,
                body,
            }),
            error: None,
        })
    } else if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        Some(OutputLine {
            id,
            custom_id,
            response: None,
            error: Some(err.clone()),
        })
    } else {
        // A result line with neither a response nor an error is malformed; surface it as
        // an error row rather than dropping it silently.
        Some(OutputLine {
            id,
            custom_id,
            response: None,
            error: Some(serde_json::json!({ "message": "result had no response or error" })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_object_renders_nullable_fields_as_null() {
        let b = BatchObject {
            id: "batch-abc".into(),
            object: "batch".into(),
            endpoint: "/v1/chat/completions".into(),
            input_file_id: "file-xyz".into(),
            completion_window: "24h".into(),
            status: "in_progress".into(),
            output_file_id: None,
            error_file_id: None,
            created_at: 100,
            in_progress_at: Some(101),
            completed_at: None,
            cancelled_at: None,
            request_counts: RequestCounts {
                total: 3,
                completed: 1,
                failed: 0,
            },
        };
        let v: Value = serde_json::to_value(&b).unwrap();
        // The OpenAI contract: these keys are PRESENT and null, never omitted.
        assert_eq!(v["object"], "batch");
        assert!(v.get("output_file_id").is_some());
        assert!(v["output_file_id"].is_null());
        assert!(v["completed_at"].is_null());
        assert_eq!(v["in_progress_at"], 101);
        assert_eq!(v["request_counts"]["total"], 3);
        assert_eq!(v["request_counts"]["completed"], 1);
    }

    #[test]
    fn file_object_shape() {
        let f = FileObject {
            id: "file-1".into(),
            object: "file".into(),
            bytes: 42,
            created_at: 7,
            filename: "batch.jsonl".into(),
            purpose: "batch".into(),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["object"], "file");
        assert_eq!(v["purpose"], "batch");
        assert_eq!(v["bytes"], 42);
    }

    #[test]
    fn list_envelope_shape() {
        let env = ListEnvelope::new(vec![json!({"id": "batch-1"})]);
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["object"], "list");
        assert_eq!(v["data"][0]["id"], "batch-1");
    }

    #[test]
    fn reshape_success_line_into_openai_shape() {
        // A forge `done` line, exactly as forge-store writes it.
        let line = json!({
            "custom_id": "req-1",
            "response": { "status_code": 200, "body": { "choices": [] } },
            "usage": { "prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4 },
            "worker_id": "w0", "latency_ms": 5, "attempt": 1, "completed_at": 0
        })
        .to_string();
        let out = reshape_result_line(&line).unwrap();
        assert!(out.id.starts_with("batch_req_"));
        assert_eq!(out.custom_id, "req-1");
        assert!(out.error.is_none());
        let resp = out.response.unwrap();
        assert_eq!(resp.status_code, 200);
        assert!(!resp.request_id.is_empty());
        assert_eq!(resp.body["choices"], json!([]));
    }

    #[test]
    fn reshape_dead_line_into_openai_error_shape() {
        // A forge dead-letter line.
        let line = json!({
            "custom_id": "req-2",
            "error": { "code": "worker_error", "message": "always 500" },
            "usage": { "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0 },
            "worker_id": "w0", "latency_ms": 0, "attempt": 5, "completed_at": 0
        })
        .to_string();
        let out = reshape_result_line(&line).unwrap();
        assert_eq!(out.custom_id, "req-2");
        assert!(out.response.is_none());
        let err = out.error.unwrap();
        assert_eq!(err["message"], "always 500");
    }

    #[test]
    fn reshape_skips_line_without_custom_id() {
        assert!(reshape_result_line("{\"oops\": true}").is_none());
        assert!(reshape_result_line("not json").is_none());
    }

    #[test]
    fn create_batch_request_parses_and_defaults_window() {
        let r: CreateBatchRequest = serde_json::from_str(
            r#"{"input_file_id":"file-1","endpoint":"/v1/chat/completions","metadata":{"k":"v"}}"#,
        )
        .unwrap();
        assert_eq!(r.input_file_id, "file-1");
        assert_eq!(r.endpoint, "/v1/chat/completions");
        assert!(r.completion_window.is_none());
    }
}
