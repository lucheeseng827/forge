//! Optional **content** validation for a worker's 2xx response.
//!
//! forge's standout over the closed Batch APIs: a structurally-bad success — an
//! empty generation, or output that isn't valid JSON when the caller asked for
//! structured output — is treated as a *soft failure*. The worker retries it
//! (re-sampling may produce a good output) and, once attempts are exhausted,
//! dead-letters it with a clear reason, **instead of silently emitting bad data**.
//! The closed Batch APIs hand back whatever the model produced; Bedrock batch mode
//! drops structured output entirely. This is opt-in: the default
//! [`ResponseCheck::Any`] is byte-for-byte the old behavior.
//!
//! The check is intentionally lightweight (no JSON-Schema dependency — that lands
//! later, feature-gated, to keep the single static binary lean). It catches the
//! dominant failure mode reported in the field: a 2xx whose *content* is empty or
//! not parseable JSON.

use serde_json::Value;

use crate::types::EndpointKind;

/// What a worker requires of a 2xx response body's **content** before accepting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResponseCheck {
    /// Accept any 2xx body (no content validation). The default — identical to
    /// having no hook at all.
    #[default]
    Any,
    /// The primary output must be present and non-empty: a non-blank assistant
    /// message / completion text, or a non-empty embedding vector.
    NonEmpty,
    /// The primary output **text** must parse as JSON — the structured-output guard.
    /// Strict: markdown-fenced JSON (```json … ```) fails, so instruct the model to
    /// emit raw JSON. For embeddings this degrades to [`NonEmpty`](Self::NonEmpty)
    /// (an embedding is not JSON text).
    Json,
}

impl ResponseCheck {
    /// Validate `body` (a parsed OpenAI-compatible response envelope) for `kind`.
    /// `Ok(())` accepts; `Err(reason)` is a soft failure the worker retries and then
    /// dead-letters.
    pub fn validate(self, kind: EndpointKind, body: &Value) -> Result<(), String> {
        match self {
            ResponseCheck::Any => Ok(()),
            ResponseCheck::NonEmpty => match kind {
                EndpointKind::Embeddings => check_embedding(body),
                EndpointKind::Chat | EndpointKind::Completions | EndpointKind::Messages => {
                    let text = primary_text(kind, body)?;
                    if text.trim().is_empty() {
                        Err("empty output content".into())
                    } else {
                        Ok(())
                    }
                }
            },
            ResponseCheck::Json => match kind {
                EndpointKind::Embeddings => check_embedding(body),
                EndpointKind::Chat | EndpointKind::Completions | EndpointKind::Messages => {
                    let text = primary_text(kind, body)?;
                    if text.trim().is_empty() {
                        return Err("empty output content".into());
                    }
                    serde_json::from_str::<Value>(text)
                        .map(|_| ())
                        .map_err(|e| format!("output is not valid JSON: {e}"))
                }
            },
        }
    }
}

/// The model's primary output text for a chat/completions response.
fn primary_text(kind: EndpointKind, body: &Value) -> Result<&str, String> {
    // Anthropic Messages puts the output at `content[0].text`, not under `choices`.
    if kind == EndpointKind::Messages {
        return body
            .get("content")
            .and_then(|c| c.as_array())
            .and_then(|a| a.first())
            .and_then(|b| b.get("text"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| "response has no content text".to_string());
    }
    let choice = body
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| "response has no choices".to_string())?;
    let text = match kind {
        EndpointKind::Chat => choice.get("message").and_then(|m| m.get("content")),
        EndpointKind::Completions => choice.get("text"),
        EndpointKind::Embeddings | EndpointKind::Messages => None,
    };
    text.and_then(|v| v.as_str())
        .ok_or_else(|| "response choice has no text content".to_string())
}

/// The first embedding vector must be present and non-empty.
fn check_embedding(body: &Value) -> Result<(), String> {
    let len = body
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|e| e.get("embedding"))
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .ok_or_else(|| "response has no embedding data".to_string())?;
    if len == 0 {
        Err("empty embedding vector".into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat(content: Value) -> Value {
        json!({"choices": [{"message": {"role": "assistant", "content": content}}]})
    }

    #[test]
    fn any_accepts_everything() {
        assert!(ResponseCheck::Any
            .validate(EndpointKind::Chat, &json!({}))
            .is_ok());
    }

    #[test]
    fn nonempty_rejects_blank_and_missing() {
        let c = ResponseCheck::NonEmpty;
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!("hello")))
            .is_ok());
        assert!(c.validate(EndpointKind::Chat, &chat(json!("   "))).is_err());
        assert!(c.validate(EndpointKind::Chat, &chat(json!(""))).is_err());
        assert!(c.validate(EndpointKind::Chat, &json!({})).is_err()); // no choices
    }

    #[test]
    fn messages_shape_is_validated_natively() {
        // Anthropic Messages: output at content[0].text, no `choices` envelope.
        let ok = json!({"content": [{"type": "text", "text": "hello"}]});
        let empty = json!({"content": [{"type": "text", "text": "  "}]});
        let jsonish = json!({"content": [{"type": "text", "text": "{\"a\":1}"}]});
        assert!(ResponseCheck::NonEmpty
            .validate(EndpointKind::Messages, &ok)
            .is_ok());
        assert!(ResponseCheck::NonEmpty
            .validate(EndpointKind::Messages, &empty)
            .is_err());
        assert!(ResponseCheck::Json
            .validate(EndpointKind::Messages, &jsonish)
            .is_ok());
        assert!(ResponseCheck::Json
            .validate(EndpointKind::Messages, &ok)
            .is_err());
    }

    #[test]
    fn json_requires_parseable_content() {
        let c = ResponseCheck::Json;
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!(r#"{"ok": true}"#)))
            .is_ok());
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!("[1, 2, 3]")))
            .is_ok());
        // prose / fenced JSON / truncated objects all fail
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!("sure! here you go")))
            .is_err());
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!("```json\n{}\n```")))
            .is_err());
        assert!(c
            .validate(EndpointKind::Chat, &chat(json!(r#"{"ok": tru"#)))
            .is_err());
    }

    #[test]
    fn completions_uses_text_field() {
        let body = json!({"choices": [{"text": "{\"a\":1}"}]});
        assert!(ResponseCheck::Json
            .validate(EndpointKind::Completions, &body)
            .is_ok());
        let empty = json!({"choices": [{"text": ""}]});
        assert!(ResponseCheck::NonEmpty
            .validate(EndpointKind::Completions, &empty)
            .is_err());
    }

    #[test]
    fn embeddings_check_vector_presence() {
        let ok = json!({"data": [{"embedding": [0.1, -0.2]}]});
        let empty = json!({"data": [{"embedding": []}]});
        let missing = json!({"data": []});
        // Json degrades to NonEmpty for embeddings.
        for c in [ResponseCheck::NonEmpty, ResponseCheck::Json] {
            assert!(c.validate(EndpointKind::Embeddings, &ok).is_ok());
            assert!(c.validate(EndpointKind::Embeddings, &empty).is_err());
            assert!(c.validate(EndpointKind::Embeddings, &missing).is_err());
        }
    }
}
