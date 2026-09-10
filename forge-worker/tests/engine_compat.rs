//! Engine compatibility matrix — one test per response shape forge claims to support.
//!
//! **What this is and is not.** These pin the *wire contract*: for each engine named
//! in `docs/API.md`, forge is driven against that engine's documented response shape
//! and asserted to extract the right content and the right token counts. They are a
//! regression guard, and they are the reason the matrix in the docs can say
//! "shape-verified" for a row.
//!
//! They are **not** a substitute for running against the real thing. A recorded shape
//! cannot catch an engine that changed its response, a gateway that rewrites headers,
//! or throughput behaviour under load. `docs/API.md` marks which rows have been run
//! against real hardware — currently none — and that distinction is deliberate: a
//! buyer evaluating forge will ask, and "we tested against a mock" is the honest
//! answer until a real run is published.

use forge_core::{EndpointKind, EngineHint, Item, ItemState, Worker, WorkerSpec};
use forge_worker::HttpWorker;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn item(url: &str) -> Item {
    Item {
        custom_id: "req-1".into(),
        method: "POST".into(),
        url: url.into(),
        body: json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
        status: ItemState::Pending,
        attempts: 0,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

async fn worker(server: &MockServer, hint: EngineHint, kind: EndpointKind) -> HttpWorker {
    HttpWorker::new(WorkerSpec::new("w", server.uri(), kind).engine_hint(hint)).unwrap()
}

/// vLLM and SGLang both serve the stock OpenAI chat shape; this is the baseline every
/// other row is a deviation from.
#[tokio::test]
async fn openai_chat_shape_vllm_and_sglang() {
    for hint in [EngineHint::Vllm, EngineHint::Sglang] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "chatcmpl-1",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}}],
                "usage": {"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16},
            })))
            .mount(&server)
            .await;
        let r = worker(&server, hint, EndpointKind::Chat)
            .await
            .submit(&item("/v1/chat/completions"))
            .await
            .unwrap();
        assert_eq!(r.usage.prompt_tokens, 11, "{hint:?}");
        assert_eq!(r.usage.total_tokens, 16, "{hint:?}");
        assert_eq!(
            r.response.unwrap().body["choices"][0]["message"]["content"],
            "hello"
        );
    }
}

/// llama.cpp's NATIVE route: no `usage` object at all, counts at the top level under
/// its own names. This is the shape that silently billed zero before.
#[tokio::test]
async fn llamacpp_native_completion_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/completion"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": "hello",
            "tokens_evaluated": 42,
            "tokens_predicted": 8,
            "stop": true,
        })))
        .mount(&server)
        .await;
    let r = worker(&server, EngineHint::LlamaCpp, EndpointKind::Completions)
        .await
        .submit(&item("/completion"))
        .await
        .unwrap();
    assert_eq!((r.usage.prompt_tokens, r.usage.completion_tokens), (42, 8));
    assert_eq!(r.usage.total_tokens, 50, "derived when the engine omits it");
}

/// llama.cpp also serves an OpenAI-compatible route; both must work from one binary.
#[tokio::test]
async fn llamacpp_openai_compatible_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "hi"}}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
        })))
        .mount(&server)
        .await;
    let r = worker(&server, EngineHint::LlamaCpp, EndpointKind::Chat)
        .await
        .submit(&item("/v1/chat/completions"))
        .await
        .unwrap();
    assert_eq!(r.usage.total_tokens, 5);
}

/// A hosted OpenAI-compatible API: the key must arrive as a Bearer header, and the
/// health probe must be skipped (hosted endpoints expose none).
#[tokio::test]
async fn hosted_openai_api_sends_bearer_and_skips_the_probe() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer sk-compat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        })))
        .mount(&server)
        .await;
    // Per-worker key, so this test cannot be perturbed by a stray global one.
    std::env::set_var("FORGE_WORKER_API_KEY_HOSTED", "sk-compat");
    let w = HttpWorker::new(
        WorkerSpec::new("hosted", server.uri(), EndpointKind::Chat)
            .engine_hint(EngineHint::OpenAiApi),
    )
    .unwrap();
    let r = w.submit(&item("/v1/chat/completions")).await.unwrap();
    assert_eq!(r.usage.total_tokens, 2);
    std::env::remove_var("FORGE_WORKER_API_KEY_HOSTED");
}

/// Anthropic Messages: different auth headers, different usage field names, and no
/// total — it has to price identically to every other provider in `forge cost`.
#[tokio::test]
async fn anthropic_messages_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-ant"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "text", "text": "hello"}],
            "usage": {"input_tokens": 20, "output_tokens": 7},
        })))
        .mount(&server)
        .await;
    std::env::set_var("FORGE_WORKER_API_KEY_ANT", "sk-ant");
    let w = HttpWorker::new(
        WorkerSpec::new("ant", server.uri(), EndpointKind::Messages)
            .engine_hint(EngineHint::AnthropicApi),
    )
    .unwrap();
    let r = w.submit(&item("/v1/messages")).await.unwrap();
    assert_eq!(r.usage.prompt_tokens, 20);
    assert_eq!(r.usage.completion_tokens, 7);
    assert_eq!(r.usage.total_tokens, 27, "Anthropic reports no total");
    std::env::remove_var("FORGE_WORKER_API_KEY_ANT");
}

/// Embeddings: the marquee batch workload, and the shape the Parquet vector column
/// reads. If this response shape changes, that column silently stops being populated.
#[tokio::test]
async fn embeddings_shape_feeds_the_token_ledger() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2, 0.3]}],
            "model": "bge-m3",
            "usage": {"prompt_tokens": 6, "total_tokens": 6},
        })))
        .mount(&server)
        .await;
    let r = worker(&server, EngineHint::Vllm, EndpointKind::Embeddings)
        .await
        .submit(&item("/v1/embeddings"))
        .await
        .unwrap();
    assert_eq!(r.usage.prompt_tokens, 6);
    let body = r.response.unwrap().body;
    assert_eq!(body["data"][0]["embedding"].as_array().unwrap().len(), 3);
}
