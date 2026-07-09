//! End-to-end: hydrate a queue, fan it across a mock OpenAI-compatible engine,
//! and assert results land in the store. Exercises the whole core pipeline
//! (forge-core loop + forge-queue + forge-worker + forge-store) over real HTTP.

use std::time::Duration;

use forge_core::{
    BatchRun, EndpointKind, Item, ItemState, Queue, RetryPolicy, RunConfig, WorkerSpec,
};
use forge_queue::SqliteQueue;
use forge_store::JsonlStore;
use forge_worker::HttpWorker;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn item(i: usize) -> Item {
    Item {
        custom_id: format!("req-{i}"),
        method: "POST".into(),
        url: "/v1/chat/completions".into(),
        body: json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
        status: ItemState::Pending,
        attempts: 0,
        leased_until: None,
        leased_by: None,
        last_error: None,
    }
}

fn fast_cfg() -> RunConfig {
    RunConfig {
        lease_for: Duration::from_secs(30),
        lease_max: Duration::from_secs(30),
        lease_batch: 16,
        poll_interval: Duration::from_millis(10),
        ready_grace: Duration::from_secs(5),
        retry: RetryPolicy {
            max_attempts: 2,
            base: Duration::from_millis(1),
            cap: Duration::from_millis(2),
        },
        load_aware: true,
    }
}

#[tokio::test]
async fn end_to_end_run_processes_jsonl() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
        })))
        .mount(&server)
        .await;

    let queue = SqliteQueue::open_in_memory().unwrap();
    let items: Vec<Item> = (0..3).map(item).collect();
    assert_eq!(queue.enqueue(&items).await.unwrap(), 3);

    let out = std::env::temp_dir().join(format!("forge-e2e-{}-ok.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&out);
    let store = JsonlStore::new(&out);
    let worker =
        HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(4))
            .unwrap();

    let totals = BatchRun::new(queue, vec![worker], store)
        .with_config(fast_cfg())
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done, 3);
    assert_eq!(totals.items_dead, 0);
    assert_eq!(totals.tokens_total(), 21); // 3 × 7

    let content = std::fs::read_to_string(&out).unwrap();
    assert_eq!(content.lines().count(), 3);
    assert!(content.contains("req-0") && content.contains("req-2"));
    let _ = std::fs::remove_file(&out);
}

#[tokio::test]
async fn end_to_end_poison_item_is_dead_lettered() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // Every inference request is a terminal 400 → all items dead-letter, no hang.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;

    let queue = SqliteQueue::open_in_memory().unwrap();
    queue.enqueue(&[item(0), item(1)]).await.unwrap();

    let out = std::env::temp_dir().join(format!("forge-e2e-{}-dead.jsonl", std::process::id()));
    let dead = std::path::PathBuf::from(format!("{}.dead.jsonl", out.display()));
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(&dead);

    let store = JsonlStore::new(&out);
    let worker =
        HttpWorker::new(WorkerSpec::new("w0", server.uri(), EndpointKind::Chat).concurrency(2))
            .unwrap();

    let totals = BatchRun::new(queue, vec![worker], store)
        .with_config(fast_cfg())
        .run()
        .await
        .unwrap();

    assert_eq!(totals.items_done, 0);
    assert_eq!(totals.items_dead, 2);
    assert!(!out.exists() || std::fs::read_to_string(&out).unwrap().lines().count() == 0);
    assert_eq!(std::fs::read_to_string(&dead).unwrap().lines().count(), 2);
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(&dead);
}
