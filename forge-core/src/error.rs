//! The one error type the whole coordinator speaks.

/// Errors surfaced by the queue, workers, store, ingest, and the run loop.
///
/// Backend crates wrap their native errors into the `String`-carrying variants so
/// `forge-core` stays free of backend dependencies.
#[derive(Debug, thiserror::Error)]
pub enum ForgeError {
    #[error("queue: {0}")]
    Queue(String),

    #[error("worker: {0}")]
    Worker(String),

    #[error("store: {0}")]
    Store(String),

    #[error("ingest: {0}")]
    Ingest(String),

    #[error("config: {0}")]
    Config(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}
