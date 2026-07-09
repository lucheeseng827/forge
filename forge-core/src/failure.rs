//! Classify a dead-letter `last_error` into a coarse **reason** so `forge status`
//! can show *why* items failed — the observability signal batch users ask for
//! ("alert when the failure mix changes"). The categories are derived from the
//! error strings the worker produces (see `forge-worker`): a terminal 4xx, an
//! exhausted-retry 5xx/429, a request/connection error, or a content-validation
//! soft-failure.

/// A coarse failure category for a dead-lettered item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FailureKind {
    /// The response content failed a `--require` check (empty / non-JSON).
    Validation,
    /// The engine returned HTTP 429 (over its rate/queue limit).
    RateLimited,
    /// A request timeout (HTTP 408 or a client-side timed-out request).
    Timeout,
    /// A connection-level error (refused / reset / DNS) — the endpoint was down.
    Connection,
    /// A non-retryable 4xx (bad request / auth / not found / unprocessable).
    ClientError,
    /// An exhausted-retry 5xx (the engine kept erroring).
    ServerError,
    /// Anything that didn't match a known pattern.
    Other,
}

impl FailureKind {
    /// Stable lowercase token for reports / metric labels.
    pub fn as_str(self) -> &'static str {
        match self {
            FailureKind::Validation => "validation",
            FailureKind::RateLimited => "rate_limited",
            FailureKind::Timeout => "timeout",
            FailureKind::Connection => "connection",
            FailureKind::ClientError => "client_error",
            FailureKind::ServerError => "server_error",
            FailureKind::Other => "other",
        }
    }
}

/// Classify a dead-letter error string into a [`FailureKind`]. Best-effort and
/// order-sensitive. HTTP status codes are matched **in their `HTTP <code>` context**
/// (not as bare substrings) so a retry count like `after 429 attempt(s)` can't be read
/// as a 429, and a 5xx wins over an incidental status word — e.g. `HTTP 504 Gateway
/// Timeout` is a `ServerError`, not a `Timeout`. Order: validation (can wrap any 2xx)
/// → rate-limit → the 408 timeout status → 5xx → connection/transport (a transport
/// timeout here is a `Timeout`) → other 4xx → a bare client-side timeout → other.
pub fn classify_failure(error: &str) -> FailureKind {
    let e = error.to_ascii_lowercase();
    let transport = e.contains("request error")
        || e.contains("connect")
        || e.contains("connection")
        || e.contains("refused")
        || e.contains("reset")
        || e.contains("dns");
    let timeoutish = e.contains("timed out") || e.contains("timeout");

    if e.contains("validation failed") {
        FailureKind::Validation
    } else if e.contains("http 429") || e.contains("too many requests") {
        FailureKind::RateLimited
    } else if e.contains("http 408") {
        FailureKind::Timeout
    } else if e.contains("http 5") {
        // A 5xx wins over any incidental "timeout" word (e.g. 504 Gateway Timeout).
        FailureKind::ServerError
    } else if transport {
        // Client-side transport failure: a timed-out request is a Timeout, otherwise
        // it's a connection-level error.
        if timeoutish {
            FailureKind::Timeout
        } else {
            FailureKind::Connection
        }
    } else if e.contains("(terminal)") || e.contains("http 4") {
        FailureKind::ClientError
    } else if timeoutish {
        // A bare client-side timeout with no HTTP/transport prefix.
        FailureKind::Timeout
    } else {
        FailureKind::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_worker_error_strings() {
        // The actual shapes forge-worker emits, wrapped by ForgeError::Worker.
        let cases = [
            (
                "worker: retries exhausted after 3 attempt(s): validation failed: output is not valid JSON: ...",
                FailureKind::Validation,
            ),
            (
                "worker: retries exhausted after 5 attempt(s): HTTP 429 Too Many Requests",
                FailureKind::RateLimited,
            ),
            (
                "worker: retries exhausted after 5 attempt(s): HTTP 500 Internal Server Error",
                FailureKind::ServerError,
            ),
            (
                "worker: HTTP 400 (terminal): bad request",
                FailureKind::ClientError,
            ),
            (
                "worker: HTTP 404 (terminal): not found",
                FailureKind::ClientError,
            ),
            (
                "worker: retries exhausted after 5 attempt(s): request error: connection refused",
                FailureKind::Connection,
            ),
            (
                "worker: retries exhausted after 5 attempt(s): request error: operation timed out",
                FailureKind::Timeout,
            ),
            ("something unexpected", FailureKind::Other),
        ];
        for (s, want) in cases {
            assert_eq!(classify_failure(s), want, "for {s:?}");
        }
    }

    #[test]
    fn validation_wins_over_http_code() {
        // A validation failure mentioning a status must still classify as validation.
        assert_eq!(
            classify_failure("validation failed: output is not valid JSON (HTTP 200)"),
            FailureKind::Validation
        );
    }

    #[test]
    fn http_status_matched_in_context_not_as_bare_substring() {
        // 504 is a 5xx, not a Timeout, despite the word "Timeout".
        assert_eq!(
            classify_failure(
                "worker: retries exhausted after 5 attempt(s): HTTP 504 Gateway Timeout"
            ),
            FailureKind::ServerError
        );
        // 408 IS a timeout status.
        assert_eq!(
            classify_failure(
                "worker: retries exhausted after 5 attempt(s): HTTP 408 Request Timeout"
            ),
            FailureKind::Timeout
        );
        // A retry-count of 429 must not be read as a rate-limit; the real 500 wins.
        assert_eq!(
            classify_failure(
                "worker: retries exhausted after 429 attempt(s): HTTP 500 Internal Server Error"
            ),
            FailureKind::ServerError
        );
    }
}
