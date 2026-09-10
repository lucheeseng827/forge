//! Request correlation and error redaction — the two halves of being able to answer
//! "what happened to my request?" without telling the internet how the server is laid
//! out.
//!
//! A 5xx body goes to the **customer**. Interpolating the underlying error into it
//! hands them absolute filesystem paths, tenant directory layout and engine URLs — and
//! it is also the least useful place to put that detail, because they cannot act on it.
//! The operator needs the detail; the customer needs a handle. So the detail is logged
//! against a **request id** and the body carries only that id.

use std::cell::RefCell;

thread_local! {
    /// The request id in scope on this thread.
    ///
    /// A thread-local rather than a parameter threaded through ~50 response call sites:
    /// both servers are `tiny_http` accept threads that handle one request at a time to
    /// completion, so "the current thread" and "the current request" are the same thing.
    /// Threading it by hand would be equivalent, plus one chance per call site to forget.
    static CURRENT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Restores the previous request id when dropped, so a nested or early-returning
/// handler can never leave a stale id bound for the next request on this thread.
pub struct RequestIdGuard(Option<String>);

impl Drop for RequestIdGuard {
    fn drop(&mut self) {
        let prev = self.0.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}

/// Bind `id` as this thread's request id until the guard drops.
#[must_use = "the id is unbound as soon as the guard drops"]
pub fn scope_request_id(id: String) -> RequestIdGuard {
    let prev = CURRENT.with(|c| c.borrow_mut().replace(id));
    RequestIdGuard(prev)
}

/// The request id in scope, if any.
pub fn current_request_id() -> Option<String> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Longest inbound id we will echo. Long enough for a UUID or a trace id, short enough
/// that it cannot be used to pump arbitrary volume into the log stream.
const MAX_INBOUND_ID: usize = 64;

/// Accept a caller-supplied request id, or mint one.
///
/// **The inbound value is sanitised, not trusted.** It is written into a response
/// header and into every log line for the request, so an unfiltered value is both a
/// header-injection vector (a `\r\n` splits the response) and a log-injection one (a
/// newline forges a log record, which is how an attacker manufactures evidence). Only
/// `[A-Za-z0-9._-]` survives, capped at 64 bytes; anything that empties out gets a
/// fresh minted id instead.
///
/// Echoing at all is worth it: a caller that already has a trace id can join forge's
/// logs to the rest of their system, which is the whole point of accepting one.
pub fn accept_request_id(inbound: Option<&str>) -> String {
    if let Some(raw) = inbound {
        let cleaned: String = raw
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            .take(MAX_INBOUND_ID)
            .collect();
        if !cleaned.is_empty() {
            return cleaned;
        }
    }
    mint_request_id()
}

/// A fresh request id: `req_` + 16 hex chars.
pub fn mint_request_id() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut buf);
    let mut s = String::with_capacity(20);
    s.push_str("req_");
    for b in buf {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The body for a 5xx: a fixed sentence plus the id that will find the detail.
///
/// Deliberately says nothing about *what* failed. "could not persist upload:
/// /srv/forge/tenants/acme/job-7/in.jsonl: No space left on device" tells a customer
/// your directory layout, your tenant naming and your disk state; it tells them nothing
/// they can do. The id does.
pub fn internal_error_body(request_id: &str) -> String {
    // Built with serde rather than by hand: this is `pub`, so the caller's id is not
    // guaranteed to have been through `accept_request_id`, and a `"` or `\\` in it would
    // otherwise emit a malformed body. The invariant belongs in the code, not in a
    // convention about who calls it.
    serde_json::json!({
        "error": {
            "type": "internal_error",
            "message": "the server could not complete this request",
        },
        "request_id": request_id,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_id_is_prefixed_and_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert!(a.starts_with("req_"), "{a}");
        assert_eq!(a.len(), 20);
        assert_ne!(a, b);
    }

    #[test]
    fn an_inbound_id_is_echoed_when_it_is_safe() {
        assert_eq!(
            accept_request_id(Some("0af7651916cd43dd8448eb211c80319c")),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(accept_request_id(Some("trace-1.2_3")), "trace-1.2_3");
    }

    #[test]
    fn header_and_log_injection_cannot_ride_in_on_a_request_id() {
        // The value lands in a response header and in every log line for the request.
        // A CRLF splits the response; a newline forges a log record.
        let split = accept_request_id(Some("abc\r\nSet-Cookie: admin=1"));
        assert!(!split.contains('\r') && !split.contains('\n'), "{split}");
        assert_eq!(split, "abcSet-Cookieadmin1");

        let forged = accept_request_id(Some("x\n{\"level\":\"INFO\",\"msg\":\"all good\"}"));
        assert!(!forged.contains('\n'), "{forged}");

        // Nothing usable at all → a fresh id, never an empty header (an empty
        // `x-request-id` is worse than none: it looks like a real correlation handle).
        for hostile in ["\r\n\t   ", "", "!!!", "\u{0}"] {
            let id = accept_request_id(Some(hostile));
            assert!(id.starts_with("req_"), "{hostile:?} -> {id}");
            assert_eq!(id.len(), 20);
        }
    }

    #[test]
    fn an_absurdly_long_id_is_truncated() {
        let long = "a".repeat(4096);
        assert_eq!(accept_request_id(Some(&long)).len(), MAX_INBOUND_ID);
    }

    #[test]
    fn the_scope_restores_the_previous_id_on_drop() {
        assert_eq!(current_request_id(), None);
        {
            let _outer = scope_request_id("req_outer".into());
            assert_eq!(current_request_id().as_deref(), Some("req_outer"));
            {
                let _inner = scope_request_id("req_inner".into());
                assert_eq!(current_request_id().as_deref(), Some("req_inner"));
            }
            assert_eq!(current_request_id().as_deref(), Some("req_outer"));
        }
        assert_eq!(
            current_request_id(),
            None,
            "a stale id must never survive into the next request on this thread"
        );
    }

    #[test]
    fn an_internal_error_body_carries_the_id_and_nothing_else() {
        let body = internal_error_body("req_abc123");
        assert!(body.contains("req_abc123"));
        // The shape a client can rely on…
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"]["type"], "internal_error");
        assert_eq!(v["request_id"], "req_abc123");
        // …and nothing that describes the server.
        for leak in ["/", "\\", "No such file", "denied"] {
            assert!(!v["error"]["message"].as_str().unwrap().contains(leak));
        }
    }
}
