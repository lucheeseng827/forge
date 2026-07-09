//! forge-proto — the **coordinator ↔ agent** wire contract.
//!
//! When the optional [`forge-agent`] is co-located with an engine on a spot
//! box, it talks to the single-writer coordinator with exactly **three** messages:
//!
//! 1. [`LeasePull`] — agent → coordinator: "lease me up to `max` items."
//! 2. [`LeaseGrant`] — coordinator → agent: the leased batch (possibly empty).
//! 3. [`ResultPost`] — agent → coordinator: one finished item (success or terminal).
//! 4. [`InterruptionNotice`] — agent → coordinator: "I caught a spot notice; I'm
//!    draining." Purely **advisory** — the coordinator re-queues on lease expiry
//!    regardless, so a dropped notice changes nothing.
//!
//! **Doctrine (baked into this crate's shape):** there are **no task-graph types**
//! here — no `depends_on`, no fan-in, no DAG. That is the dagron boundary
//! (ARCHITECTURE §1/§2). This is a flat lease-proxy protocol and nothing more; the
//! coordinator stays the single writer (the agent only *proposes* results, never
//! mutates the queue itself). Keeping the contract this small is deliberate — it
//! exists only so the agent can hand the ~30s in-VM spot window back to the
//! coordinator without duplicating dagron's plumbing.
//!
//! These are pure serde types with zero logic. The mapping to/from the in-process
//! [`forge_core`] model lives in `forge-agent` (the only crate that needs both).

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// agent → coordinator. Request up to `max` items for `worker_id`, each to be leased
/// for `lease_secs` (the visibility timeout the coordinator stamps).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePull {
    /// Stable id of the engine the agent fronts (the lease owner / fencing key).
    pub worker_id: String,
    /// Upper bound on the batch size — the agent sizes this to its declared
    /// concurrency so it never holds more leases than it can run.
    pub max: u32,
    /// Visibility timeout for the granted leases, in seconds. The coordinator owns
    /// the clock; this is the agent's request.
    pub lease_secs: u64,
}

/// One leased work item on the wire. A minimal projection of the coordinator's item
/// row — only what the agent needs to (a) replay the request and (b) fence the
/// result. **No `depends_on` / graph edges**, by doctrine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireItem {
    /// Identity + idempotency/dedup key + resume key, all at once.
    pub custom_id: String,
    /// HTTP method (almost always `POST`).
    pub method: String,
    /// Request path; empty → the worker's endpoint default.
    pub url: String,
    /// The request payload, passed through to the engine verbatim.
    pub body: serde_json::Value,
    /// The lease generation the coordinator stamped — echoed back in [`ResultPost`]
    /// so a stale agent (whose lease already expired and was re-leased elsewhere)
    /// can't close an item it no longer owns.
    pub lease_generation: u64,
    /// Attempts already spent on this item before this lease.
    #[serde(default)]
    pub attempts: u32,
}

/// coordinator → agent. The granted lease batch; an empty `items` means "nothing
/// ready right now" (the agent backs off and polls again).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseGrant {
    pub items: Vec<WireItem>,
}

impl LeaseGrant {
    /// An empty grant (nothing ready).
    pub fn empty() -> Self {
        Self { items: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Token counts captured from the engine response (mirrors the OpenAI `usage`
/// object). Carried on the wire so the coordinator's stored result — and therefore
/// `forge cost` — stays faithful across the network hop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireUsage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

/// The terminal disposition of one item the agent ran.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireOutcome {
    /// A validated 2xx response. Carries everything the coordinator needs to write a
    /// *faithful* stored result — the raw body plus the captured `usage` and timing —
    /// so the network path loses none of the cost/observability signal.
    Done {
        status_code: u16,
        response: serde_json::Value,
        #[serde(default)]
        usage: WireUsage,
        #[serde(default)]
        latency_ms: u64,
    },
    /// Retries exhausted (or a terminal 4xx / failed content check) → dead-letter.
    DeadLetter { error: String },
}

/// agent → coordinator. One finished item, fenced by `lease_generation`. The
/// coordinator stores-then-acks (the exactly-once-*effect* ordering lives on the
/// coordinator, not here): a post is an idempotent proposal, safe to retry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultPost {
    pub custom_id: String,
    pub lease_generation: u64,
    pub worker_id: String,
    pub outcome: WireOutcome,
}

/// coordinator → agent. The reply to a [`ResultPost`]: whether the lease-fenced
/// transition actually applied. `false` is a **harmless no-op** — the lease was stale
/// (expired and re-leased elsewhere) and the store write already de-duplicated — so
/// the agent treats both as success and moves on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckReply {
    pub acked: bool,
}

/// agent → coordinator. A best-effort heads-up that this box caught a spot/preempt
/// notice and has stopped pulling new leases. **Advisory only** — correctness rides
/// on lease expiry + idempotent writes, never on this arriving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterruptionNotice {
    pub worker_id: String,
    /// Cloud the notice came from (`aws` / `gcp` / `azure`).
    pub cloud: String,
    /// Notice kind (`terminate` / `rebalance`).
    pub kind: String,
    /// The cloud's own deadline (raw ISO-8601), when it provides one — advisory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip<T>(v: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let s = serde_json::to_string(v).expect("serialize");
        serde_json::from_str(&s).expect("deserialize")
    }

    #[test]
    fn lease_pull_roundtrips() {
        let p = LeasePull {
            worker_id: "gpu1".into(),
            max: 64,
            lease_secs: 120,
        };
        assert_eq!(p, roundtrip(&p));
    }

    #[test]
    fn lease_grant_roundtrips_with_items() {
        let g = LeaseGrant {
            items: vec![WireItem {
                custom_id: "req-1".into(),
                method: "POST".into(),
                url: "/v1/chat/completions".into(),
                body: serde_json::json!({"model": "m", "messages": []}),
                lease_generation: 7,
                attempts: 1,
            }],
        };
        assert_eq!(g, roundtrip(&g));
        assert!(!g.is_empty());
        assert!(LeaseGrant::empty().is_empty());
    }

    #[test]
    fn wire_item_attempts_defaults_to_zero() {
        // A coordinator that omits `attempts` (fresh item) still parses.
        let raw = r#"{"custom_id":"c","method":"POST","url":"","body":{},"lease_generation":1}"#;
        let it: WireItem = serde_json::from_str(raw).expect("parse");
        assert_eq!(it.attempts, 0);
    }

    #[test]
    fn result_post_outcomes_roundtrip() {
        let done = ResultPost {
            custom_id: "c".into(),
            lease_generation: 3,
            worker_id: "gpu1".into(),
            outcome: WireOutcome::Done {
                status_code: 200,
                response: serde_json::json!({"choices": []}),
                usage: WireUsage {
                    prompt_tokens: 11,
                    completion_tokens: 22,
                    total_tokens: 33,
                },
                latency_ms: 1234,
            },
        };
        assert_eq!(done, roundtrip(&done));

        let dead = ResultPost {
            custom_id: "c".into(),
            lease_generation: 3,
            worker_id: "gpu1".into(),
            outcome: WireOutcome::DeadLetter {
                error: "HTTP 500 after 5 attempts".into(),
            },
        };
        assert_eq!(dead, roundtrip(&dead));
    }

    #[test]
    fn usage_defaults_when_omitted_and_ack_reply_roundtrips() {
        // A coordinator/agent that omits usage/latency (older peer) still parses.
        let raw = r#"{"custom_id":"c","lease_generation":1,"worker_id":"w","outcome":{"done":{"status_code":200,"response":{}}}}"#;
        let p: ResultPost = serde_json::from_str(raw).expect("parse");
        match p.outcome {
            WireOutcome::Done {
                usage, latency_ms, ..
            } => {
                assert_eq!(usage, WireUsage::default());
                assert_eq!(latency_ms, 0);
            }
            _ => panic!("expected done"),
        }
        let a = AckReply { acked: true };
        assert_eq!(a, roundtrip(&a));
    }

    #[test]
    fn interruption_notice_roundtrips_and_omits_absent_deadline() {
        let n = InterruptionNotice {
            worker_id: "gpu1".into(),
            cloud: "gcp".into(),
            kind: "terminate".into(),
            deadline: None,
        };
        assert_eq!(n, roundtrip(&n));
        let s = serde_json::to_string(&n).unwrap();
        assert!(
            !s.contains("deadline"),
            "absent deadline must be omitted: {s}"
        );
    }
}
