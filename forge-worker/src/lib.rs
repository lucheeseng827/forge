//! forge-worker — the [`Worker`] over a black-box OpenAI-compatible HTTP engine.
//!
//! One persistent keep-alive [`reqwest::Client`] per `base_url`; an **AIMD
//! adaptive** in-flight limiter ceilinged at the engine's own concurrency cap (so
//! forge never oversubscribes into an OOM, and backs off when the engine signals
//! overload — converging on real throughput without hand-tuning); and
//! backoff-with-full-jitter retry owned here — [`Worker::submit`] returns `Err`
//! only once the [`RetryPolicy`] is exhausted, so the loop sees terminal
//! success/failure. [`Worker::probe`] does a health-`GET` and caches readiness.
//!
//! TLS is `rustls` (so `https://` engines work and the static-musl build stays
//! pure-Rust); see the workspace `reqwest` features.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use forge_core::{
    now_ms, EndpointKind, EngineHint, ForgeError, Item, ItemResponse, ItemResult, ResponseCheck,
    RetryPolicy, Worker, WorkerSpec,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Sentinel stored in [`HttpWorker`]'s cached load when the engine's queue depth is
/// unknown (no metric endpoint, or the fetch failed). Maps back to `None`.
const LOAD_UNKNOWN: u32 = u32::MAX;

/// The direct, un-keyed GCRA rate limiter (a single global token bucket, no per-key
/// state). Only compiled with the `governor` feature.
#[cfg(feature = "governor")]
type WorkerRateLimiter = governor::DefaultDirectRateLimiter;

/// Additive increase: grow the effective in-flight cap by 1 after this many
/// consecutive successes (gentle, TCP-congestion-control style).
const AIMD_INCREASE_AFTER: usize = 8;

/// AIMD adaptive in-flight limiter (the §7 backpressure control). Starts at the
/// declared ceiling and **multiplicatively decreases** on a transient overload
/// signal (429 / 5xx / timeout), then **additively increases** back toward the
/// ceiling on a streak of successes — converging on the engine's real throughput
/// **without hand-tuning**. Bounded to `[floor, ceiling]`, so it never oversubscribes
/// past the worker's `concurrency_limit`.
///
/// Built on a `tokio::Semaphore`: a decrease that can't immediately reclaim in-flight
/// permits records a `debt` that is paid down as those permits return (a permit owed
/// to the debt is *forgotten* rather than returned to the pool). Invariant:
/// `available + in_flight == effective + debt`.
struct AimdLimit {
    sem: Arc<Semaphore>,
    inner: Mutex<AimdInner>,
    ceiling: usize,
    floor: usize,
}

struct AimdInner {
    effective: usize,
    debt: usize,
    streak: usize,
}

impl AimdLimit {
    fn new(ceiling: usize) -> Arc<Self> {
        let ceiling = ceiling.max(1);
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(ceiling)),
            inner: Mutex::new(AimdInner {
                effective: ceiling,
                debt: 0,
                streak: 0,
            }),
            ceiling,
            floor: 1,
        })
    }

    /// Acquire one in-flight slot, waiting if the effective cap is reached. The
    /// returned guard pays down `debt` (if any) when it drops.
    async fn acquire(self: &Arc<Self>) -> Result<AimdPermit, ForgeError> {
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .map_err(|e| ForgeError::Worker(format!("semaphore closed: {e}")))?;
        Ok(AimdPermit {
            limit: Arc::clone(self),
            permit: Some(permit),
        })
    }

    /// Record one request's congestion signal: `true` for a clean 2xx (additive
    /// increase after a streak), `false` for a transient overload (multiplicative
    /// decrease). Terminal 4xx / content-validation failures are NOT congestion and
    /// must not be recorded.
    fn record(&self, success: bool) {
        let mut st = self.inner.lock().unwrap();
        if success {
            st.streak += 1;
            if st.streak >= AIMD_INCREASE_AFTER && st.debt == 0 && st.effective < self.ceiling {
                st.effective += 1;
                st.streak = 0;
                self.sem.add_permits(1);
            }
        } else {
            st.streak = 0;
            let target = (st.effective / 2).max(self.floor);
            st.debt += st.effective - target;
            st.effective = target;
            // Forget as many free permits as we can now; the rest are paid as
            // in-flight permits return (AimdPermit::drop).
            while st.debt > 0 {
                match self.sem.try_acquire() {
                    Ok(p) => {
                        p.forget();
                        st.debt -= 1;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    #[cfg(test)]
    fn effective(&self) -> usize {
        self.inner.lock().unwrap().effective
    }
    #[cfg(test)]
    fn debt(&self) -> usize {
        self.inner.lock().unwrap().debt
    }
}

/// An in-flight slot from an [`AimdLimit`]. On drop it returns the permit to the
/// pool — unless there is outstanding `debt`, in which case it is forgotten (paying
/// the debt) so the multiplicative decrease actually takes effect.
struct AimdPermit {
    limit: Arc<AimdLimit>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for AimdPermit {
    fn drop(&mut self) {
        let mut st = self.limit.inner.lock().unwrap();
        if st.debt > 0 {
            st.debt -= 1;
            if let Some(p) = self.permit.take() {
                p.forget();
            }
        }
        // else: the OwnedSemaphorePermit drops normally, returning to the pool.
    }
}

/// A worker bound to one OpenAI-compatible endpoint.
#[derive(Clone)]
pub struct HttpWorker {
    spec: WorkerSpec,
    client: reqwest::Client,
    /// AIMD-adaptive in-flight limiter, ceilinged at `spec.concurrency_limit`
    /// (= the engine's own `max-num-seqs` / `--max-running-requests` / `--parallel`).
    inflight: Arc<AimdLimit>,
    /// Cached readiness; updated by [`Worker::probe`]. Optimistic at start so a run
    /// can begin before the first probe lands.
    ready: Arc<AtomicBool>,
    /// Cached engine queue depth for **load-aware dispatch** (B3); `LOAD_UNKNOWN`
    /// until/unless a probe reads the engine's metric. Lower = less loaded.
    load: Arc<AtomicU32>,
    /// Optional **global request-rate ceiling** (GCRA). `None` = unlimited. Compiled
    /// only with the `governor` feature; orthogonal to the AIMD concurrency limiter.
    #[cfg(feature = "governor")]
    rate: Option<Arc<WorkerRateLimiter>>,
    /// Provider-advised cooldown: the epoch-ms instant this worker should hold new
    /// requests until, **learned from the responses themselves** — `Retry-After` on a
    /// 429/503, or an `x-ratelimit-reset-*` when the matching `x-ratelimit-remaining-*`
    /// hits 0. Zero = no cooldown. Every `submit` waits out any live cooldown at entry,
    /// so the *whole fleet* backs off in lockstep with what the endpoint told us —
    /// proactively, instead of each item re-discovering the limit with its own 429.
    cooldown_until_ms: Arc<AtomicI64>,
    retry: RetryPolicy,
    /// Opt-in content validation of a 2xx body. A failure is a *soft* failure: it
    /// retries like a 5xx, then dead-letters — so bad data is never silently emitted.
    check: ResponseCheck,
}

fn w(e: impl std::fmt::Display) -> ForgeError {
    ForgeError::Worker(e.to_string())
}

/// Build a direct GCRA limiter at `per_second` cells/sec (burst == rate). `0` is
/// clamped to `1` so the `NonZeroU32` never panics.
#[cfg(feature = "governor")]
fn make_rate_limiter(per_second: u32) -> Arc<WorkerRateLimiter> {
    let q = governor::Quota::per_second(std::num::NonZeroU32::new(per_second.max(1)).unwrap());
    Arc::new(governor::RateLimiter::direct(q))
}

/// 429, request-timeout, and any 5xx are transient → retry. Other 4xx are a bad
/// request → terminal, no retry.
fn is_retryable_status(s: reqwest::StatusCode) -> bool {
    s == reqwest::StatusCode::TOO_MANY_REQUESTS
        || s == reqwest::StatusCode::REQUEST_TIMEOUT
        || s.is_server_error()
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// The maximum cooldown a single response can request — a malformed or hostile
/// `Retry-After: 999999` must not strand a worker. Longer waits still happen, just
/// re-armed a chunk at a time as the endpoint keeps saying "not yet".
const MAX_COOLDOWN: Duration = Duration::from_secs(120);

/// A `<digits>[unit]` rate-limit duration → `Duration`. OpenAI-style resets look like
/// `1s`, `6m0s`, `2m59.56s`, or a bare number of seconds. Returns `None` if unparseable.
fn parse_reset_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Bare number → seconds (float tolerated).
    if let Ok(secs) = s.parse::<f64>() {
        return (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs));
    }
    // `<num><unit>` segments: h / m / s / ms.
    let mut total = 0f64;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            chars.next();
        } else {
            let mut unit = String::new();
            while let Some(&u) = chars.peek() {
                if u.is_ascii_alphabetic() {
                    unit.push(u);
                    chars.next();
                } else {
                    break;
                }
            }
            let v: f64 = num.parse().ok()?;
            num.clear();
            total += match unit.as_str() {
                "h" => v * 3600.0,
                "m" => v * 60.0,
                "s" => v,
                "ms" => v / 1000.0,
                _ => return None,
            };
        }
    }
    (total > 0.0).then(|| Duration::from_secs_f64(total))
}

/// Learn a cooldown from a response's rate-limit signals (adaptive rate control):
/// honor an explicit `Retry-After`
/// (seconds only — HTTP-date form is rare on these APIs and treated as absent), else
/// pause for `x-ratelimit-reset-{requests,tokens}` when the matching `-remaining-*`
/// reached 0. Returns the longest such signal, clamped to [`MAX_COOLDOWN`].
fn cooldown_from_headers(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());

    let mut cooldown = get("retry-after")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);

    // A reset only bites when the paired budget is actually spent.
    for (remaining, reset) in [
        (
            "x-ratelimit-remaining-requests",
            "x-ratelimit-reset-requests",
        ),
        ("x-ratelimit-remaining-tokens", "x-ratelimit-reset-tokens"),
    ] {
        let spent = get(remaining)
            .and_then(|v| v.trim().parse::<f64>().ok())
            .is_some_and(|r| r <= 0.0);
        if spent {
            if let Some(d) = get(reset).and_then(parse_reset_duration) {
                cooldown = Some(cooldown.map_or(d, |c| c.max(d)));
            }
        }
    }
    cooldown.map(|c| c.min(MAX_COOLDOWN))
}

impl HttpWorker {
    /// Arm the shared cooldown to `now + d` if that is later than the current arm
    /// (monotonic max — a stronger signal never gets shortened by a weaker later one
    /// within the same window). `None`/zero is a no-op.
    fn arm_cooldown(&self, d: Option<Duration>) {
        if let Some(d) = d {
            let until = now_ms().saturating_add(d.as_millis() as i64);
            self.cooldown_until_ms.fetch_max(until, Ordering::Relaxed);
        }
    }

    /// Sleep out any live cooldown (capped per-nap at [`MAX_COOLDOWN`] so a long arm is
    /// re-checked rather than held in one un-cancellable sleep).
    async fn wait_out_cooldown(&self) {
        loop {
            let until = self.cooldown_until_ms.load(Ordering::Relaxed);
            let now = now_ms();
            if until <= now {
                return;
            }
            let nap = (until - now).min(MAX_COOLDOWN.as_millis() as i64);
            tokio::time::sleep(Duration::from_millis(nap as u64)).await;
        }
    }

    /// Build a worker from its spec: a keep-alive HTTP client and an in-flight
    /// semaphore sized to the declared concurrency.
    pub fn new(spec: WorkerSpec) -> Result<Self, ForgeError> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(spec.concurrency_limit.max(1))
            // Bound stalls: a dead/hung engine must not pin a semaphore permit
            // forever. `connect_timeout` is tight; the overall request timeout is
            // generous since batch generations can legitimately run minutes.
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(600))
            .build()
            .map_err(w)?;
        let inflight = AimdLimit::new(spec.concurrency_limit);
        // Build the rate limiter from the spec (when the feature is on) before `spec`
        // is moved into the struct.
        #[cfg(feature = "governor")]
        let rate = spec.rate_limit_per_second.map(make_rate_limiter);
        Ok(Self {
            spec,
            client,
            inflight,
            ready: Arc::new(AtomicBool::new(true)),
            load: Arc::new(AtomicU32::new(LOAD_UNKNOWN)),
            #[cfg(feature = "governor")]
            rate,
            cooldown_until_ms: Arc::new(AtomicI64::new(0)),
            retry: RetryPolicy::default(),
            check: ResponseCheck::default(),
        })
    }

    /// Set a global request-rate ceiling (req/sec). Orthogonal to the AIMD concurrency
    /// limiter: that bounds in-flight requests, this bounds the emission *rate* (e.g.
    /// against a shared engine or a cloud Batch API's documented RPS cap).
    #[cfg(feature = "governor")]
    pub fn with_rate_limit(mut self, per_second: u32) -> Self {
        self.rate = Some(make_rate_limiter(per_second));
        self
    }

    /// Override the retry policy (defaults to [`RetryPolicy::default`]).
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Require a content check on every 2xx response (defaults to
    /// [`ResponseCheck::Any`], i.e. no check). A failing body is retried and then
    /// dead-lettered instead of being emitted as a (silently bad) result.
    pub fn with_validation(mut self, check: ResponseCheck) -> Self {
        self.check = check;
        self
    }

    /// Test/manual override of the cached readiness flag.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// Best-effort fetch of the engine's queue-depth metric (B3). Returns `None` when
    /// there's no metric endpoint for the engine, the GET fails, or the body doesn't
    /// parse — the caller maps that to `LOAD_UNKNOWN`. Short-circuited by a tight
    /// timeout so a slow metrics endpoint can't stall the probe.
    async fn fetch_load(&self) -> Option<u32> {
        let path = self.spec.engine_hint.metrics_path()?;
        let url = format!("{}{}", self.spec.base_url.trim_end_matches('/'), path);
        let resp = self
            .client
            .get(&url)
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body = resp.text().await.ok()?;
        parse_load(self.spec.engine_hint, &body)
    }
}

/// Parse the queue-depth metric for `hint` from a raw response body. Pure +
/// dependency-free (Prometheus text is hand-scanned; JSON via the existing
/// `serde_json`), so no new crate touches the lean binary. Best-effort: any shape it
/// doesn't recognize yields `None` (→ load unknown → round-robin for that worker).
fn parse_load(hint: EngineHint, body: &str) -> Option<u32> {
    match hint {
        EngineHint::Vllm => parse_vllm_waiting(body),
        EngineHint::LlamaCpp => {
            parse_llamacpp_busy_slots(&serde_json::from_str::<serde_json::Value>(body).ok()?)
        }
        EngineHint::Sglang => {
            parse_sglang_waiting(&serde_json::from_str::<serde_json::Value>(body).ok()?)
        }
        EngineHint::Router => None,
    }
}

/// vLLM `/metrics` (Prometheus text): the gauge `vllm:num_requests_waiting` (the
/// dispatch-relevant backlog). Skips `#` comment lines and any `{labels}`, takes the
/// trailing value, rounds to `u32`. A decoy like `vllm:num_requests_running` is
/// ignored because the metric *name token* must match exactly.
fn parse_vllm_waiting(metrics_text: &str) -> Option<u32> {
    const NAME: &str = "vllm:num_requests_waiting";
    for line in metrics_text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A sample line is `name{labels} value [timestamp]`. Take the value as the
        // field immediately AFTER the name (+ optional `{labels}`) — never the last
        // field, which may be an optional Prometheus timestamp (`metric 7 171234`).
        let mut parts = line.split_whitespace();
        let name_and_labels = parts.next()?;
        let name = name_and_labels
            .split_once('{')
            .map(|(n, _)| n)
            .unwrap_or(name_and_labels);
        if name != NAME {
            continue;
        }
        let value = parts.next()?;
        let n = value.parse::<f64>().ok()?;
        if n.is_finite() && n >= 0.0 {
            return Some(n.round() as u32);
        }
    }
    None
}

/// llama.cpp `/slots`: a JSON array of slot objects. Load = count of slots currently
/// busy (`is_processing == true`, falling back to a non-zero `state` on older builds).
fn parse_llamacpp_busy_slots(slots: &serde_json::Value) -> Option<u32> {
    let arr = slots.as_array()?;
    let busy = arr
        .iter()
        .filter(|s| {
            s.get("is_processing")
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| s.get("state").and_then(|v| v.as_u64()).unwrap_or(0) != 0)
        })
        .count();
    Some(busy as u32)
}

/// SGLang `/get_server_info`: queue depth under `internal_states[*]` (one per DP
/// rank), summed; falls back to top-level `waiting_queue_len` / `num_waiting_reqs`.
fn parse_sglang_waiting(info: &serde_json::Value) -> Option<u32> {
    let field = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_u64());
    if let Some(states) = info.get("internal_states").and_then(|v| v.as_array()) {
        let mut total: u64 = 0;
        let mut any = false;
        for st in states {
            if let Some(n) =
                field(st, "num_waiting_reqs").or_else(|| field(st, "waiting_queue_len"))
            {
                total += n;
                any = true;
            }
        }
        if any {
            return Some(total.min(u32::MAX as u64) as u32);
        }
    }
    field(info, "waiting_queue_len")
        .or_else(|| field(info, "num_waiting_reqs"))
        .map(|n| n.min(u32::MAX as u64) as u32)
}

impl Worker for HttpWorker {
    fn spec(&self) -> &WorkerSpec {
        &self.spec
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    fn load(&self) -> Option<u32> {
        match self.load.load(Ordering::Relaxed) {
            LOAD_UNKNOWN => None,
            n => Some(n),
        }
    }

    async fn probe(&self) -> bool {
        let url = format!(
            "{}{}",
            self.spec.base_url.trim_end_matches('/'),
            self.spec.health_path
        );
        let ready = match self.client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        };
        self.ready.store(ready, Ordering::Relaxed);
        // Best-effort load refresh for B3 dispatch bias — strictly a side channel:
        // readiness above is never coupled to it, and any failure leaves the load
        // `LOAD_UNKNOWN` (so a stale value never sticks and dispatch falls back to
        // round-robin for this worker).
        let depth = if ready {
            self.fetch_load().await.unwrap_or(LOAD_UNKNOWN)
        } else {
            LOAD_UNKNOWN
        };
        self.load.store(depth, Ordering::Relaxed);
        ready
    }

    async fn submit(&self, item: &Item) -> Result<ItemResult, ForgeError> {
        // Bound in-flight requests for the whole submit (including backoff waits).
        let _permit = self
            .inflight
            .acquire()
            .await
            .map_err(|e| ForgeError::Worker(format!("semaphore closed: {e}")))?;

        // Provider-advised cooldown, learned from prior responses (Retry-After /
        // x-ratelimit-reset). Waited out once at entry so a `429`/exhausted-budget
        // signal one item saw makes every *other* item hold too — the fleet tracks the
        // endpoint's real limit instead of hammering it into repeated 429s. Not an AIMD
        // signal (the concurrency controller is orthogonal to the rate ceiling).
        self.wait_out_cooldown().await;

        // Global rate ceiling (GCRA), if configured. Gated **once per submit**, after
        // the AIMD permit and before the retry loop — so a slow rate cap doesn't
        // separately throttle this item's retries, and the rate ceiling (not the
        // concurrency cap) is the global bottleneck by design. A rate wait is *not* an
        // overload signal, so it never touches the AIMD controller.
        #[cfg(feature = "governor")]
        if let Some(rl) = &self.rate {
            rl.until_ready().await;
        }

        let url = self.spec.request_url(&item.url);
        // Validate against the *item's* endpoint (a run may carry a per-line mix of
        // chat / completions / embeddings), falling back to the worker's declared
        // kind when the item left `url` blank.
        let kind = EndpointKind::from_path(&item.url).unwrap_or(self.spec.endpoint_kind);
        let started = std::time::Instant::now();
        let attempts = self.retry.max_attempts.max(1);
        let mut last_err = String::from("no attempt made");

        for attempt in 0..attempts {
            // Re-check the shared cooldown before every attempt, not just at submit() entry —
            // a prior attempt in *this* retry loop can arm it (a 429 or exhausted budget on
            // attempt N must not be retried on the same fleet-wide backoff-less schedule as
            // attempt N+1). A no-op fast path when nothing is armed.
            self.wait_out_cooldown().await;
            match self.client.post(&url).json(&item.body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    // Learn from the response's rate-limit headers regardless of status
                    // — a near-exhausted budget on an otherwise-clean 2xx still tells us
                    // to slow down before the endpoint has to 429 us.
                    self.arm_cooldown(cooldown_from_headers(resp.headers()));
                    if status.is_success() {
                        let request_id = resp
                            .headers()
                            .get("x-request-id")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        let body: serde_json::Value = resp
                            .json()
                            .await
                            .map_err(|e| ForgeError::Worker(format!("decoding response: {e}")))?;
                        // Opt-in content validation: a structurally-bad 2xx (empty
                        // generation / non-JSON when JSON was required) is a *soft*
                        // failure — fall through to retry like a 5xx rather than emit
                        // it. Re-sampling may produce a good output; if not, the item
                        // dead-letters with this reason instead of corrupting results.
                        if let Err(reason) = self.check.validate(kind, &body) {
                            last_err = format!("validation failed: {reason}");
                        } else {
                            // AIMD: a clean 2xx is the "increase" signal.
                            self.inflight.record(true);
                            // `usage` is captured free from every OpenAI-compatible body.
                            let usage = body
                                .get("usage")
                                .map(forge_core::TokenUsage::from_openai_usage)
                                .unwrap_or_default();
                            return Ok(ItemResult {
                                custom_id: item.custom_id.clone(),
                                response: Some(ItemResponse {
                                    status_code: status.as_u16(),
                                    request_id,
                                    body,
                                }),
                                error: None,
                                usage,
                                worker_id: self.spec.worker_id.clone(),
                                latency_ms: started.elapsed().as_millis() as u64,
                                attempt: item.attempts,
                                completed_at: now_ms(),
                            });
                        }
                    } else if is_retryable_status(status) {
                        last_err = format!("HTTP {status}");
                        // AIMD: 429/5xx is a transient-overload signal → decrease.
                        self.inflight.record(false);
                    } else {
                        // Terminal client error (400/401/403/404/422/…) — never retry.
                        let body = resp.text().await.unwrap_or_default();
                        return Err(ForgeError::Worker(format!(
                            "HTTP {status} (terminal): {}",
                            truncate(&body, 200)
                        )));
                    }
                }
                Err(e) => {
                    // Connection refused / timeout / reset — transient.
                    last_err = format!("request error: {e}");
                    // AIMD: a connection/timeout error is also an overload signal.
                    self.inflight.record(false);
                }
            }

            if attempt + 1 < attempts {
                tokio::time::sleep(self.retry.backoff(attempt)).await;
            }
        }

        Err(ForgeError::Worker(format!(
            "retries exhausted after {attempts} attempt(s): {last_err}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_core::{EndpointKind, ItemState};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn worker(base: &str) -> HttpWorker {
        HttpWorker::new(WorkerSpec::new("w0", base, EndpointKind::Chat).concurrency(4))
            .unwrap()
            .with_retry(RetryPolicy {
                max_attempts: 3,
                base: Duration::from_millis(1),
                cap: Duration::from_millis(4),
            })
    }

    fn item() -> Item {
        Item {
            custom_id: "req-1".into(),
            method: "POST".into(),
            url: "/v1/chat/completions".into(),
            body: json!({"model": "m", "messages": []}),
            status: ItemState::Leased,
            attempts: 1,
            leased_until: None,
            leased_by: None,
            last_error: None,
        }
    }

    #[test]
    fn builds_from_spec() {
        let spec = WorkerSpec::new("gpu1", "http://gpu1:8000", EndpointKind::Chat).concurrency(64);
        let w = HttpWorker::new(spec).expect("build");
        assert_eq!(w.spec().worker_id, "gpu1");
        assert_eq!(w.spec().concurrency_limit, 64);
        assert!(w.is_ready()); // optimistic until first probe
        w.set_ready(false);
        assert!(!w.is_ready());
    }

    #[tokio::test]
    async fn submit_success_parses_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "hi"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let r = worker(&server.uri()).submit(&item()).await.unwrap();
        assert!(r.is_success());
        assert_eq!(r.usage.total_tokens, 15);
        assert_eq!(r.response.unwrap().status_code, 200);
    }

    #[tokio::test]
    async fn submit_retries_then_exhausts_on_persistent_5xx() {
        let server = MockServer::start().await;
        // max_attempts = 3 → the POST is tried exactly 3 times before giving up.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .expect(3)
            .mount(&server)
            .await;

        let err = worker(&server.uri()).submit(&item()).await.unwrap_err();
        assert!(format!("{err}").contains("exhausted"), "got: {err}");
        // server drop verifies the .expect(3)
    }

    #[tokio::test]
    async fn submit_retries_then_succeeds_on_transient_outage() {
        // A flaky endpoint: 503 for the first two calls, then 200. With max_attempts=3
        // the item recovers on attempt 3 instead of dead-lettering — the spot/engine
        // "briefly down" case must not lose good work.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            })))
            .with_priority(2)
            .mount(&server)
            .await;

        let r = worker(&server.uri()).submit(&item()).await.unwrap();
        assert!(r.is_success(), "should recover on the 3rd attempt");
        assert_eq!(r.usage.total_tokens, 2);
    }

    #[tokio::test]
    async fn submit_terminal_4xx_does_not_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .expect(1) // exactly one call — no retry on a terminal 4xx
            .mount(&server)
            .await;

        let err = worker(&server.uri()).submit(&item()).await.unwrap_err();
        assert!(format!("{err}").contains("400"), "got: {err}");
    }

    #[test]
    fn parse_reset_duration_forms() {
        assert_eq!(parse_reset_duration("1s"), Some(Duration::from_secs(1)));
        assert_eq!(parse_reset_duration("6m0s"), Some(Duration::from_secs(360)));
        assert_eq!(
            parse_reset_duration("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_reset_duration("2"), Some(Duration::from_secs(2)));
        assert_eq!(parse_reset_duration(""), None);
        assert_eq!(parse_reset_duration("soon"), None);
    }

    #[test]
    fn cooldown_prefers_retry_after_and_honors_exhausted_budget() {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        fn hm(pairs: &[(&str, &str)]) -> HeaderMap {
            let mut h = HeaderMap::new();
            for (k, v) in pairs {
                h.insert(
                    k.parse::<HeaderName>().unwrap(),
                    HeaderValue::from_str(v).unwrap(),
                );
            }
            h
        }
        // Retry-After (seconds) is honored.
        assert_eq!(
            cooldown_from_headers(&hm(&[("retry-after", "3")])),
            Some(Duration::from_secs(3))
        );
        // A reset only bites when the paired remaining budget is 0.
        assert_eq!(
            cooldown_from_headers(&hm(&[
                ("x-ratelimit-remaining-requests", "42"),
                ("x-ratelimit-reset-requests", "10s"),
            ])),
            None,
            "reset with budget left must not cool down"
        );
        assert_eq!(
            cooldown_from_headers(&hm(&[
                ("x-ratelimit-remaining-tokens", "0"),
                ("x-ratelimit-reset-tokens", "5s"),
            ])),
            Some(Duration::from_secs(5))
        );
        // Overlong Retry-After is clamped, never strands the worker.
        assert_eq!(
            cooldown_from_headers(&hm(&[("retry-after", "999999")])),
            Some(MAX_COOLDOWN)
        );
        assert_eq!(cooldown_from_headers(&hm(&[])), None);
    }

    #[tokio::test]
    async fn a_429_with_retry_after_arms_the_shared_cooldown() {
        // First POST returns 429 + Retry-After; the retry succeeds. The worker must learn a
        // cooldown from the 429 *and* honor it before its own next attempt — not just leave
        // it armed for other items to discover while this one hammers straight through.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_string("slow down"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            })))
            .mount(&server)
            .await;

        let w = worker(&server.uri());
        let started = std::time::Instant::now();
        w.submit(&item()).await.unwrap();
        // The retry loop must have waited out the armed cooldown before its own next
        // attempt, not raced straight through on the item's (much shorter) backoff alone.
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "retry must honor the armed cooldown before re-attempting, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn probe_reflects_health() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let w = worker(&server.uri());
        w.set_ready(false);
        assert!(w.probe().await);
        assert!(w.is_ready());
    }

    #[tokio::test]
    async fn probe_marks_unreachable_unready() {
        // Nothing listening on this port → probe fails → unready.
        let w = worker("http://127.0.0.1:1");
        assert!(!w.probe().await);
        assert!(!w.is_ready());
    }

    #[tokio::test]
    async fn validation_failure_retries_then_dead_letters() {
        let server = MockServer::start().await;
        // A 200 whose content is NOT valid JSON. With `--require json` this is a soft
        // failure: the worker re-POSTs up to max_attempts (3), then surfaces Err so
        // the item dead-letters — it is never emitted as a good result.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "sure, here is your answer!"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
            })))
            .expect(3)
            .mount(&server)
            .await;

        let w = worker(&server.uri()).with_validation(ResponseCheck::Json);
        let err = w.submit(&item()).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("validation failed"), "got: {msg}");
        assert!(msg.contains("not valid JSON"), "got: {msg}");
    }

    #[tokio::test]
    async fn aimd_halves_on_failure_then_recovers_on_success() {
        let l = AimdLimit::new(8);
        assert_eq!(l.effective(), 8);

        // No permits held → a failure halves 8→4 and the 4 freed permits are
        // forgotten immediately (debt fully paid).
        l.record(false);
        assert_eq!(l.effective(), 4);
        assert_eq!(l.debt(), 0);
        assert_eq!(l.sem.available_permits(), 4);

        // Another failure: 4→2.
        l.record(false);
        assert_eq!(l.effective(), 2);
        assert_eq!(l.sem.available_permits(), 2);

        // Successes additively increase, one step per AIMD_INCREASE_AFTER.
        for _ in 0..AIMD_INCREASE_AFTER {
            l.record(true);
        }
        assert_eq!(l.effective(), 3);
        assert_eq!(l.sem.available_permits(), 3);

        // Never grows past the ceiling.
        for _ in 0..(AIMD_INCREASE_AFTER * 100) {
            l.record(true);
        }
        assert_eq!(l.effective(), 8);
        assert_eq!(l.sem.available_permits(), 8);
    }

    #[tokio::test]
    async fn aimd_debt_is_paid_as_in_flight_permits_return() {
        let l = AimdLimit::new(4);
        // Hold all 4 permits in flight.
        let held: Vec<_> = [
            l.acquire().await.unwrap(),
            l.acquire().await.unwrap(),
            l.acquire().await.unwrap(),
            l.acquire().await.unwrap(),
        ]
        .into();
        assert_eq!(l.sem.available_permits(), 0);

        // A failure halves 4→2, but nothing is free to forget yet → debt 2.
        l.record(false);
        assert_eq!(l.effective(), 2);
        assert_eq!(l.debt(), 2);
        assert_eq!(l.sem.available_permits(), 0);

        // Drop two in-flight permits → they pay the debt (forgotten, not returned).
        let mut held = held;
        held.pop();
        held.pop();
        assert_eq!(l.debt(), 0);
        assert_eq!(l.sem.available_permits(), 0);

        // The remaining two return normally → available climbs to the new cap of 2.
        held.clear();
        assert_eq!(l.sem.available_permits(), 2);
        assert_eq!(l.effective(), 2);
    }

    #[tokio::test]
    async fn validation_uses_item_endpoint_not_worker_default() {
        // A Chat-default worker handling an embeddings item (per-line url) must
        // validate it as an embedding — not look for chat `choices`.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"embedding": [0.1, -0.2, 0.3]}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 0, "total_tokens": 4}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut emb = item();
        emb.url = "/v1/embeddings".into();
        let w = worker(&server.uri()).with_validation(ResponseCheck::Json);
        // Passes: an embedding vector is present (Json degrades to NonEmpty here).
        assert!(w.submit(&emb).await.unwrap().is_success());
    }

    #[tokio::test]
    async fn validation_success_emits_result() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "{\"answer\": 42}"}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
            })))
            .expect(1) // valid on the first try — no retry
            .mount(&server)
            .await;

        let w = worker(&server.uri()).with_validation(ResponseCheck::Json);
        let r = w.submit(&item()).await.unwrap();
        assert!(r.is_success());
        assert_eq!(r.usage.total_tokens, 7);
    }

    // ── B3 load-aware dispatch ──

    fn worker_hint(base: &str, hint: EngineHint) -> HttpWorker {
        HttpWorker::new(WorkerSpec::new("w0", base, EndpointKind::Chat).engine_hint(hint)).unwrap()
    }

    #[test]
    fn parse_vllm_waiting_picks_the_right_gauge() {
        let body = "\
# HELP vllm:num_requests_waiting Number of requests waiting.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_running{model_name=\"x\"} 2.0
vllm:num_requests_waiting{model_name=\"x\"} 7.0
";
        assert_eq!(parse_vllm_waiting(body), Some(7));
        // Unlabeled form parses too.
        assert_eq!(parse_vllm_waiting("vllm:num_requests_waiting 3"), Some(3));
        // An optional trailing Prometheus timestamp must be ignored (take the value,
        // not the last field).
        assert_eq!(
            parse_vllm_waiting("vllm:num_requests_waiting 7 1718000000000"),
            Some(7)
        );
        // Missing metric / malformed value → None.
        assert_eq!(parse_vllm_waiting("vllm:num_requests_running 1.0"), None);
        assert_eq!(parse_vllm_waiting("vllm:num_requests_waiting NaNish"), None);
    }

    #[test]
    fn parse_llamacpp_counts_busy_slots() {
        let slots = json!([
            {"id": 0, "is_processing": true},
            {"id": 1, "is_processing": false},
            {"id": 2, "is_processing": true},
        ]);
        assert_eq!(parse_llamacpp_busy_slots(&slots), Some(2));
        assert_eq!(parse_llamacpp_busy_slots(&json!([])), Some(0));
        // Older builds expose a `state` int (non-zero = busy).
        assert_eq!(
            parse_llamacpp_busy_slots(&json!([{"state": 1}, {"state": 0}])),
            Some(1)
        );
        // Not an array → unknown.
        assert_eq!(parse_llamacpp_busy_slots(&json!({"slots": 1})), None);
    }

    #[test]
    fn parse_sglang_sums_waiting_reqs() {
        let info = json!({"internal_states": [{"num_waiting_reqs": 5}, {"num_waiting_reqs": 3}]});
        assert_eq!(parse_sglang_waiting(&info), Some(8));
        // Top-level fallback.
        assert_eq!(
            parse_sglang_waiting(&json!({"waiting_queue_len": 4})),
            Some(4)
        );
        // Absent → None.
        assert_eq!(parse_sglang_waiting(&json!({"version": "0.4"})), None);
    }

    #[tokio::test]
    async fn probe_updates_load_for_vllm() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/metrics"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("vllm:num_requests_waiting{model_name=\"m\"} 4.0\n"),
            )
            .mount(&server)
            .await;

        let w = worker_hint(&server.uri(), EngineHint::Vllm);
        assert!(w.probe().await);
        assert_eq!(w.load(), Some(4));
    }

    #[tokio::test]
    async fn probe_updates_load_for_llamacpp() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/slots"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"id": 0, "is_processing": true},
                {"id": 1, "is_processing": true},
                {"id": 2, "is_processing": false},
            ])))
            .mount(&server)
            .await;

        let w = worker_hint(&server.uri(), EngineHint::LlamaCpp);
        assert!(w.probe().await);
        assert_eq!(w.load(), Some(2));
    }

    #[tokio::test]
    async fn load_unknown_when_metrics_missing_but_health_ok() {
        // /health 200, /metrics 404 → ready stays true, load stays unknown. Readiness
        // must NOT be coupled to the (best-effort) load fetch.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/metrics"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let w = worker_hint(&server.uri(), EngineHint::Vllm);
        assert!(w.probe().await);
        assert!(w.is_ready());
        assert_eq!(w.load(), None);
    }

    #[tokio::test]
    async fn router_hint_never_fetches_load() {
        // A router self-balances → no metrics_path → probe never reads a load.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let w = worker_hint(&server.uri(), EngineHint::Router);
        assert!(w.probe().await);
        assert_eq!(w.load(), None);
    }

    #[tokio::test]
    async fn unready_clears_stale_load() {
        // A previously-known load must not stick once the worker goes unready.
        let w = worker_hint("http://127.0.0.1:1", EngineHint::Vllm);
        w.load.store(5, Ordering::Relaxed);
        assert!(!w.probe().await); // unreachable → unready
        assert_eq!(w.load(), None); // stale load cleared
    }

    // ── governor GCRA rate ceiling (feature-gated; CI runs --features governor) ──

    #[cfg(feature = "governor")]
    #[test]
    fn gcra_bounds_rate_deterministically() {
        // Drive the exact limiter the worker uses with a fake clock — no wall-clock,
        // no flakiness. 2 cells/sec ⇒ a burst of 2, then one cell per 500ms.
        use governor::clock::FakeRelativeClock;
        use governor::{Quota, RateLimiter};

        let clock = FakeRelativeClock::default();
        let q = Quota::per_second(std::num::NonZeroU32::new(2).unwrap());
        let rl = RateLimiter::direct_with_clock(q, clock.clone());

        assert!(rl.check().is_ok(), "cell 1 (burst)");
        assert!(rl.check().is_ok(), "cell 2 (burst)");
        assert!(rl.check().is_err(), "burst exhausted → throttled");

        clock.advance(Duration::from_millis(500)); // replenish exactly one cell
        assert!(rl.check().is_ok(), "one cell back after 500ms");
        assert!(rl.check().is_err(), "and throttled again");
    }

    #[cfg(feature = "governor")]
    #[tokio::test]
    async fn worker_with_rate_limit_still_submits() {
        // A generous ceiling never blocks, so submit succeeds through the gate.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            })))
            .mount(&server)
            .await;

        let w = worker(&server.uri()).with_rate_limit(1000);
        assert!(w.submit(&item()).await.unwrap().is_success());
    }

    #[cfg(feature = "governor")]
    #[tokio::test]
    async fn rate_limit_is_orthogonal_to_aimd() {
        // A rate wait must NOT be read as overload — the AIMD effective limit stays at
        // the ceiling across a run of clean 2xx (the two controls are independent).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"total_tokens": 1}
            })))
            .mount(&server)
            .await;

        let w = worker(&server.uri()).with_rate_limit(1000); // worker() => concurrency(4)
        for _ in 0..3 {
            assert!(w.submit(&item()).await.unwrap().is_success());
        }
        assert_eq!(w.inflight.effective(), 4, "AIMD untouched by the rate gate");
        assert_eq!(w.inflight.debt(), 0);
    }
}
