//! forge-spot — **poll-only** spot/preemptible interruption detection.
//!
//! Each cloud exposes a link-local metadata endpoint that arms shortly before a
//! spot/preemptible VM is reclaimed. A [`InterruptionSource`] polls it on a ~5s loop
//! and yields a [`Notice`] when an interruption is imminent, so a co-located drain
//! agent (the optional `forge-agent`) can **stop pulling new leases** and let in-flight
//! items finish *if the window allows*.
//!
//! **Hard rule (baked into code + docs):** forge *consumes* these signals and
//! re-queues at item granularity — it **never** provisions, launches, or tears down
//! instances. Acquiring capacity is the provisioner's / the operator's job. There
//! is no API in this crate that starts or stops a VM.
//!
//! **Correctness never depends on the notice.** Best-effort by design: a missed or
//! late notice changes nothing — lease-expiry re-queue + idempotent writes are the
//! backbone (ARCHITECTURE §6). So [`InterruptionSource::poll`] returns `None` (not an
//! error) when the metadata service is unreachable or the box isn't being reclaimed;
//! only a genuinely-armed interruption yields `Some`.
//!
//! Design to the **smallest** window: AWS gives ~120s, **GCP and Azure only ~30s**.
//! The `deadline` a notice carries (when the cloud provides one) is advisory; the
//! drain decision is simply "stop taking new work now."

// `InterruptionSource::poll` is used only through generic bounds (never `dyn`), so
// the `async fn` desugaring is fine; silence the forward-compat lint deliberately,
// matching `forge-core`.
#![allow(async_fn_in_trait)]

use std::time::Duration;

/// Which cloud a [`Notice`] / [`InterruptionSource`] is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cloud {
    Aws,
    Gcp,
    Azure,
}

/// What kind of interruption signal fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeKind {
    /// The VM is being reclaimed imminently (terminate/stop/preempt).
    Terminate,
    /// AWS rebalance recommendation — an *earlier*, softer heads-up that the
    /// instance is at elevated risk, before the hard 2-minute action.
    Rebalance,
}

/// A pending interruption surfaced by the cloud's metadata service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub cloud: Cloud,
    pub kind: NoticeKind,
    /// The cloud's own deadline timestamp (raw ISO-8601), when it provides one.
    /// Advisory only — forge designs to the smallest window regardless.
    pub deadline: Option<String>,
}

/// One cloud's poll-only interruption source. Drive [`poll`](Self::poll) on a ~5s
/// loop (or use [`watch`]).
pub trait InterruptionSource {
    /// Which cloud this source talks to.
    fn cloud(&self) -> Cloud;

    /// Poll the metadata service once. `Some(Notice)` iff an interruption is armed;
    /// `None` otherwise — including when the metadata service is unreachable or the
    /// response can't be parsed (best-effort: correctness rides on lease-expiry, not
    /// on this firing).
    async fn poll(&self) -> Option<Notice>;
}

/// Poll `source` every `interval` until it returns a [`Notice`], then yield it.
/// Drop the future to cancel. This never *causes* correctness — it only lets a
/// co-located agent drain gracefully ahead of the reaper.
pub async fn watch(source: &impl InterruptionSource, interval: Duration) -> Notice {
    loop {
        if let Some(notice) = source.poll().await {
            tracing::info!(cloud = ?notice.cloud, kind = ?notice.kind, "spot interruption notice");
            return notice;
        }
        tokio::time::sleep(interval).await;
    }
}

/// A short-timeout HTTP client for link-local metadata (these endpoints are local
/// and must answer fast; a hung metadata call must not stall the poll loop).
fn metadata_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap_or_default()
}

// ───────────────────────────── AWS ─────────────────────────────

/// AWS EC2 Spot via **IMDSv2**: a session token, then
/// `/latest/meta-data/spot/instance-action` (404 until armed → JSON `{action,time}`)
/// with `/events/recommendations/rebalance` as the earlier heads-up.
pub struct AwsSpot {
    client: reqwest::Client,
    base: String,
}

impl Default for AwsSpot {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsSpot {
    /// Point at the real IMDS endpoint (`http://169.254.169.254`).
    pub fn new() -> Self {
        Self::with_base("http://169.254.169.254")
    }

    /// Override the metadata base URL (for tests).
    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            client: metadata_client(),
            base: base.into(),
        }
    }

    /// Fetch a short-lived IMDSv2 token (required for every metadata read).
    async fn token(&self) -> Option<String> {
        let resp = self
            .client
            .put(format!("{}/latest/api/token", self.base))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.text().await.ok().filter(|t| !t.is_empty())
    }

    async fn rebalance(&self, token: &str) -> Option<Notice> {
        let resp = self
            .client
            .get(format!(
                "{}/latest/meta-data/events/recommendations/rebalance",
                self.base
            ))
            .header("X-aws-ec2-metadata-token", token)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let v: serde_json::Value = resp.json().await.ok()?;
        Some(Notice {
            cloud: Cloud::Aws,
            kind: NoticeKind::Rebalance,
            deadline: v
                .get("noticeTime")
                .and_then(|x| x.as_str())
                .map(str::to_string),
        })
    }
}

impl InterruptionSource for AwsSpot {
    fn cloud(&self) -> Cloud {
        Cloud::Aws
    }

    async fn poll(&self) -> Option<Notice> {
        let token = self.token().await?;
        let resp = self
            .client
            .get(format!(
                "{}/latest/meta-data/spot/instance-action",
                self.base
            ))
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .await
            .ok()?;
        // 404 = not armed for termination; fall back to the earlier rebalance hint.
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return self.rebalance(&token).await;
        }
        if !resp.status().is_success() {
            return None;
        }
        let v: serde_json::Value = resp.json().await.ok()?;
        // {"action":"terminate"|"stop"|"hibernate","time":"2026-..Z"}
        Some(Notice {
            cloud: Cloud::Aws,
            kind: NoticeKind::Terminate,
            deadline: v.get("time").and_then(|x| x.as_str()).map(str::to_string),
        })
    }
}

// ───────────────────────────── GCP ─────────────────────────────

/// GCP Spot via metadata `instance/preempted` (`TRUE` once the VM is being
/// preempted; the ACPI G2 soft-off / SIGTERM follows within the ~30s window).
pub struct GcpSpot {
    client: reqwest::Client,
    base: String,
}

impl Default for GcpSpot {
    fn default() -> Self {
        Self::new()
    }
}

impl GcpSpot {
    pub fn new() -> Self {
        Self::with_base("http://metadata.google.internal")
    }

    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            client: metadata_client(),
            base: base.into(),
        }
    }
}

impl InterruptionSource for GcpSpot {
    fn cloud(&self) -> Cloud {
        Cloud::Gcp
    }

    async fn poll(&self) -> Option<Notice> {
        let resp = self
            .client
            .get(format!(
                "{}/computeMetadata/v1/instance/preempted",
                self.base
            ))
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body = resp.text().await.ok()?;
        body.trim().eq_ignore_ascii_case("TRUE").then_some(Notice {
            cloud: Cloud::Gcp,
            kind: NoticeKind::Terminate,
            deadline: None,
        })
    }
}

// ──────────────────────────── Azure ────────────────────────────

/// Azure Spot via `/metadata/scheduledevents` — a `Preempt` event arms ~30s before
/// reclaim, carrying a `NotBefore` deadline.
pub struct AzureSpot {
    client: reqwest::Client,
    base: String,
}

impl Default for AzureSpot {
    fn default() -> Self {
        Self::new()
    }
}

impl AzureSpot {
    pub fn new() -> Self {
        Self::with_base("http://169.254.169.254")
    }

    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            client: metadata_client(),
            base: base.into(),
        }
    }
}

impl InterruptionSource for AzureSpot {
    fn cloud(&self) -> Cloud {
        Cloud::Azure
    }

    async fn poll(&self) -> Option<Notice> {
        let resp = self
            .client
            .get(format!(
                "{}/metadata/scheduledevents?api-version=2020-07-01",
                self.base
            ))
            .header("Metadata", "true")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let v: serde_json::Value = resp.json().await.ok()?;
        let events = v.get("Events").and_then(|e| e.as_array())?;
        for ev in events {
            if ev.get("EventType").and_then(|x| x.as_str()) == Some("Preempt") {
                return Some(Notice {
                    cloud: Cloud::Azure,
                    kind: NoticeKind::Terminate,
                    deadline: ev
                        .get("NotBefore")
                        .and_then(|x| x.as_str())
                        .map(str::to_string),
                });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn aws_armed_termination() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/latest/api/token"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tok"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/latest/meta-data/spot/instance-action"))
            .and(header("X-aws-ec2-metadata-token", "tok"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "action": "terminate",
                "time": "2026-06-30T18:00:00Z"
            })))
            .mount(&server)
            .await;

        let src = AwsSpot::with_base(server.uri());
        let n = src.poll().await.expect("armed");
        assert_eq!(n.cloud, Cloud::Aws);
        assert_eq!(n.kind, NoticeKind::Terminate);
        assert_eq!(n.deadline.as_deref(), Some("2026-06-30T18:00:00Z"));
    }

    #[tokio::test]
    async fn aws_not_armed_falls_back_to_rebalance() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tok"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/latest/meta-data/spot/instance-action"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/latest/meta-data/events/recommendations/rebalance"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "noticeTime": "2026-06-30T17:59:00Z"
            })))
            .mount(&server)
            .await;

        let n = AwsSpot::with_base(server.uri())
            .poll()
            .await
            .expect("rebalance");
        assert_eq!(n.kind, NoticeKind::Rebalance);
    }

    #[tokio::test]
    async fn aws_quiet_when_nothing_armed() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tok"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        assert!(AwsSpot::with_base(server.uri()).poll().await.is_none());
    }

    #[tokio::test]
    async fn gcp_preempted_true() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/computeMetadata/v1/instance/preempted"))
            .and(header("Metadata-Flavor", "Google"))
            .respond_with(ResponseTemplate::new(200).set_body_string("TRUE"))
            .mount(&server)
            .await;
        let n = GcpSpot::with_base(server.uri())
            .poll()
            .await
            .expect("preempted");
        assert_eq!(n.cloud, Cloud::Gcp);

        // FALSE → no notice.
        let server2 = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("FALSE"))
            .mount(&server2)
            .await;
        assert!(GcpSpot::with_base(server2.uri()).poll().await.is_none());
    }

    #[tokio::test]
    async fn azure_preempt_event() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/metadata/scheduledevents"))
            .and(header("Metadata", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Events": [
                    {"EventType": "Reboot", "NotBefore": "x"},
                    {"EventType": "Preempt", "NotBefore": "2026-06-30T18:00:00Z"}
                ]
            })))
            .mount(&server)
            .await;
        let n = AzureSpot::with_base(server.uri())
            .poll()
            .await
            .expect("preempt");
        assert_eq!(n.cloud, Cloud::Azure);
        assert_eq!(n.deadline.as_deref(), Some("2026-06-30T18:00:00Z"));
    }

    #[tokio::test]
    async fn azure_no_preempt_event() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Events": [{"EventType": "Freeze", "NotBefore": "x"}]
            })))
            .mount(&server)
            .await;
        assert!(AzureSpot::with_base(server.uri()).poll().await.is_none());
    }

    #[tokio::test]
    async fn unreachable_metadata_is_quiet_not_an_error() {
        // Nothing listening → poll returns None (best-effort), never panics/errs.
        assert!(AwsSpot::with_base("http://127.0.0.1:1")
            .poll()
            .await
            .is_none());
        assert!(GcpSpot::with_base("http://127.0.0.1:1")
            .poll()
            .await
            .is_none());
        assert!(AzureSpot::with_base("http://127.0.0.1:1")
            .poll()
            .await
            .is_none());
    }
}
