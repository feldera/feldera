//! Per-tenant registry of describer build log channels.
//!
//! Reuses the same log fan-out infrastructure that streams pipeline
//! runtime logs from runner to console: each active session is a
//! [`crate::runner::pipeline_logs::start_thread_pipeline_logs`]
//! invocation, with a `LogsSender` for the cargo-output reader and a
//! follow-request channel for HTTP subscribers.
//!
//! Per tenant we hold at most one active session. A new build calls
//! [`BuildLogBus::begin_session`], which drops any prior session
//! (terminating its thread and closing existing subscribers) and
//! returns a fresh [`LogsSender`] for the cargo-output reader. The
//! log thread itself owns the [`LogsBuffer`], so late subscribers
//! arriving mid-build are caught up automatically by
//! `catch_up_and_add_follower`.
//!
//! When a build finishes, the session is **not** torn down — the
//! buffer is retained so late subscribers can still see the most
//! recent build's output. The session is replaced only when the next
//! build begins. The on-disk `<workspace>/build.log` remains the
//! persistent record across pipeline-manager restarts.
//!
//! [`LogsSender`]: crate::runner::pipeline_logs::LogsSender
//! [`LogsBuffer`]: crate::runner::pipeline_logs::LogsBuffer

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::db::types::tenant::TenantId;
use crate::runner::pipeline_logs::{start_thread_pipeline_logs, LogMessage, LogsSender};

/// Maximum number of follow-request slots per session.
///
/// Each HTTP `GET /v0/connectors/build-log` consumes one slot until
/// the request channel pulls it; concurrent subscribers exceeding this
/// will see a 503-equivalent error from the endpoint. Sized for
/// expected console use (1–2 active operators per tenant).
const FOLLOW_REQUEST_BUFFER: usize = 16;

/// Maximum log messages buffered between the cargo-output reader and
/// the log thread before send blocks. Two readers (stdout, stderr)
/// produce at most a few hundred lines per second under cargo's
/// chattiest output.
const LOGS_CHANNEL_BUFFER: usize = 1_024;

/// One in-flight or recently-completed build session.
struct Session {
    /// Sender for cargo-output readers. Cloning this hands out
    /// additional senders (one per stdio stream).
    sender: LogsSender,
    /// Used by HTTP subscribers to register themselves with the log
    /// thread. The thread catches them up from `LogsBuffer` and then
    /// forwards live lines.
    follow_sender: mpsc::Sender<mpsc::Sender<String>>,
    /// Held to terminate the log thread when this session is
    /// replaced. Drop alone is also sufficient (closing all senders
    /// breaks the thread out of its `select!` loop).
    _terminate: oneshot::Sender<()>,
    /// Kept alive so the thread is not aborted when the session is
    /// dropped. Awaited only on test teardown.
    _join: JoinHandle<()>,
}

/// Deployment-wide map: `TenantId → Session`.
///
/// Wrapped in `Arc` because both the API endpoint and the spawned
/// describer-build task hold references.
#[derive(Default)]
pub struct BuildLogBus {
    sessions: Mutex<HashMap<TenantId, Session>>,
}

impl BuildLogBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Begin a new build session for `tenant_id`.
    ///
    /// Replaces any prior session — its log thread is terminated and
    /// any HTTP subscribers see an end-of-stream marker. Returns a
    /// `LogsSender` that the cargo-output reader uses to feed lines
    /// into the new session's buffer and broadcast.
    pub async fn begin_session(&self, tenant_id: TenantId) -> LogsSender {
        // Channels for the new session.
        let (logs_sender, logs_receiver) = mpsc::channel(LOGS_CHANNEL_BUFFER);
        let (follow_sender, follow_receiver) = mpsc::channel(FOLLOW_REQUEST_BUFFER);
        let sender = LogsSender::new(logs_sender);

        // The log thread uses pipeline_id / pipeline_name as identity
        // tags in formatted control-plane log lines. For describer
        // builds we tag them with the tenant ID so JSON-formatted log
        // outputs (production deployments) can be filtered.
        let (terminate, join) = start_thread_pipeline_logs(
            tenant_id.0.to_string(),
            String::new(),
            follow_receiver,
            logs_receiver,
        );

        // Replace any prior session for this tenant. The old `Session`
        // drops here; that drop closes its sender side, which the old
        // log thread observes and exits.
        let mut guard = self.sessions.lock().await;
        guard.insert(
            tenant_id,
            Session {
                sender: sender.clone(),
                follow_sender,
                _terminate: terminate,
                _join: join,
            },
        );
        sender
    }

    /// Forward a single line of cargo output to the active session.
    /// No-op when no session is active for the tenant.
    ///
    /// Called by the stdout/stderr readers spawned alongside cargo.
    pub async fn append_line(&self, tenant_id: TenantId, line: &str) {
        let sender = {
            let guard = self.sessions.lock().await;
            guard.get(&tenant_id).map(|s| s.sender.clone())
        };
        if let Some(mut sender) = sender {
            sender.send(LogMessage::new_from_pipeline(line)).await;
        }
    }

    /// Subscribe an HTTP client to the active session's live tail.
    ///
    /// Returns the receiver end of an mpsc channel: the log thread
    /// will first replay the buffer (catch-up), then forward live
    /// lines until the session ends or the receiver is dropped.
    ///
    /// Returns `None` when no session has run for this tenant yet, or
    /// when the follow-request slot pool is exhausted.
    pub async fn subscribe(&self, tenant_id: TenantId) -> Option<mpsc::Receiver<String>> {
        let follow_sender = {
            let guard = self.sessions.lock().await;
            guard.get(&tenant_id).map(|s| s.follow_sender.clone())
        };
        let follow_sender = follow_sender?;
        let (sender, receiver) = mpsc::channel::<String>(MAXIMUM_BUFFERED_LINES_PER_FOLLOWER);
        match follow_sender.try_send(sender) {
            Ok(()) => Some(receiver),
            Err(_) => None,
        }
    }
}

/// Mirrors the runner's per-follower buffer size. Sized to cover the
/// catch-up replay of a fully-populated `LogsBuffer` plus headroom.
const MAXIMUM_BUFFERED_LINES_PER_FOLLOWER: usize = 100_000;

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn tid(n: u128) -> TenantId {
        TenantId(Uuid::from_u128(n))
    }

    #[tokio::test]
    async fn subscribe_to_unknown_tenant_returns_none() {
        let bus = BuildLogBus::default();
        assert!(bus.subscribe(tid(1)).await.is_none());
    }

    /// Drain `rx` until a line containing `needle` appears, or until the
    /// 1-second deadline passes.  Returns whether the needle was found —
    /// the log thread emits a control-plane "Fresh start" preamble that
    /// arbitrary tests need to skip past.
    async fn wait_for_line(rx: &mut mpsc::Receiver<String>, needle: &str) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(line)) if line.contains(needle) => return true,
                Ok(Some(_)) => continue,
                Ok(None) => return false,
                Err(_) => return false,
            }
        }
    }

    #[tokio::test]
    async fn lines_appended_after_session_begin_are_received() {
        let bus = BuildLogBus::default();
        bus.begin_session(tid(1)).await;
        let mut rx = bus.subscribe(tid(1)).await.expect("subscribe");
        bus.append_line(tid(1), "compiling foo").await;
        assert!(
            wait_for_line(&mut rx, "compiling foo").await,
            "expected to see 'compiling foo' on the live tail"
        );
    }

    #[tokio::test]
    async fn late_subscribers_see_history_via_catch_up() {
        let bus = BuildLogBus::default();
        bus.begin_session(tid(1)).await;
        bus.append_line(tid(1), "earlier line").await;
        // Give the log thread a moment to drain the message into its buffer.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut rx = bus.subscribe(tid(1)).await.expect("subscribe");
        // Catch-up replay first — the "Fresh start" control-plane line
        // and then any pipeline lines.
        let mut seen = Vec::new();
        while let Ok(line) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            match line {
                Some(l) => seen.push(l),
                None => break,
            }
        }
        assert!(
            seen.iter().any(|l| l.contains("earlier line")),
            "expected catch-up to include earlier line, got {seen:?}"
        );
    }

    #[tokio::test]
    async fn begin_session_replaces_prior_session() {
        let bus = BuildLogBus::default();
        bus.begin_session(tid(1)).await;
        let mut old_rx = bus.subscribe(tid(1)).await.expect("subscribe");

        // Replace the session.
        bus.begin_session(tid(1)).await;
        bus.append_line(tid(1), "second build").await;

        // The new subscriber sees the new line.
        let mut new_rx = bus.subscribe(tid(1)).await.expect("subscribe");
        assert!(
            wait_for_line(&mut new_rx, "second build").await,
            "expected new subscriber to see 'second build'"
        );

        // The old subscriber's stream eventually ends because the prior
        // log thread terminated.
        let mut closed = false;
        for _ in 0..50 {
            match tokio::time::timeout(std::time::Duration::from_millis(100), old_rx.recv()).await {
                Ok(None) => {
                    closed = true;
                    break;
                }
                Ok(Some(_)) => continue,
                Err(_) => continue,
            }
        }
        assert!(closed, "old receiver should close after session replaced");
    }

    #[tokio::test]
    async fn tenants_are_isolated() {
        let bus = BuildLogBus::default();
        bus.begin_session(tid(1)).await;
        bus.begin_session(tid(2)).await;
        bus.append_line(tid(1), "tenant 1 line").await;
        bus.append_line(tid(2), "tenant 2 line").await;

        let mut rx1 = bus.subscribe(tid(1)).await.expect("subscribe");
        let mut found_t1 = false;
        for _ in 0..50 {
            match tokio::time::timeout(std::time::Duration::from_millis(100), rx1.recv()).await {
                Ok(Some(line)) => {
                    if line.contains("tenant 1 line") {
                        found_t1 = true;
                        break;
                    }
                    assert!(!line.contains("tenant 2 line"), "cross-tenant leakage");
                }
                _ => continue,
            }
        }
        assert!(found_t1, "tenant 1 should have seen its own line");
    }
}
