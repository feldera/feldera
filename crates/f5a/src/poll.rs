//! Background polling. The terminal never waits on the network: this task
//! fetches on its own cadence and streams results to the reducer as messages.

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::app::Msg;
use crate::gateway::Gateway;

/// Control messages into the poller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PollControl {
    /// Fetch everything now, resetting the cadence.
    RefreshNow,
    /// Poll this pipeline's detail (stats + time series) every cycle.
    Watch(Option<String>),
    /// Change the poll cadence.
    SetRefreshSecs(u64),
    /// Stop polling.
    Shutdown,
}

/// How many dashboard rows may fetch `/stats` concurrently per cycle.
const ROW_STATS_CONCURRENCY: usize = 6;

/// Epoch milliseconds now; the poller stamps every message it emits.
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Run the poll loop until [`PollControl::Shutdown`] or all channels close.
///
/// Every cycle fetches the overview; when it succeeds, the queryable rows'
/// stats are fetched with bounded concurrency, and the watched pipeline gets
/// its detail (stats plus time series).
pub async fn run<G: Gateway>(
    gateway: G,
    mut control: mpsc::UnboundedReceiver<PollControl>,
    messages: mpsc::UnboundedSender<Msg>,
    refresh_secs: u64,
) {
    let mut refresh = Duration::from_secs(refresh_secs.clamp(1, 60));
    let mut watched: Option<String> = None;

    loop {
        // Apply control that arrived while fetching, so a watch set before
        // the first cycle already shapes the first cycle.
        loop {
            match control.try_recv() {
                Ok(PollControl::Watch(pipeline)) => watched = pipeline,
                Ok(PollControl::SetRefreshSecs(seconds)) => {
                    refresh = Duration::from_secs(seconds.clamp(1, 60));
                }
                Ok(PollControl::RefreshNow) => {}
                Ok(PollControl::Shutdown) => return,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
        let cycle_started = tokio::time::Instant::now();
        let deferred = poll_once(&gateway, &watched, &messages, &mut control).await;
        // Control that arrived mid-cycle applies now; a refresh request or a
        // changed watch starts the next cycle without waiting.
        let mut skip_wait = false;
        for command in deferred {
            match command {
                PollControl::Watch(pipeline) => {
                    skip_wait |= watched != pipeline;
                    watched = pipeline;
                }
                PollControl::SetRefreshSecs(seconds) => {
                    refresh = Duration::from_secs(seconds.clamp(1, 60));
                }
                PollControl::RefreshNow => skip_wait = true,
                PollControl::Shutdown => return,
            }
        }
        if skip_wait {
            continue;
        }

        // Sleep the remainder of the cadence, handling control messages as
        // they arrive; a refresh request short-circuits the wait.
        let mut wake = cycle_started + refresh;
        loop {
            tokio::select! {
                command = control.recv() => match command {
                    Some(PollControl::RefreshNow) => break,
                    Some(PollControl::Watch(pipeline)) => {
                        // A newly watched pipeline deserves data immediately.
                        let changed = watched != pipeline;
                        watched = pipeline;
                        if changed {
                            break;
                        }
                    }
                    Some(PollControl::SetRefreshSecs(seconds)) => {
                        refresh = Duration::from_secs(seconds.clamp(1, 60));
                        wake = cycle_started + refresh;
                    }
                    Some(PollControl::Shutdown) | None => return,
                },
                _ = tokio::time::sleep_until(wake) => break,
            }
        }
        if messages.is_closed() {
            return;
        }
    }
}

/// One poll cycle. Returns the control messages that arrived while row
/// stats were being fetched; a refresh request or a watch change cuts that
/// phase short so the caller can start the next cycle at once.
async fn poll_once<G: Gateway>(
    gateway: &G,
    watched: &Option<String>,
    messages: &mpsc::UnboundedSender<Msg>,
    control: &mut mpsc::UnboundedReceiver<PollControl>,
) -> Vec<PollControl> {
    let started = tokio::time::Instant::now();
    let overview = gateway.overview().await;
    let latency_millis = started.elapsed().as_millis() as u64;

    let queryable: Vec<String> = overview
        .as_ref()
        .map(|overview| {
            overview
                .pipelines
                .iter()
                .filter(|row| row.is_queryable())
                .map(|row| row.name.clone())
                .collect()
        })
        .unwrap_or_default();

    let _ = messages.send(Msg::Overview {
        result: overview,
        latency_millis,
        now_millis: now_millis(),
    });

    // The watched pipeline gets the full detail; other queryable rows get
    // cheap stats for the live dashboard columns.
    if let Some(watched) = watched {
        let result = gateway.detail(watched).await;
        let _ = messages.send(Msg::Detail {
            pipeline: watched.clone(),
            result,
            now_millis: now_millis(),
        });
    }

    // Row stats yield to control: a refresh or a new watch leaves the
    // remaining rows to the next cycle, which starts right away.
    let mut row_stats = futures::stream::iter(
        queryable
            .into_iter()
            .filter(|name| Some(name.as_str()) != watched.as_deref())
            .map(|name| async move {
                let stats = gateway.quick_stats(&name).await;
                (name, stats)
            }),
    )
    .buffer_unordered(ROW_STATS_CONCURRENCY);
    let mut deferred = Vec::new();
    loop {
        tokio::select! {
            next = row_stats.next() => match next {
                Some(ready) => forward_row_stats(messages, ready),
                None => break,
            },
            command = control.recv() => match command {
                Some(command) => {
                    let interrupts = match &command {
                        PollControl::RefreshNow | PollControl::Shutdown => true,
                        PollControl::Watch(pipeline) => pipeline != watched,
                        PollControl::SetRefreshSecs(_) => false,
                    };
                    deferred.push(command);
                    if interrupts {
                        break;
                    }
                }
                None => break,
            },
        }
    }
    deferred
}

fn forward_row_stats(
    messages: &mpsc::UnboundedSender<Msg>,
    (pipeline, result): (
        String,
        Result<crate::model::stats::PipelineStats, crate::http::ApiError>,
    ),
) {
    // Rows that fail stats (racing a shutdown) simply keep their last values.
    if let Ok(stats) = result {
        let _ = messages.send(Msg::RowStats {
            pipeline,
            stats,
            now_millis: now_millis(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{PollControl, run};
    use crate::app::Msg;
    use crate::gateway::PipelineDetail;
    use crate::testutil::{FakeGateway, overview_with, stats_with_processed};
    use tokio::sync::mpsc;

    fn detail() -> PipelineDetail {
        PipelineDetail {
            stats: stats_with_processed(1),
            series: Default::default(),
        }
    }

    async fn recv(messages: &mut mpsc::UnboundedReceiver<Msg>) -> Msg {
        tokio::time::timeout(std::time::Duration::from_secs(5), messages.recv())
            .await
            .expect("poller should emit a message")
            .expect("channel open")
    }

    /// Drain until the poller has been asked to stop and has exited.
    async fn shutdown(
        control_tx: &mpsc::UnboundedSender<PollControl>,
        poller: tokio::task::JoinHandle<()>,
    ) {
        control_tx.send(PollControl::Shutdown).unwrap();
        poller.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_refresh_request_cuts_the_row_stats_phase_short() {
        let gateway = FakeGateway::default();
        let names: Vec<String> = (0..40).map(|index| format!("p{index}")).collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
        gateway.push_overview(overview_with(&borrowed));
        gateway.push_overview(overview_with(&borrowed));
        // Each stats fetch takes ten virtual seconds on this slow link.
        gateway.state.lock().unwrap().quick_stats_delay = Some(std::time::Duration::from_secs(10));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let poller = tokio::spawn(run(gateway.clone(), control_rx, message_tx, 60));
        while !matches!(recv(&mut message_rx).await, Msg::Overview { .. }) {}
        tokio::task::yield_now().await;

        // A refresh during the stats phase yields a fresh overview well
        // before the 40 slow fetches could have finished.
        let asked_at = tokio::time::Instant::now();
        control_tx.send(PollControl::RefreshNow).unwrap();
        while !matches!(recv(&mut message_rx).await, Msg::Overview { .. }) {}
        assert!(
            asked_at.elapsed() < std::time::Duration::from_secs(20),
            "the second overview waited {:?}",
            asked_at.elapsed()
        );
        shutdown(&control_tx, poller).await;
        assert!(
            gateway.state.lock().unwrap().quick_stats_requests.len() < 80,
            "the interrupted phase left rows for the next cycle"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_cycle_reports_overview_then_detail_then_row_stats() {
        let gateway = FakeGateway::default();
        gateway.push_overview(overview_with(&["watched", "other"]));
        gateway.push_detail(detail());
        gateway.push_quick_stats(stats_with_processed(7));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        control_tx
            .send(PollControl::Watch(Some("watched".to_string())))
            .unwrap();
        let poller = tokio::spawn(run(gateway.clone(), control_rx, message_tx, 2));

        // The watch was queued before the first cycle, so it already applies.
        let first = recv(&mut message_rx).await;
        assert!(matches!(first, Msg::Overview { .. }));
        let mut got_detail = false;
        let mut got_row = false;
        for _ in 0..6 {
            match recv(&mut message_rx).await {
                Msg::Detail { pipeline, .. } => {
                    assert_eq!(pipeline, "watched");
                    got_detail = true;
                }
                Msg::RowStats { pipeline, .. } => {
                    assert_eq!(
                        pipeline, "other",
                        "the watched pipeline is not double-fetched"
                    );
                    got_row = true;
                }
                _ => {}
            }
            if got_detail && got_row {
                break;
            }
        }
        assert!(got_detail && got_row);
        control_tx.send(PollControl::Shutdown).unwrap();
        poller.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn watch_changes_trigger_an_immediate_cycle() {
        let gateway = FakeGateway::default();
        gateway.push_overview(overview_with(&["a"]));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let poller = tokio::spawn(run(gateway.clone(), control_rx, message_tx, 60));
        assert!(matches!(recv(&mut message_rx).await, Msg::Overview { .. }));

        gateway.push_overview(overview_with(&["a"]));
        gateway.push_detail(detail());
        control_tx
            .send(PollControl::Watch(Some("a".to_string())))
            .unwrap();
        assert!(matches!(recv(&mut message_rx).await, Msg::Overview { .. }));
        assert!(matches!(recv(&mut message_rx).await, Msg::Detail { .. }));
        assert_eq!(
            gateway.state.lock().unwrap().detail_requests,
            vec!["a".to_string()]
        );
        control_tx.send(PollControl::Shutdown).unwrap();
        poller.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_now_short_circuits_a_long_cadence() {
        let gateway = FakeGateway::default();
        gateway.push_overview(overview_with(&[]));
        gateway.push_overview(overview_with(&[]));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let poller = tokio::spawn(run(gateway.clone(), control_rx, message_tx, 60));
        assert!(matches!(recv(&mut message_rx).await, Msg::Overview { .. }));
        control_tx.send(PollControl::RefreshNow).unwrap();
        assert!(matches!(recv(&mut message_rx).await, Msg::Overview { .. }));
        control_tx.send(PollControl::SetRefreshSecs(1)).unwrap();
        control_tx.send(PollControl::Shutdown).unwrap();
        poller.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_overviews_still_reach_the_app() {
        let gateway = FakeGateway::default();
        // No scripted responses: every call errors.
        let (_control_tx, control_rx) = mpsc::unbounded_channel();
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let poller = tokio::spawn(run(gateway.clone(), control_rx, message_tx, 2));
        let Msg::Overview { result, .. } = recv(&mut message_rx).await else {
            panic!("expected an overview message");
        };
        assert!(result.is_err());
        drop(message_rx);
        drop(_control_tx);
        poller.await.unwrap();
    }
}
