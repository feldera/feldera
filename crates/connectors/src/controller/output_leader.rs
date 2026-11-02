//! Commits through one leader for distributed output connectors.
//!
//! A distributed output connector runs a part on every host of a multihost pipeline.  For a sink
//! that must commit each step at once, such as Delta Lake, each part writes its own data and then
//! reports it, for each committed transaction, to the leader, which is the part on the host that
//! the coordinator assigned to the connector's stream.  The leader waits for every part's report
//! of the step and then commits all of them together.  A part's report returns once that commit
//! is done, so no part counts its output as written before it is.
//!
//! | Host       | Role              | For each committed transaction at step S                     |
//! |------------|-------------------|--------------------------------------------------------------|
//! | The leader | [CommitLeader]    | Waits for the other parts' reports of S, commits, and answers |
//! | Others     | [PeerClient]      | Reports S to the leader and waits for the answer              |

use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Result as AnyResult, anyhow};
use feldera_storage::tokio::TOKIO;
use feldera_types::coordination::{OutputPartReport, Step};
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::snapshot_parts::PeerClient;

/// How many results of finished steps the leader keeps, to answer a report that a part repeats
/// after a network error cut off the answer.  A part that waits for the answer for step S cannot
/// report a later step, and the leader cannot finish a step without every part's report, so S is
/// still the last step that the leader finished when the part repeats its report.
const KEPT_RESULTS: usize = 1;

/// How long the leader waits for the other parts before it logs which ones it waits for.
const WAIT_WARNING: Duration = Duration::from_secs(60);

/// This host's part of a distributed output connector that commits through a leader.
#[derive(Clone)]
pub struct OutputPart {
    /// The name of the output connector.
    pub endpoint: String,

    /// The ordinal of this host.
    pub host: usize,

    /// The ordinal of the leader's host.
    pub leader_host: usize,

    /// What this part does.
    pub role: OutputPartRole,
}

#[derive(Clone)]
pub enum OutputPartRole {
    /// This part is the leader.
    Leader(Arc<CommitLeader>),

    /// This part reports to the leader through this client.
    Follower(PeerClient),
}

impl OutputPart {
    /// Commits `payload`, this part's output for the transaction that committed at `step`,
    /// together with the other parts' output.
    ///
    /// On the leader, `commit` receives every part's payload and commits them.  Elsewhere, the
    /// payload goes to the leader.  Either way, this returns once the leader's commit is done.
    ///
    /// While the leader waits a long time for the other parts, it calls `on_wait` with how long it
    /// has waited and the hosts that have reported, so that the connector can show the wait.
    pub fn commit(
        &self,
        step: Step,
        payload: JsonValue,
        shutdown: &CancellationToken,
        on_wait: &mut dyn FnMut(Duration, &[usize]),
        commit: impl FnOnce(Vec<JsonValue>) -> AnyResult<()>,
    ) -> AnyResult<()> {
        match &self.role {
            OutputPartRole::Leader(leader) => {
                let Some(mut payloads) = leader.gather(step, shutdown, on_wait) else {
                    return Err(anyhow!("the pipeline stopped before every part reported"));
                };
                payloads.insert(0, payload);
                let result = commit(payloads);
                leader.finish(
                    step,
                    result.as_ref().map(|_| ()).map_err(|e| format!("{e:#}")),
                );
                result
            }
            OutputPartRole::Follower(client) => {
                let report = OutputPartReport {
                    endpoint: self.endpoint.clone(),
                    host: self.host,
                    step,
                    payload,
                };
                TOKIO.block_on(async {
                    shutdown
                        .run_until_cancelled(client.report_output(self.leader_host, &report))
                        .await
                        .unwrap_or_else(|| {
                            Err(anyhow!("the pipeline stopped before the leader committed"))
                        })
                })
            }
        }
    }
}

/// The leader of a distributed output connector, which collects the other parts' reports.
pub struct CommitLeader {
    /// The number of parts other than the leader.
    n_followers: usize,

    state: Mutex<LeaderState>,

    /// Signals the leader's output thread when a report arrives.
    reported: Condvar,

    /// Counts finished steps, to wake the parts that wait for a result.
    finished: tokio::sync::watch::Sender<u64>,

    /// How long [Self::gather] waits between reports of a long wait.
    wait_warning: Duration,
}

#[derive(Default)]
struct LeaderState {
    /// The reports of steps not yet committed, by step and then by host.
    pending: BTreeMap<Step, BTreeMap<usize, JsonValue>>,

    /// The result of each recently finished step.
    results: BTreeMap<Step, Result<(), String>>,

    /// The last step the leader finished.
    last_finished: Option<Step>,

    /// Set when the leader's connector stops, after which every report fails.
    closed: bool,
}

/// The error of a report to a leader whose connector stopped.
const CLOSED: &str = "the leader's part of the connector stopped";

impl CommitLeader {
    /// Returns a leader for a connector with `n_hosts` parts, including the leader.
    pub fn new(n_hosts: usize) -> Self {
        Self {
            n_followers: n_hosts - 1,
            state: Mutex::new(LeaderState::default()),
            reported: Condvar::new(),
            finished: tokio::sync::watch::Sender::new(0),
            wait_warning: WAIT_WARNING,
        }
    }

    /// Records `host`'s report of `step` and waits until the leader finishes the step.  Returns
    /// the result of the leader's commit.
    pub async fn report(&self, host: usize, step: Step, payload: JsonValue) -> Result<(), String> {
        let mut finished = self.finished.subscribe();
        {
            let mut state = self.state.lock().unwrap();
            if let Some(result) = state.results.get(&step) {
                return result.clone();
            }
            if state.closed {
                return Err(CLOSED.into());
            }
            if state.last_finished.is_some_and(|last| step <= last) {
                return Err(format!(
                    "the leader finished step {step} without the report of host {host}"
                ));
            }
            state
                .pending
                .entry(step)
                .or_default()
                .entry(host)
                .or_insert(payload);
            self.reported.notify_all();
        }
        loop {
            {
                let state = self.state.lock().unwrap();
                if let Some(result) = state.results.get(&step) {
                    return result.clone();
                }
                if state.closed {
                    return Err(CLOSED.into());
                }
                // The leader finished the step and then dropped its result, as it does for a
                // step that it skipped (see [Self::finish]).
                if state.last_finished.is_some_and(|last| step <= last) {
                    return Err(format!(
                        "the leader finished step {step} without the report of host {host}"
                    ));
                }
            }
            if finished.changed().await.is_err() {
                return Err("the leader went away".into());
            }
        }
    }

    /// Waits until every other part has reported `step`, and returns their payloads.  Returns
    /// `None` if `shutdown` is cancelled first.
    ///
    /// After each warning interval of waiting, logs a warning and calls `on_wait` with how long it
    /// has waited and the hosts that have reported.
    pub fn gather(
        &self,
        step: Step,
        shutdown: &CancellationToken,
        on_wait: &mut dyn FnMut(Duration, &[usize]),
    ) -> Option<Vec<JsonValue>> {
        let start = Instant::now();
        let mut next_warning = self.wait_warning;
        let mut state = self.state.lock().unwrap();
        loop {
            let n_reports = state.pending.get(&step).map_or(0, |reports| reports.len());
            if n_reports >= self.n_followers {
                let reports = state.pending.remove(&step).unwrap_or_default();
                return Some(reports.into_values().collect());
            }
            if shutdown.is_cancelled() {
                return None;
            }
            if start.elapsed() >= next_warning {
                let reported: Vec<_> = state
                    .pending
                    .get(&step)
                    .map(|reports| reports.keys().copied().collect())
                    .unwrap_or_default();
                let waited = start.elapsed();
                warn!(
                    "waited {waited:?} for the parts of a distributed output connector to report step {step}; the hosts that reported are {reported:?}"
                );
                next_warning += self.wait_warning;
                drop(state);
                on_wait(waited, &reported);
                state = self.state.lock().unwrap();
                continue;
            }
            state = self
                .reported
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap()
                .0;
        }
    }

    /// Fails every waiting and later report, because the leader's connector stopped.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.pending.clear();
        drop(state);
        self.finished.send_modify(|n| *n += 1);
    }

    /// Records `result` as the outcome of `step` and wakes the parts that wait for it.
    ///
    /// The parts report the same steps as the leader, so a pending report of an earlier step
    /// means that something went wrong; it fails.
    pub fn finish(&self, step: Step, result: Result<(), String>) {
        let mut state = self.state.lock().unwrap();
        let earlier: Vec<Step> = state.pending.range(..step).map(|(s, _)| *s).collect();
        for s in earlier {
            state.pending.remove(&s);
            state.results.insert(
                s,
                Err(format!(
                    "the leader did not commit step {s}, but it committed step {step}"
                )),
            );
        }
        state.results.insert(step, result);
        while state.results.len() > KEPT_RESULTS {
            state.results.pop_first();
        }
        state.last_finished = Some(step);
        drop(state);
        self.finished.send_modify(|n| *n += 1);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    use super::CommitLeader;

    /// The leader gathers every part's report of a step, and each part gets the result of the
    /// leader's commit, including a part that repeats its report afterward.
    #[test]
    fn leader_gathers_and_answers() {
        let leader = Arc::new(CommitLeader::new(3));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let reports: Vec<_> = (1..3)
            .map(|host| {
                let leader = leader.clone();
                runtime.spawn(async move { leader.report(host, 7, json!(host)).await })
            })
            .collect();

        let mut payloads = leader
            .gather(7, &CancellationToken::new(), &mut |_, _| {})
            .unwrap();
        payloads.sort_by_key(|p| p.as_u64());
        assert_eq!(payloads, [json!(1), json!(2)]);
        leader.finish(7, Err("no room".into()));

        for report in reports {
            assert_eq!(runtime.block_on(report).unwrap(), Err("no room".into()));
        }
        assert_eq!(
            runtime.block_on(leader.report(1, 7, json!(1))),
            Err("no room".into())
        );
    }

    /// A report of a step that the leader already finished without it fails, and so does a
    /// pending report of a step that the leader skipped.
    #[test]
    fn mismatched_steps_fail() {
        let leader = Arc::new(CommitLeader::new(2));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let skipped = runtime.spawn({
            let leader = leader.clone();
            async move { leader.report(1, 4, json!(null)).await }
        });
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        leader.finish(5, Ok(()));
        assert!(runtime.block_on(skipped).unwrap().is_err());
        assert!(runtime.block_on(leader.report(1, 3, json!(null))).is_err());
        assert_eq!(runtime.block_on(leader.report(1, 5, json!(null))), Ok(()));
    }

    /// Closing the leader fails the reports that wait for it and the reports
    /// that come later.
    #[test]
    fn close_fails_reports() {
        let leader = Arc::new(CommitLeader::new(2));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let waiting = runtime.spawn({
            let leader = leader.clone();
            async move { leader.report(1, 4, json!(null)).await }
        });
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        leader.close();
        let within = |report| {
            runtime
                .block_on(async { tokio::time::timeout(Duration::from_secs(10), report).await })
                .expect("the report should fail, not wait")
        };
        assert!(within(waiting).unwrap().is_err());
        assert!(
            within(runtime.spawn({
                let leader = leader.clone();
                async move { leader.report(1, 5, json!(null)).await }
            }))
            .unwrap()
            .is_err()
        );
    }

    /// While the leader waits for parts that do not report, it calls `on_wait` with the hosts
    /// that did report, so that the connector can show the wait.
    #[test]
    fn gather_reports_a_long_wait() {
        let mut leader = CommitLeader::new(3);
        leader.wait_warning = Duration::from_millis(50);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let leader = Arc::new(leader);
        let _report = runtime.spawn({
            let leader = leader.clone();
            async move { leader.report(2, 1, json!(null)).await }
        });
        let shutdown = CancellationToken::new();
        // Stops the wait if `on_wait` is never called.
        runtime.spawn({
            let shutdown = shutdown.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                shutdown.cancel();
            }
        });
        let mut waits = Vec::new();
        let gathered = leader.gather(1, &shutdown, &mut |_, reported| {
            waits.push(reported.to_vec());
            if waits.len() == 2 {
                shutdown.cancel();
            }
        });
        assert!(gathered.is_none());
        assert_eq!(waits, [vec![2], vec![2]]);
    }

    /// The leader stops waiting when the pipeline stops.
    #[test]
    fn gather_stops_on_shutdown() {
        let leader = CommitLeader::new(2);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        assert!(leader.gather(1, &shutdown, &mut |_, _| {}).is_none());
    }
}
