//! Finds preprocessor calls that run longer than the maximum allowed call
//! duration.
//!
//! Each preprocessor instance has a
//! [TransitionCounter](feldera_adapterlib::utils::transition_counter::TransitionCounter).
//! The counter changes when each `process_with_metadata` call starts and
//! when it returns.  Calling the preprocessor does not involve reading the clock.
//!
//! The statistics thread calls [PreprocessorStalls::check] periodically.
//! Its [PreprocessorWatchdog]s read the counters of each input endpoint.
//! If a counter shows a call in progress, and has the same value as at the
//! previous check, the call has not returned since the previous check.  When a
//! call has been in progress for longer than the limit,
//! the watchdog marks the endpoint unhealthy and logs a warning, repeated
//! each time the elapsed time doubles.  As soon as the late calls return, the
//! check clears the stall and logs that the call returned.
//!
//! The period of the check is by default one second.  The check measures the
//! duration of a call from the first check that sees the call in progress, so
//! the measured duration is less than the real duration by up to one period.
//! So the check can miss a call that runs only a little longer than the limit,
//! but it never reports a call that has run for less than the limit.  A late
//! call can also return right after the check reads its counter.  The stall is
//! then reported until the next check.  The check does not interrupt an
//! ongoing call.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use feldera_adapterlib::preprocess::PreprocessorWatchdog;
use feldera_adapterlib::utils::long_operation::LongOperationWarning;
use feldera_adapterlib::utils::transition_counter::Transitions;
use feldera_types::duration::ConfigDuration;
use tracing::{info, warn};

use super::stats::InputEndpointStatus;
use super::{ControllerInner, EndpointId};

/// One preprocessor instance's call counter, recorded at the last check.
struct CallState {
    /// The value of the counter at the last check.
    transitions: Transitions,

    /// The first time we saw `transitions`.
    since: Instant,
}

/// The calls to one input endpoint's preprocessor.
#[derive(Default)]
struct EndpointCalls {
    /// One [CallState] for each instance of the preprocessor, recorded at the
    /// last check.
    calls: Vec<CallState>,

    /// Set only while a call runs longer than the limit.  The
    /// `Instant` is the start of the oldest call in progress.
    ///
    /// One stall describes all the late calls of one endpoint, because the
    /// health and the warnings are per endpoint, not for each call.
    /// When the oldest late call returns and another late call continues,
    /// the warnings start again from the start of the next oldest call.
    stall: Option<(Instant, LongOperationWarning)>,
}

impl EndpointCalls {
    /// Updates `calls` based on the state of the `watchdog` counters.
    fn update(&mut self, now: Instant, watchdog: &PreprocessorWatchdog) {
        // Map key: a counter value at the previous check.  Map value: the
        // first time that value was seen.
        let previous: HashMap<Transitions, Instant> = std::mem::take(&mut self.calls)
            .into_iter()
            .map(|state| (state.transitions, state.since))
            .collect();
        self.calls = watchdog
            .readers()
            .into_iter()
            .filter_map(|reader| {
                let transitions = reader.get()?;
                let since = previous.get(&transitions).copied().unwrap_or(now);
                Some(CallState { transitions, since })
            })
            .collect();
    }

    /// Logs a stall and records it in `endpoint` while a call is in progress
    /// for longer than `limit`, and clears it afterward.  `host` is the index
    /// of this host in a multihost pipeline.
    fn report(
        &mut self,
        now: Instant,
        endpoint: &InputEndpointStatus,
        watchdog: &PreprocessorWatchdog,
        limit: Duration,
        host: Option<usize>,
    ) {
        let is_overdue = |state: &&CallState| {
            state.transitions.in_call() && now.saturating_duration_since(state.since) >= limit
        };
        let Some(oldest) = self
            .calls
            .iter()
            .filter(is_overdue)
            .map(|state| state.since)
            .min()
        else {
            if self.stall.take().is_some() {
                *endpoint.preprocessor_stall.lock().unwrap() = None;
                info!(
                    "input connector '{}': the calls to preprocessor '{}' that ran longer than {} have returned",
                    endpoint.endpoint_name,
                    watchdog.name(),
                    ConfigDuration::from(limit)
                );
            }
            return;
        };

        let n_overdue = self.calls.iter().filter(is_overdue).count();
        let host = host.map_or(String::new(), |host| format!(" on host {host}"));
        let description = format!(
            "preprocessor '{}'{host}: {n_overdue} of {} instances have a call in progress for more than {}",
            watchdog.name(),
            self.calls.len(),
            ConfigDuration::from(limit)
        );
        *endpoint.preprocessor_stall.lock().unwrap() = Some(description.clone());

        if self
            .stall
            .as_ref()
            .is_none_or(|(start, _warning)| *start != oldest)
        {
            self.stall = Some((oldest, LongOperationWarning::new_at(oldest, limit)));
        }
        let (_start, warning) = self.stall.as_mut().unwrap();
        warning.check_at(now, |elapsed| {
            warn!(
                "input connector '{}': {description}; the oldest call has run for {} seconds",
                endpoint.endpoint_name,
                elapsed.as_secs()
            )
        });
    }
}

/// Finds preprocessor calls that run longer than the maximum allowed call
/// duration, logs warnings about them, and makes their input endpoints
/// unhealthy.
#[derive(Default)]
pub(super) struct PreprocessorStalls {
    /// Map key: input endpoint id.  Map value: the calls to the endpoint's
    /// preprocessor.
    endpoints: HashMap<EndpointId, EndpointCalls>,
}

impl PreprocessorStalls {
    /// Checks the preprocessor calls of all `endpoints` at time `now`.  `host`
    /// is the index of this host in a multihost pipeline.
    fn check_at(
        &mut self,
        now: Instant,
        endpoints: &BTreeMap<EndpointId, InputEndpointStatus>,
        host: Option<usize>,
    ) {
        self.endpoints.retain(|id, _| endpoints.contains_key(id));
        for (id, endpoint) in endpoints {
            let Some(watchdog) = &endpoint.preprocessor_watchdog else {
                continue;
            };
            let Some(limit) = watchdog.max_allowed_call_duration() else {
                continue;
            };
            let endpoint_calls = self.endpoints.entry(*id).or_default();
            endpoint_calls.update(now, watchdog);
            endpoint_calls.report(now, endpoint, watchdog, limit, host);
        }
    }

    /// Checks the preprocessor calls of all input endpoints of `controller`.
    pub(super) fn check(&mut self, controller: &ControllerInner) {
        let layout = &controller.layout;
        let host = layout.is_multihost().then(|| layout.local_host_idx());
        self.check_at(Instant::now(), &controller.status.input_status(), host);
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use feldera_adapterlib::format::{
        MessageOrientedPreprocessedParser, Parser, StreamingPreprocessedParser,
    };
    use feldera_adapterlib::preprocess::{Preprocessor, PreprocessorWatchdog};
    use feldera_types::adapter_stats::{ConnectorHealth, ConnectorHealthStatus};
    use feldera_types::config::{FormatConfig, InputEndpointConfig};
    use feldera_types::program_schema::Relation;
    use serde_json::{Value, json};

    use super::PreprocessorStalls;
    use crate::controller::EndpointId;
    use crate::controller::stats::InputEndpointStatus;
    use crate::format::{ParseError, Splitter};
    use crate::test::{TestStruct, mock_parser_pipeline};

    type Endpoints = BTreeMap<EndpointId, InputEndpointStatus>;

    /// Passes data through, and runs a callback while each call is in
    /// progress.
    struct CallbackPreprocessor(Arc<dyn Fn() + Send + Sync>);

    impl Preprocessor for CallbackPreprocessor {
        fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
            (self.0)();
            (data.to_vec(), vec![])
        }

        fn fork(&self) -> Box<dyn Preprocessor> {
            Box::new(CallbackPreprocessor(self.0.clone()))
        }

        fn splitter(&self) -> Option<Box<dyn Splitter>> {
            None
        }
    }

    fn unhealthy_description(health: Option<ConnectorHealth>) -> Option<String> {
        health.and_then(|health| match health.status {
            ConnectorHealthStatus::Healthy => None,
            ConnectorHealthStatus::Unhealthy => health.description,
        })
    }

    /// One input endpoint, with id 0, whose preprocessor `callback` has
    /// `limit` as its `max_allowed_call_duration`.
    fn endpoints(limit: Value) -> (PreprocessorWatchdog, Endpoints) {
        let config: InputEndpointConfig = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {"name": "file_input", "config": {"path": "unused"}},
            "format": {"name": "csv"},
            "preprocessor": [{
                "name": "callback",
                "message_oriented": true,
                "config": {},
                "max_allowed_call_duration": limit
            }]
        }))
        .unwrap();
        let watchdog =
            PreprocessorWatchdog::new(&config.connector_config.preprocessor.as_ref().unwrap()[0]);
        let endpoint =
            InputEndpointStatus::new("test_input", config, None, None, Some(watchdog.clone()));
        (watchdog, [(0, endpoint)].into())
    }

    /// Wraps `preprocessor` and a CSV parser in the parser for a
    /// message-oriented or a streaming preprocessor.
    fn preprocessed_parser(
        preprocessor: Box<dyn Preprocessor>,
        watchdog: PreprocessorWatchdog,
        message_oriented: bool,
    ) -> Box<dyn Parser> {
        let (_consumer, parser, _input_handle) = mock_parser_pipeline::<TestStruct, TestStruct>(
            &Relation::empty(),
            &FormatConfig {
                name: Cow::from("csv"),
                config: json!({}),
            },
        )
        .unwrap();
        let parser = Box::new(parser);
        if message_oriented {
            Box::new(MessageOrientedPreprocessedParser::new(
                preprocessor,
                parser,
                watchdog,
            ))
        } else {
            Box::new(StreamingPreprocessedParser::new(
                preprocessor,
                parser,
                watchdog,
            ))
        }
    }

    /// A preprocessor call that runs longer than `max_allowed_call_duration`
    /// makes the endpoint unhealthy until the call returns.  The stall does
    /// not replace the health that the connector reports.
    fn stalled_call_makes_endpoint_unhealthy(message_oriented: bool) {
        let (watchdog, endpoints) = endpoints(json!("10s"));
        endpoints[&0].update_health(ConnectorHealth::unhealthy("connector error"));
        let stalls = Arc::new(Mutex::new((PreprocessorStalls::default(), endpoints)));

        // While the call is in progress, check at 0 s, 9 s, and 10 s after
        // the first check, and record the endpoint's health after each check.
        let start = Instant::now();
        let during_call = Arc::new(Mutex::new(Vec::new()));
        let callback = {
            let stalls = stalls.clone();
            let during_call = during_call.clone();
            move || {
                let (stalls, endpoints) = &mut *stalls.lock().unwrap();
                for secs in [0, 9, 10] {
                    stalls.check_at(start + Duration::from_secs(secs), endpoints, None);
                    during_call
                        .lock()
                        .unwrap()
                        .push(unhealthy_description(endpoints[&0].health()));
                }
            }
        };

        let mut parser = preprocessed_parser(
            Box::new(CallbackPreprocessor(Arc::new(callback))),
            watchdog,
            message_oriented,
        );
        parser.parse(b"", None);

        let stall =
            "preprocessor 'callback': 1 of 1 instances have a call in progress for more than 10s";
        assert_eq!(
            *during_call.lock().unwrap(),
            [
                Some("connector error".to_string()),
                Some("connector error".to_string()),
                Some(format!("{stall}; connector error")),
            ]
        );

        // After the call returns, the endpoint has its own health again.
        let (stalls, endpoints) = &mut *stalls.lock().unwrap();
        stalls.check_at(start + Duration::from_secs(11), endpoints, None);
        assert_eq!(
            unhealthy_description(endpoints[&0].health()),
            Some("connector error".to_string())
        );
    }

    #[test]
    fn stalled_call_message_oriented() {
        stalled_call_makes_endpoint_unhealthy(true);
    }

    #[test]
    fn stalled_call_streaming() {
        stalled_call_makes_endpoint_unhealthy(false);
    }

    /// Each fork of a preprocessed parser adds a counter to the watchdog, and
    /// the watchdog forgets the counter of a dropped fork.
    #[test]
    fn forks_add_and_remove_counters() {
        for message_oriented in [true, false] {
            let (watchdog, _endpoints) = endpoints(json!("10s"));
            let parser = preprocessed_parser(
                Box::new(CallbackPreprocessor(Arc::new(|| {}))),
                watchdog.clone(),
                message_oriented,
            );
            assert_eq!(watchdog.readers().len(), 1);
            let fork = parser.fork();
            assert_eq!(watchdog.readers().len(), 2);
            drop(fork);
            assert_eq!(watchdog.readers().len(), 1);
        }
    }

    /// Short calls do not make the endpoint unhealthy, even when a call is in
    /// progress at each check.
    #[test]
    fn short_calls_do_not_stall() {
        let (watchdog, endpoints) = endpoints(json!("10s"));
        let mut counter = watchdog.new_counter();
        let mut stalls = PreprocessorStalls::default();
        let start = Instant::now();

        // Each check runs during a different call.
        for secs in [0, 10, 20] {
            counter.count(|| stalls.check_at(start + Duration::from_secs(secs), &endpoints, None));
            assert_eq!(unhealthy_description(endpoints[&0].health()), None);
        }
    }

    /// The endpoint is unhealthy while any instance has a late call, and the
    /// description counts the instances with a late call.
    #[test]
    fn late_calls_on_two_instances() {
        let (watchdog, endpoints) = endpoints(json!("10s"));
        let mut first = watchdog.new_counter();
        let mut second = watchdog.new_counter();
        let mut stalls = PreprocessorStalls::default();
        let start = Instant::now();
        let mut check = |secs| {
            stalls.check_at(start + Duration::from_secs(secs), &endpoints, None);
            unhealthy_description(endpoints[&0].health())
        };

        // Both calls start before the check at 0 s.  The second call returns
        // after the check at 10 s, and the first after the check at 11 s.
        let (both_late, first_late) = first.count(|| {
            let both_late = second.count(|| {
                check(0);
                check(10)
            });
            (both_late, check(11))
        });
        let none_late = check(12);

        let stall = |n| {
            Some(format!(
                "preprocessor 'callback': {n} of 2 instances have a call in progress for more than 10s"
            ))
        };
        assert_eq!(both_late, stall(2));
        assert_eq!(first_late, stall(1));
        assert_eq!(none_late, None);
    }

    /// A zero limit acts as a limit of 1 second.
    #[test]
    fn zero_limit_is_one_second() {
        let (watchdog, endpoints) = endpoints(json!("0s"));
        let mut counter = watchdog.new_counter();
        let mut stalls = PreprocessorStalls::default();
        let start = Instant::now();
        let mut check = |secs| {
            stalls.check_at(start + Duration::from_secs(secs), &endpoints, None);
            unhealthy_description(endpoints[&0].health())
        };

        let (at_0s, at_1s) = counter.count(|| (check(0), check(1)));
        assert_eq!(at_0s, None);
        assert_eq!(
            at_1s,
            Some(
                "preprocessor 'callback': 1 of 1 instances have a call in progress for more than 1s"
                    .to_string()
            )
        );
    }

    /// A `null` limit disables the check.
    #[test]
    fn null_limit_disables_the_check() {
        let (watchdog, endpoints) = endpoints(json!(null));
        let mut counter = watchdog.new_counter();
        let mut stalls = PreprocessorStalls::default();
        let start = Instant::now();

        counter.count(|| {
            for secs in [0, 100] {
                stalls.check_at(start + Duration::from_secs(secs), &endpoints, None);
                assert_eq!(unhealthy_description(endpoints[&0].health()), None);
            }
        });
    }
}
