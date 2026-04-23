use super::mock_framework::{STREAM_NAME, SUBJECT_NAME};
use super::util;
use crate::controller::{ControllerStatusContext, TransactionInfo};
use crate::test::{TestStruct, init_test_logger, test_circuit, wait};
use crate::{Controller, PipelineConfig};
use anyhow::Result as AnyResult;
use csv::ReaderBuilder as CsvReaderBuilder;
use feldera_types::memory_pressure::MemoryPressure;
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fs::create_dir, thread::sleep, time::Duration};
use tempfile::TempDir;

/// Timeout budget for a single FT round to transmit expected records.
const ROUND_TRANSMIT_TIMEOUT_MS: u128 = 10_000;

/// Atomic actions for declarative Controller-based FT tests.
///
/// Each test is expressed as a sequence of actions executed by
/// [`run_nats_controller_test`]. The runner maintains internal state (NATS
/// process, tokio runtime, storage dir, cumulative record counts, etc.)
/// and executes actions in order, printing a trace for easy debugging.
#[derive(Clone, Debug)]
pub(super) enum NatsControllerAction {
    /// Start a NATS server process. Stores guard + URL in runner state.
    StartNats,
    /// Create the JetStream stream (hardcoded stream="str", subject="sub").
    CreateStream,
    /// Purge all messages from the JetStream stream.
    PurgeStream,
    /// Delete the JetStream stream entirely.
    DeleteStream,

    /// Publish `n` records, start a Controller pipeline, wait for all
    /// outstanding records (including replayed from previous uncommitted
    /// rounds) to be transmitted, verify CSV output, optionally checkpoint,
    /// then stop the pipeline.
    RunFtCycle { publish: usize, checkpoint: bool },

    /// Start the pipeline in a background thread and assert startup enters an
    /// error path (retrying non-fatal or fatal) within timeout.
    ExpectStartupRetrying,

    /// Start the pipeline and assert startup fails fatally within timeout.
    ExpectStartupFatal,
}

/// Internal state held by the Controller FT test runner.
pub(super) struct NatsControllerRunner {
    // NATS process / runtime
    nats_guard: Option<util::ProcessKillGuard>,
    nats_url: Option<String>,
    rt: tokio::runtime::Runtime,

    // Temp FS state kept alive for duration of test
    _tempdir: TempDir,
    storage_dir: std::path::PathBuf,
    output_path: std::path::PathBuf,

    // Config knobs
    consumer_name: Option<String>,
    inactivity_timeout_secs: Option<u64>,

    // Parsed pipeline config (built lazily after NATS URL is known)
    pipeline_config: Option<PipelineConfig>,
    controller: Option<Controller>,

    // Cumulative record bookkeeping across rounds
    total_published: usize,
    checkpointed: usize,
}

impl NatsControllerRunner {
    pub(super) fn new() -> AnyResult<Self> {
        let tempdir = TempDir::new()?;
        let tempdir_path = tempdir.path();
        let storage_dir = tempdir_path.join("storage");
        create_dir(&storage_dir)?;
        let output_path = tempdir_path.join("output.csv");

        Ok(Self {
            nats_guard: None,
            nats_url: None,
            rt: tokio::runtime::Runtime::new()?,
            _tempdir: tempdir,
            storage_dir,
            output_path,
            consumer_name: None,
            inactivity_timeout_secs: None,
            pipeline_config: None,
            controller: None,
            total_published: 0,
            checkpointed: 0,
        })
    }

    pub(super) fn with_consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    pub(super) fn with_inactivity_timeout_secs(mut self, secs: u64) -> Self {
        self.inactivity_timeout_secs = Some(secs);
        self
    }

    fn nats_url(&self) -> &str {
        self.nats_url
            .as_deref()
            .expect("StartNats must be called before this action")
    }

    fn controller(&self) -> &Controller {
        self.controller
            .as_ref()
            .expect("StartPipeline must be called before this action")
    }

    /// Build (or return cached) the pipeline config from current runner state.
    fn pipeline_config(&mut self) -> &PipelineConfig {
        if self.pipeline_config.is_none() {
            let nats_url = self.nats_url();
            let storage_dir = &self.storage_dir;
            let output_path = &self.output_path;

            let consumer_name_line = self
                .consumer_name
                .as_deref()
                .map(|n| format!("name: {n}"))
                .unwrap_or_default();

            let inactivity_timeout_line = self
                .inactivity_timeout_secs
                .map(|s| format!("inactivity_timeout_secs: {s}"))
                .unwrap_or_default();

            let config_str = format!(
                r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: nats_input
            config:
                connection_config:
                    server_url: {nats_url}
                stream_name: {STREAM_NAME}
                {inactivity_timeout_line}
                consumer_config:
                    {consumer_name_line}
                    deliver_policy: All
                    filter_subjects: [{SUBJECT_NAME}]
        format:
            name: json
            config:
                update_format: raw
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
"#
            );

            self.pipeline_config = Some(serde_yaml::from_str(&config_str).unwrap());
        }
        self.pipeline_config.as_ref().unwrap()
    }

    fn publish_records(&mut self, publish: usize) -> AnyResult<()> {
        if publish == 0 {
            return Ok(());
        }

        println!(
            "Writing records {}..{}",
            self.total_published,
            self.total_published + publish
        );
        let start = self.total_published;
        let records: Vec<TestStruct> = (start..start + publish)
            .map(|id| TestStruct {
                id: id as u32,
                b: id % 2 == 0,
                i: Some(id as i64),
                s: format!("msg{}", id),
            })
            .collect();
        let nats_url = self.nats_url().to_string();
        self.rt
            .block_on(util::publish_json(&nats_url, SUBJECT_NAME, &records))?;
        self.total_published += publish;

        Ok(())
    }

    fn start_pipeline(&mut self) {
        assert!(
            self.controller.is_none(),
            "StartPipeline called while a controller is already running"
        );

        println!("start pipeline");
        let config = self.pipeline_config().clone();
        let controller = Controller::with_test_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(|e, _tag| {
                println!("Controller error: {e}");
                panic!("Controller error: {e}");
            }),
        )
        .unwrap();

        controller.start();
        self.controller = Some(controller);
    }

    fn wait_for_transmitted(&self) {
        let controller = self.controller();
        let total = self.total_published;
        let checkpointed = self.checkpointed;

        println!(
            "wait for {} records {checkpointed}..{total}",
            total - checkpointed
        );
        let mut last_n = 0;
        let result = wait(
            || {
                let n = controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records() as usize;

                if n > last_n {
                    println!("received {n} records of {total}");
                    last_n = n;
                }
                n >= total
            },
            ROUND_TRANSMIT_TIMEOUT_MS,
        );

        if let Err(()) = result {
            println!(
                "Controller status:\n{}",
                serde_json::to_string_pretty(&controller.status().to_api_type(
                    ControllerStatusContext {
                        suspend_error: Ok(()),
                        checkpoint_activity: feldera_types::checkpoint::CheckpointActivity::Idle,
                        permanent_checkpoint_errors: None,
                        pipeline_complete: false,
                        transaction_info: TransactionInfo::default(),
                        memory_pressure: MemoryPressure::default(),
                        memory_pressure_epoch: 0,
                        include_connector_errors: false,
                    },
                ))
                .unwrap()
            );
            panic!("Failed to receive expected records within timeout");
        }

        // No more records should arrive, but give the controller some
        // time to send some more in case there's a bug.
        sleep(Duration::from_millis(100));

        // Verify transmitted count is exact.
        assert_eq!(
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records(),
            total as u64
        );
    }

    fn checkpoint(&self) {
        println!("checkpoint");
        self.controller().checkpoint().unwrap();
    }

    fn stop_pipeline(&mut self) {
        println!("stop controller");
        self.controller
            .take()
            .expect("StartPipeline must be called before this action")
            .stop()
            .unwrap();
    }

    fn verify_output_since_checkpoint(&self) {
        let total = self.total_published;
        let checkpointed = self.checkpointed;

        let mut actual = CsvReaderBuilder::new()
            .has_headers(false)
            .from_path(&self.output_path)
            .unwrap()
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (val, weight) = res.unwrap();
                assert_eq!(weight, 1);
                val
            })
            .collect::<Vec<_>>();
        actual.sort_by_key(|item| item.id);

        assert_eq!(actual.len(), total - checkpointed);
        for (record, expect_record) in
            actual
                .into_iter()
                .zip((checkpointed..).map(|id| TestStruct {
                    id: id as u32,
                    b: id % 2 == 0,
                    i: Some(id as i64),
                    s: format!("msg{}", id),
                }))
        {
            assert_eq!(record, expect_record);
        }
    }

    fn mark_checkpointed(&mut self) {
        self.checkpointed = self.total_published;
    }

    fn exec(&mut self, action: &NatsControllerAction) -> AnyResult<()> {
        match action {
            NatsControllerAction::StartNats => {
                let (guard, url) = util::start_nats_and_get_address()?;
                self.nats_guard = Some(guard);
                self.nats_url = Some(url);
            }

            NatsControllerAction::CreateStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::create_stream(&nats_url, STREAM_NAME, SUBJECT_NAME))?;
            }

            NatsControllerAction::PurgeStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::purge_stream(&nats_url, STREAM_NAME))?;
                println!("Stream purged");
            }

            NatsControllerAction::DeleteStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::delete_stream(&nats_url, STREAM_NAME))?;
                // Invalidate cached pipeline config so a new stream URL
                // isn't stale if the stream is recreated.
                self.pipeline_config = None;
                println!("Stream deleted");
            }

            NatsControllerAction::RunFtCycle {
                publish,
                checkpoint,
            } => {
                let publish = *publish;
                let checkpoint = *checkpoint;

                self.publish_records(publish)?;
                self.start_pipeline();
                self.wait_for_transmitted();

                if checkpoint {
                    self.checkpoint();
                }

                self.stop_pipeline();
                self.verify_output_since_checkpoint();

                if checkpoint {
                    self.mark_checkpointed();
                }
            }

            NatsControllerAction::ExpectStartupRetrying => {
                assert!(
                    self.controller.is_none(),
                    "ExpectStartupRetrying requires no running controller"
                );

                let got_error = Arc::new(AtomicBool::new(false));
                let got_error_clone = got_error.clone();
                let config = self.pipeline_config().clone();
                let controller = Controller::with_test_config(
                    |circuit_config| {
                        Ok(test_circuit::<TestStruct>(
                            circuit_config,
                            &[],
                            &[Some("output")],
                        ))
                    },
                    &config,
                    Box::new(move |e, _tag| {
                        println!("Controller error: {e}");
                        got_error_clone.store(true, Ordering::Release);
                    }),
                )
                .unwrap();
                controller.start();

                let timeout_ms = self
                    .inactivity_timeout_secs
                    .map(|s| (s as u128 + 15) * 1000)
                    .unwrap_or(30_000);

                let result = wait(
                    || {
                        got_error.load(Ordering::Acquire)
                            || controller
                                .status()
                                .input_status()
                                .get(&0)
                                .unwrap()
                                .metrics
                                .num_transport_errors
                                .load(Ordering::Acquire)
                                > 0
                    },
                    timeout_ms,
                );

                if result.is_err() {
                    eprintln!(
                        "FAIL: Expected a startup error within {timeout_ms}ms, but no error was reported."
                    );
                    std::process::abort();
                }

                controller.stop().unwrap();
            }

            NatsControllerAction::ExpectStartupFatal => {
                assert!(
                    self.controller.is_none(),
                    "ExpectStartupFatal requires no running controller"
                );

                let got_fatal = Arc::new(AtomicBool::new(false));
                let got_fatal_clone = got_fatal.clone();
                let config = self.pipeline_config().clone();
                let controller = Controller::with_test_config(
                    |circuit_config| {
                        Ok(test_circuit::<TestStruct>(
                            circuit_config,
                            &[],
                            &[Some("output")],
                        ))
                    },
                    &config,
                    Box::new(move |e, _tag| {
                        println!("Controller error: {e}");
                        got_fatal_clone.store(true, Ordering::Release);
                    }),
                )
                .unwrap();
                controller.start();

                let timeout_ms = self
                    .inactivity_timeout_secs
                    .map(|s| (s as u128 + 15) * 1000)
                    .unwrap_or(30_000);

                let result = wait(|| got_fatal.load(Ordering::Acquire), timeout_ms);

                if result.is_err() {
                    eprintln!(
                        "FAIL: Expected a fatal startup error within {timeout_ms}ms, but none was reported."
                    );
                    std::process::abort();
                }

                controller.stop().unwrap();
            }
        }
        Ok(())
    }
}

pub(super) fn run_nats_controller_test(
    runner: NatsControllerRunner,
    actions: &[NatsControllerAction],
) -> AnyResult<()> {
    init_test_logger();

    let mut runner = runner;
    for (i, action) in actions.iter().enumerate() {
        println!("--- action {i}: {action:?} ---");
        runner.exec(action)?;
    }

    Ok(())
}

/// Convenience: run a controller FT test with a default runner (no special config).
pub(super) fn run_nats_ft_default(actions: &[NatsControllerAction]) {
    run_nats_controller_test(NatsControllerRunner::new().unwrap(), actions).unwrap();
}
