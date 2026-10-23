//! The NATS connectors in whole pipelines: fault-tolerant input across restarts, checkpoints,
//! and streams that are purged or deleted; and output from a pipeline to a stream.
//!
//! Each fault tolerance case is a sequence of `NatsControllerAction`s that
//! `run_nats_controller_test` executes against a private `nats-server`.

use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fs::create_dir, thread::sleep, time::Duration};

use anyhow::Result as AnyResult;
use feldera_adapterlib::test::{Updates, wait};
use feldera_types::memory_pressure::MemoryPressure;
use feldera_types::program_schema::Relation;
use futures::StreamExt as _;
use serde_json::{Value as JsonValue, json};
use tempfile::{NamedTempFile, TempDir};

use crate::Controller;
use crate::PipelineConfig;
use crate::controller::{ControllerStatusContext, TransactionInfo};
use crate::test::nats::{self as util, STREAM_NAME, SUBJECT_NAME};
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::{init_test_logger, published_schema, read, relations};
use crate::transport::nats::output::MAX_MESSAGE_ID_LEN;

/// The table, and view, of each case.
pub fn relation() -> Relation {
    relations::basic("nats_rows")
}

/// The circuit of the fault tolerance cases: the table `test_input1` and the view
/// `test_output1`, whose persistent id is `output`.
fn circuit() -> TestCircuit {
    TestCircuit::Identity {
        relation: relation(),
        persistent_output_ids: vec![Some("output".to_string())],
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
    }
}

/// Returns the record with `id`, as the fault tolerance cases publish it.
fn record(id: usize) -> JsonValue {
    json!({"id": id, "b": id.is_multiple_of(2), "i": id, "s": format!("msg{id}")})
}

/// Returns the records with `ids`, each inserted once.
fn records(ids: Range<usize>) -> Updates {
    let rows: Vec<String> = ids.map(|id| record(id).to_string()).collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(&relation()), &rows).unwrap()
}

/// Timeout budget for a single FT round to transmit expected records.
const ROUND_TRANSMIT_TIMEOUT_MS: u64 = 10_000;

/// Atomic actions for the declarative fault tolerance cases of these tests.
///
/// Each test is expressed as a sequence of actions executed by
/// [`run_nats_controller_test`]. The runner maintains internal state (NATS
/// process, tokio runtime, storage dir, cumulative record counts, etc.)
/// and executes actions in order, printing a trace for easy debugging.
#[derive(Clone, Debug)]
enum NatsControllerAction {
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
struct NatsControllerRunner<'a> {
    engine: &'a dyn PipelineTestEngine,

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

impl<'a> NatsControllerRunner<'a> {
    fn new(engine: &'a dyn PipelineTestEngine) -> AnyResult<Self> {
        let tempdir = TempDir::new()?;
        let tempdir_path = tempdir.path();
        let storage_dir = tempdir_path.join("storage");
        create_dir(&storage_dir)?;
        let output_path = tempdir_path.join("output.csv");

        Ok(Self {
            engine,
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

    fn with_consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    fn with_inactivity_timeout_secs(mut self, secs: u64) -> Self {
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

            let mut config = json!({
                "name": "test",
                "workers": 4,
                "storage_config": {
                    "path": storage_dir,
                },
                "storage": true,
                "fault_tolerance": {},
                "clock_resolution_usecs": null,
                "inputs": {
                    "test_input1": {
                        "stream": "test_input1",
                        "transport": {
                            "name": "nats_input",
                            "config": {
                                "connection_config": {
                                    "server_url": nats_url,
                                },
                                "stream_name": STREAM_NAME,
                                "consumer_config": {
                                    "deliver_policy": "All",
                                    "filter_subjects": [SUBJECT_NAME],
                                },
                            },
                        },
                        "format": {
                            "name": "json",
                            "config": {
                                "update_format": "raw",
                            },
                        },
                    },
                },
                "outputs": {
                    "test_output1": {
                        "stream": "test_output1",
                        "transport": {
                            "name": "file_output",
                            "config": {
                                "path": output_path,
                            },
                        },
                        "format": {
                            "name": "csv",
                        },
                    },
                },
            });
            let transport_config = &mut config["inputs"]["test_input1"]["transport"]["config"];
            if let Some(secs) = self.inactivity_timeout_secs {
                transport_config["inactivity_timeout_secs"] = json!(secs);
            }
            if let Some(name) = &self.consumer_name {
                transport_config["consumer_config"]["name"] = json!(name);
            }

            self.pipeline_config = Some(serde_json::from_value(config).unwrap());
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
        let records: Vec<JsonValue> = (start..start + publish).map(record).collect();
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
        let controller = self
            .engine
            .controller(
                &circuit(),
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
                        disk_usage: None,
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

        let actual =
            read::csv_updates(&relation(), &std::fs::read(&self.output_path).unwrap()).unwrap();
        assert!(actual.weights().iter().all(|weight| *weight == 1));
        assert_eq!(actual.len(), total - checkpointed);
        assert_eq!(
            actual.zset().unwrap(),
            records(checkpointed..total).zset().unwrap()
        );
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
                let controller = self
                    .engine
                    .controller(
                        &circuit(),
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
                    .map(|s| (s + 15) * 1000)
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
                let controller = self
                    .engine
                    .controller(
                        &circuit(),
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
                    .map(|s| (s + 15) * 1000)
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

fn run_nats_controller_test(
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
fn run_nats_ft_default(engine: &dyn PipelineTestEngine, actions: &[NatsControllerAction]) {
    run_nats_controller_test(NatsControllerRunner::new(engine).unwrap(), actions).unwrap();
}

// ---------------------------------------------------------------------------
// Fault Tolerance Cycles
// ---------------------------------------------------------------------------

/// One round delivers its records and checkpoints.
pub fn ft_simple(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
        ],
    );
}

/// Restarts after checkpoints deliver each record once.
pub fn ft_with_checkpoints(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 10,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 20,
                checkpoint: true,
            },
        ],
    );
}

/// Restarts without checkpoints replay every record.
pub fn ft_without_checkpoints(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 20,
                checkpoint: false,
            },
        ],
    );
}

/// Rounds with and without checkpoints alternate, starting with a checkpoint.
pub fn ft_alternating(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 10,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 20,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: true,
            },
        ],
    );
}

/// The first round has no records and no checkpoint.
pub fn ft_initially_zero_without_checkpoint(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 0,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 20,
                checkpoint: true,
            },
        ],
    );
}

/// The first round has no records and a checkpoint.
pub fn ft_initially_zero_with_checkpoint(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 15,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: false,
            },
            RunFtCycle {
                publish: 20,
                checkpoint: true,
            },
        ],
    );
}

/// Rounds without records checkpoint between rounds with records.
pub fn ft_empty_step_checkpoint(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_ft_default(
        engine,
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 10,
                checkpoint: true,
            },
        ],
    );
}

// ---------------------------------------------------------------------------
// Fault Tolerance Replay and Stream Lifecycle
// ---------------------------------------------------------------------------

/// Tests that replay fails with an error (rather than looping forever) when the
/// stream has been purged and the checkpointed messages no longer exist.
///
/// Scenario:
/// 1. Publish 5 messages, run pipeline, checkpoint (sequences committed)
/// 2. Publish 5 more messages, consume them but do NOT checkpoint, stop
/// 3. Purge the stream (all messages deleted)
/// 4. Start pipeline: the FT framework sends a Replay for the uncommitted
///    sequences, but those messages are gone -> expect fatal error
pub fn ft_replay_after_stream_purge(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_inactivity_timeout_secs(1),
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 5,
                checkpoint: false,
            },
            PurgeStream,
            ExpectStartupFatal,
        ],
    )
    .unwrap();
}

/// Tests rapid restart+replay with a named consumer.
///
/// This reproduces a bug where the previous ordered consumer hadn't expired yet,
/// causing "consumer already exists" errors. The fix generates unique consumer names
/// by appending a UUID suffix when a name is explicitly configured.
pub fn ft_with_named_consumer(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_consumer_name("my_named_consumer"),
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
        ],
    )
    .unwrap();
}

/// Checkpoint, delete+recreate stream, then restart. Replay should fail
/// because the committed sequence numbers no longer exist in the new stream.
pub fn ft_stream_deletion_and_recreation(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_inactivity_timeout_secs(1),
        &[
            StartNats,
            CreateStream,
            // Round 1: publish 5 records, consume and checkpoint.
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            // Round 2: publish 5 more, consume but do NOT checkpoint.
            RunFtCycle {
                publish: 5,
                checkpoint: false,
            },
            // Delete and recreate the stream: all old messages are gone.
            DeleteStream,
            CreateStream,
            // Restart: FT framework tries to replay uncommitted sequences,
            // but they don't exist in the fresh stream -> fatal startup error.
            ExpectStartupFatal,
        ],
    )
    .unwrap();
}

/// Checkpoint, delete+recreate stream, publish new data, and restart.
/// Even with a fully checkpointed round, the checkpoint resume sequence no
/// longer matches the recreated stream and startup should fail fast.
pub fn ft_stream_deletion_after_full_checkpoint(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine).unwrap(),
        &[
            StartNats,
            CreateStream,
            // Round 1: publish and checkpoint everything.
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            // Delete and recreate stream, but everything was checkpointed,
            // so nothing needs replaying.
            DeleteStream,
            CreateStream,
            // Resume metadata points at old stream sequence space; startup fails fatally.
            ExpectStartupFatal,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Startup Behavior
// ---------------------------------------------------------------------------

/// Startup should enter retrying error mode when the server is up but the
/// configured stream has not been created yet.
pub fn ft_startup_retries_when_stream_missing(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_inactivity_timeout_secs(1),
        &[
            StartNats,
            // Intentionally skip CreateStream to force startup retry mode.
            ExpectStartupRetrying,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// The full pipeline with the default json output format and message ids:
/// records flow through the encoder and the endpoint to the stream bound to
/// the subject, and every message carries a `Nats-Msg-Id` header the
/// connector derived from the payload without knowing its format.
pub fn output_pipeline_end_to_end(engine: &dyn PipelineTestEngine) {
    check_output_pipeline_end_to_end(engine).unwrap();
}

fn check_output_pipeline_end_to_end(engine: &dyn PipelineTestEngine) -> anyhow::Result<()> {
    init_test_logger();

    let rt = tokio::runtime::Runtime::new()?;
    let (guard, addr) = util::start_nats_and_get_address()?;
    let _guard = guard;
    rt.block_on(util::create_stream(&addr, "str", "sub"))?;

    // Two inserts followed by a delete of the first record.
    let input_file = NamedTempFile::new()?;
    input_file.as_file().write_all(
        br#"{"insert":{"id":1,"b":true,"s":"first"}}
{"insert":{"id":2,"b":false,"s":"second"}}
{"delete":{"id":1,"b":true,"s":"first"}}
"#,
    )?;

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "file1": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": input_file.path(),
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete"
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "nats_output",
            "config": {
              "connection_config": {
                "server_url": addr,
              },
              "subject": "sub"
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete"
            }
          }
        }
      }
    }))?;

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = engine.controller(
        &TestCircuit::identity(relation()),
        &config,
        Box::new(move |e, _| {
            err_sender
                .send(format!("nats_output_test: error: {e}"))
                .unwrap()
        }),
    )?;

    controller.start();
    controller.start_input_endpoint("file1")?;

    // Three records in, three records out.
    wait(
        || controller.status().num_total_processed_records() == 3 || !err_receiver.is_empty(),
        10_000,
    )
    .expect("timeout");

    assert!(
        err_receiver.is_empty(),
        "pipeline reported errors: {:?}",
        err_receiver.try_iter().collect::<Vec<_>>()
    );

    let messages = rt.block_on(async {
        let client = async_nats::connect(&addr).await?;
        let js = async_nats::jetstream::new(client);
        let stream = js.get_stream("str").await?;
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                filter_subject: "sub".into(),
                ..Default::default()
            })
            .await?;
        let mut messages = Vec::new();
        let mut stream_messages = consumer.batch().max_messages(10).messages().await?;
        loop {
            // Read until a 5-second idle gap: the number of messages the
            // connector published is what this test asserts on, so the
            // reader cannot assume it up front. The gap is generous because
            // output delivery runs on the endpoint thread, trailing the
            // circuit's processed-records count the test waits on.
            match tokio::time::timeout(Duration::from_secs(5), stream_messages.next()).await {
                Ok(Some(Ok(message))) => messages.push(message),
                Ok(Some(Err(e))) => return Err(anyhow::anyhow!("NATS receive failed: {e}")),
                // Batch ended: the server had no more messages to send.
                Ok(None) => break,
                // No message for 1s: the connector has published everything
                // it is going to; the pipeline is idle.
                Err(_) => break,
            }
        }
        Ok::<_, anyhow::Error>(messages)
    })?;

    // The test circuit materializes the input zset: the batch the encoder
    // sees is the net change of the view, so `+1, +2, -1` collapses to the
    // single record `+2`, which the encoder emits in one buffer.
    assert_eq!(
        messages.len(),
        1,
        "the net change of one record must arrive as one message, got: {:?}",
        messages
            .iter()
            .map(|m| String::from_utf8_lossy(&m.payload).to_string())
            .collect::<Vec<_>>()
    );
    let record: serde_json::Value = serde_json::from_slice(&messages[0].payload)?;
    assert_eq!(record["insert"]["s"], "second");
    let message_id = messages[0]
        .headers
        .as_ref()
        .and_then(|headers| headers.get(async_nats::header::NATS_MESSAGE_ID))
        .expect("message must carry a Nats-Msg-Id header")
        .as_str();
    assert!(
        message_id.len() <= MAX_MESSAGE_ID_LEN && message_id.matches('-').count() == 2,
        "unexpected message id shape: {message_id}"
    );

    controller.stop()?;
    Ok(())
}
