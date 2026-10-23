//! The controller's output connectors that pause and start: they drop the output they
//! receive while paused, defer the snapshot they owe, flush their buffers, lift
//! backpressure, and keep their state across a restart.

use std::fs::{File, create_dir};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use feldera_adapterlib::test::wait;
use feldera_types::config::{OutputEndpointConfig, PipelineConfig};
use serde_json::json;
use tempfile::TempDir;

use crate::test::init_test_logger;
use crate::test::pipeline::PipelineTestEngine;
use crate::test::suites::controller::{
    BlockedOutputEndpoint, append_input, check_file_contents, check_file_ids, identity,
    output_endpoint_metrics, wait_for_records,
};
use crate::{Controller, OutputEndpoint};

/// Configuration for the output-connector pause tests: one input file feeding a
/// view with two output connectors, `test_output1` and `test_output2`.  The
/// tests pause `test_output1`; `test_output2` keeps running and shows what the
/// pipeline produced meanwhile.
fn output_pause_config(
    input_path: &Path,
    output1_path: &Path,
    output2_path: &Path,
    storage_dir: &Path,
    output1_paused: bool,
) -> PipelineConfig {
    let output = |path: &Path, paused: bool| {
        json!({
            "stream": "test_output1",
            "paused": paused,
            "transport": {
                "name": "file_output",
                "config": { "path": path.display().to_string() },
            },
            "format": { "name": "csv", "config": {} },
        })
    };
    serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "fault_tolerance": {},
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path.display().to_string(),
                        "follow": true,
                    },
                },
                "format": { "name": "csv" },
            },
        },
        "outputs": {
            "test_output1": output(output1_path, output1_paused),
            "test_output2": output(output2_path, false),
        },
    }))
    .unwrap()
}

/// Starts a controller for [output_pause_config].
fn start_output_pause_controller(
    engine: &dyn PipelineTestEngine,
    config: &PipelineConfig,
) -> Controller {
    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();
    wait(|| !controller.is_replaying(), 10_000).unwrap();
    controller
}

/// A paused output connector drops the output it receives on the floor: the
/// records never reach its sink and its `transmitted_records` stops advancing.
/// The output is still accounted as processed, so the pipeline's progress does
/// not depend on a paused connector, and the connector queues nothing while it
/// is paused.
///
/// Output produced while the connector was paused is gone for good: starting it
/// again resumes with the next output the pipeline produces.
pub fn output_connector_pause_drops_output(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        false,
    );

    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    wait_for_records(&controller, &[100, 100]);
    assert!(
        !controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );

    // Pause `test_output1` and feed the pipeline another 100 records. Only
    // `test_output2` receives them; `wait_for_records` fails the test if
    // `test_output1` transmits any of them.
    controller.pause_output_endpoint("test_output1").unwrap();
    assert!(
        controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[100, 200]);

    // The dropped output still counts as processed. `wait_for_records` returned
    // when the running connector transmitted its last record, and the two
    // connectors drain independently, so wait for the paused one to get there
    // rather than sampling it at that instant.
    let processed = controller
        .status()
        .global_metrics
        .num_total_processed_records();
    wait(
        || {
            output_endpoint_metrics(&controller, "test_output1").total_processed_input_records
                == processed
        },
        10_000,
    )
    .unwrap();

    // Nothing accumulates in the paused connector's queue: the entries it
    // receives carry no records.
    let paused = output_endpoint_metrics(&controller, "test_output1");
    assert_eq!(paused.queued_records, 0);
    assert_eq!(paused.transmitted_records, 100);

    // Start the connector again: it picks up with the output produced from now
    // on. The records dropped while it was paused never arrive.
    controller.start_output_endpoint("test_output1").unwrap();
    assert!(
        !controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );
    append_input(&input_path, 200..300);
    wait_for_records(&controller, &[200, 300]);
    controller.stop().unwrap();

    check_file_ids(&output1_path, (0..100).chain(200..300));
    check_file_contents(&output2_path, 0..300);
}

/// A connector configured with `paused: true` starts out paused and transmits
/// nothing until it is started.
pub fn output_connector_paused_on_startup(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        true,
    );

    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    assert!(
        controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );
    wait_for_records(&controller, &[0, 100]);

    controller.start_output_endpoint("test_output1").unwrap();
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[100, 200]);
    controller.stop().unwrap();

    check_file_ids(&output1_path, 100..200);
    check_file_contents(&output2_path, 0..200);
}

/// Pausing a connector flushes what its output buffer already holds. Those
/// updates were produced before the pause, so they are still owed to the sink,
/// and the pipeline's progress counter for the endpoint waits on them.
///
/// Nothing else would flush them: a paused connector receives no more output to
/// grow the buffer past its size limit, and the time limit is not configured
/// here (nor by default).
pub fn output_connector_pause_flushes_the_output_buffer(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let mut config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        false,
    );
    config
        .outputs
        .get_mut("test_output1")
        .unwrap()
        .connector_config
        .output_buffer_config
        .enable_output_buffer = true;

    // The buffered connector holds its output; the unbuffered one shows that
    // the pipeline produced it.
    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    wait_for_records(&controller, &[0, 100]);

    // Pausing flushes the buffer.
    controller.pause_output_endpoint("test_output1").unwrap();
    wait_for_records(&controller, &[100, 100]);

    // Output produced from now on is dropped, and the endpoint keeps up with
    // the pipeline instead of stalling behind a buffer that never flushes.
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[100, 200]);
    let processed = controller
        .status()
        .global_metrics
        .num_total_processed_records();
    wait(
        || {
            let paused = output_endpoint_metrics(&controller, "test_output1");
            paused.total_processed_input_records == processed && paused.buffered_records == 0
        },
        10_000,
    )
    .unwrap();

    controller.stop().unwrap();
    check_file_contents(&output1_path, 0..100);
    check_file_contents(&output2_path, 0..200);
}

/// A paused connector cannot push the pipeline into backpressure.
///
/// The circuit thread parks when an output endpoint has more than
/// `max_queued_records` records queued (see `output_buffers_full`). Because a
/// paused endpoint is handed entries with no data, its queue stops growing, and
/// the pipeline keeps processing input even though the endpoint is wedged in a
/// write that never returns.
///
/// The bound is on growth, not on what is already queued: an endpoint that is
/// already over the limit when it is paused keeps the pipeline parked until its
/// queue drains, which is why the test pauses the connector while its queue is
/// still small.
pub fn output_connector_pause_lifts_backpressure(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    File::create_new(&input_path).unwrap();

    // `max_queued_records` is small, so a connector that keeps receiving
    // records reaches the backpressure threshold within a few steps.
    const MAX_QUEUED: usize = 1_000;
    const AFTER_PAUSE: usize = 20 * MAX_QUEUED;

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_path.display().to_string(), "follow": true },
                },
                "format": { "name": "csv" },
            },
        },
        "outputs": {},
    }))
    .unwrap();

    append_input(&input_path, 0..100);

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    let blocked = Arc::new(AtomicBool::new(false));
    let hold = Arc::new(AtomicBool::new(true));
    let endpoint_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_output1",
        "max_queued_records": MAX_QUEUED,
        "transport": { "name": "null_output" },
        "format": { "name": "csv", "config": {} },
    }))
    .unwrap();
    controller
        .add_output_endpoint(
            "test_output1",
            &endpoint_config,
            Box::new(BlockedOutputEndpoint {
                blocked: blocked.clone(),
                hold: hold.clone(),
            }),
            None,
        )
        .unwrap();
    controller.start();

    // Wait until the connector is stuck in a write it will never finish.
    wait(|| blocked.load(Ordering::Acquire), 10_000).unwrap();

    // Pause it while its queue is still well under the threshold, then feed the
    // pipeline far more records than the threshold allows to be queued.
    controller.pause_output_endpoint("test_output1").unwrap();
    let queued_when_paused = output_endpoint_metrics(&controller, "test_output1").queued_records;
    assert!(queued_when_paused < MAX_QUEUED as u64);

    append_input(&input_path, 100..100 + AFTER_PAUSE);
    wait(
        || {
            controller
                .status()
                .global_metrics
                .num_total_processed_records()
                >= (100 + AFTER_PAUSE) as u64
        },
        30_000,
    )
    .unwrap();

    // The endpoint is still stuck, and its queue never grew: the records the
    // pipeline processed while it was paused were never queued for it.
    assert!(hold.load(Ordering::Acquire));
    let paused = output_endpoint_metrics(&controller, "test_output1");
    assert_eq!(paused.queued_records, queued_when_paused);
    assert_eq!(paused.transmitted_records, 0);

    hold.store(false, Ordering::Release);
    controller.stop().unwrap();
}

/// An output endpoint that accepts and discards everything, standing in for one
/// created through the API rather than the pipeline configuration.
struct DiscardOutputEndpoint;

impl OutputEndpoint for DiscardOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, anyhow::Error, Option<&'static str>) + Send + Sync>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> Result<(), anyhow::Error> {
        unreachable!("the test uses a format that pushes buffers, not keys")
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

/// A connector that starts paused with `send_snapshot` keeps owing its
/// snapshot rather than dropping it, and receives it when it is started --
/// reflecting the state of the view at that moment, not at startup.
pub fn output_connector_paused_defers_the_initial_snapshot(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let mut config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        true,
    );
    config
        .outputs
        .get_mut("test_output1")
        .unwrap()
        .connector_config
        .send_snapshot = true;

    // The paused connector receives nothing, including its snapshot.
    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    wait_for_records(&controller, &[0, 100]);

    // More input, still nothing: the snapshot is deferred, not dropped.
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[0, 200]);

    // Starting the connector delivers the snapshot, which covers everything the
    // view holds by then -- both the records that arrived before the connector
    // was created and those that arrived while it was paused.
    controller.start_output_endpoint("test_output1").unwrap();
    wait_for_records(&controller, &[200, 200]);
    controller.stop().unwrap();

    check_file_contents(&output1_path, 0..200);
    check_file_contents(&output2_path, 0..200);
}

/// Replay tolerates a recorded pause for an output endpoint that no longer
/// exists.
///
/// Output endpoints are not journaled as a set: replay rebuilds them from the
/// pipeline configuration. An endpoint added through the API -- an `/egress`
/// stream, which the manager also reaches as connector `api-<uuid>` of its view
/// -- is not in that configuration, so its recorded paused state names an
/// endpoint that is gone. Failing over it would leave the pipeline unable to
/// start, and every retry would fail the same way.
pub fn output_connector_pause_replays_without_the_endpoint(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        false,
    );

    // Add an endpoint the pipeline configuration does not have.
    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    let api_endpoint_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_output1",
        "transport": { "name": "null_output" },
        "format": { "name": "csv", "config": {} },
    }))
    .unwrap();
    controller
        .add_output_endpoint(
            "test_output1.api-test",
            &api_endpoint_config,
            Box::new(DiscardOutputEndpoint),
            None,
        )
        .unwrap();

    // Let a step record it as running: an endpoint the journal has never seen
    // is written down with whatever state it has, and only a later change is
    // journaled as one.
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[200, 200]);

    // Now pause it, and feed the pipeline again so that the step carrying the
    // change reaches the journal.
    controller
        .pause_output_endpoint("test_output1.api-test")
        .unwrap();
    append_input(&input_path, 200..300);
    wait_for_records(&controller, &[300, 300]);
    controller.stop().unwrap();

    // The endpoint is gone on restart, so replaying its pause has nothing to
    // act on. The pipeline must come up all the same.
    let controller = start_output_pause_controller(engine, &config);
    append_input(&input_path, 300..400);
    wait_for_records(&controller, &[400, 400]);
    controller.stop().unwrap();
}

/// A pause survives a restart: the checkpoint records the state the user left
/// the connector in, and the journal carries a change that happened after the
/// last checkpoint.
pub fn output_connector_pause_survives_restart(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let output1_path = tempdir_path.join("output1.csv");
    let output2_path = tempdir_path.join("output2.csv");
    File::create_new(&input_path).unwrap();

    let config = output_pause_config(
        &input_path,
        &output1_path,
        &output2_path,
        &storage_dir,
        false,
    );

    // Round 0: pause the connector, then checkpoint, so that the checkpoint
    // carries the paused state.
    append_input(&input_path, 0..100);
    let controller = start_output_pause_controller(engine, &config);
    wait_for_records(&controller, &[100, 100]);
    controller.pause_output_endpoint("test_output1").unwrap();
    append_input(&input_path, 100..200);
    wait_for_records(&controller, &[100, 200]);
    controller.checkpoint().unwrap();
    controller.stop().unwrap();

    // Round 1: the connector comes back paused, and stays that way until it is
    // started again.  This round does not checkpoint, so only the journal
    // records the start.
    let controller = start_output_pause_controller(engine, &config);
    assert!(
        controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );
    append_input(&input_path, 200..300);
    wait_for_records(&controller, &[100, 300]);

    controller.start_output_endpoint("test_output1").unwrap();
    append_input(&input_path, 300..400);
    wait_for_records(&controller, &[200, 400]);
    controller.stop().unwrap();

    // Round 2: replaying the journal reproduces the start, over the paused
    // state that the round 0 checkpoint restores.
    let controller = start_output_pause_controller(engine, &config);
    assert!(
        !controller
            .is_output_endpoint_paused("test_output1")
            .unwrap()
    );
    controller.stop().unwrap();
}
