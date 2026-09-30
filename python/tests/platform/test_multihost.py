import threading
import time

from feldera.enums import PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT
from .helper import API_PREFIX, gen_pipeline_name, http_request, wait_for_records
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


def _ids(handler):
    return sorted(record["id"] for record in handler.to_dict())


class _LogWatcher:
    """
    Collects a pipeline's log lines in the background, so that a test can
    wait for a line with a deadline.  (Reading the log directly blocks until
    the next line arrives, however long that takes.)
    """

    def __init__(self, pipeline):
        self._lines = []
        self._lock = threading.Lock()
        self._stream = pipeline.resume_logs()
        threading.Thread(target=self._collect, daemon=True).start()

    def _collect(self):
        try:
            for line in self._stream:
                with self._lock:
                    self._lines.append(line)
        except Exception:
            # `close` ends the stream by breaking the connection.
            pass

    def count(self, text):
        """The number of lines so far that contain `text`."""
        with self._lock:
            return sum(text in line for line in self._lines)

    def wait_for(self, text, n, step=None, timeout_s=120):
        """
        Waits until at least `n` lines contain `text`.  Calls `step`, if
        given, about once a second while waiting.
        """
        deadline = time.monotonic() + timeout_s
        while self.count(text) < n:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"timed out waiting for {n} log lines with {text!r} "
                    f"(found {self.count(text)})"
                )
            if step is not None:
                step()
            time.sleep(1)

    def close(self):
        self._stream.close()


def _started(stream):
    """The debug log line that a host writes when it starts gathering `stream`."""
    return f"started gathering output stream '{stream}'"


def _stopped(stream):
    """The debug log line that a host writes when it stops gathering `stream`."""
    return f"stopped gathering output stream '{stream}'"


@gen_pipeline_name
def test_input_connectors(pipeline_name):
    """
    An input connector resides on a single host, but multiple input
    connectors for a single table are spread across hosts.  If the
    pipeline doesn't properly reshard data from multiple input connectors
    for a single table, this can cause unsoundness.

    This test checks for soundness with two input connectors that
    generate the same data for a single table.  If each one produces
    N records then a view should only be able to count N records, not
    2N.

    (This test passes with single-host also, of course.)
    """
    sql = """
CREATE TABLE Input1 (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
) with (
  'connectors' = '[{
    "name": "input1",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 2500
          }]
      }
    }
  },{
    "name": "input2",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 2500
          }]
      }
    }
  }]'
);

CREATE MATERIALIZED VIEW Count AS SELECT COUNT(*) from input1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.wait_for_completion(timeout_s=300)
    result = list(pipeline.query("SELECT * FROM Count;"))
    assert result == [{"EXPR$0": 2500}]


@gen_pipeline_name
def test_preprocessor_on_every_host(pipeline_name):
    """
    Test that preprocessors work with a multi-host setup.

    If the pipeline reaches the RUNNING state the preprocessor
    has been registered.
    """
    connector = """{
    "name": "%s",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 1
          }]
      }
    },
    "preprocessor": [{
      "name": "example",
      "message_oriented": true,
      "config": {}
    }]
  }"""
    sql = f"""
CREATE TABLE Input (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
) with (
  'connectors' = '[{connector % "input1"},{connector % "input2"}]'
);
"""
    udf_rust = """
use feldera_adapterlib::format::{ParseError, Splitter};
use feldera_adapterlib::preprocess::{
    Preprocessor, PreprocessorCreateError, PreprocessorFactory,
};
use feldera_types::preprocess::PreprocessorConfig;

pub struct ExamplePreprocessor;

impl Preprocessor for ExamplePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        (data.to_vec(), vec![])
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(ExamplePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        None
    }
}

pub struct ExamplePreprocessorFactory;

impl PreprocessorFactory for ExamplePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(ExamplePreprocessor))
    }
}
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        udf_rust=udf_rust,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING
    pipeline.stop(force=True)


@gen_pipeline_name
def test_postprocessor_on_every_host(pipeline_name):
    """
    Test that postprocessors work with a multi-host setup.

    The coordinator allocates connectors in round-robin over hosts.
    We use two views over and hosts, checking that postprocessors
    are registered correctly.
    """
    connector = """{
    "name": "%s",
    "transport": {
      "name": "file_output",
      "config": {
          "path": "/dev/null"
      }
    },
    "format": { "name": "json" },
    "postprocessor": [{
      "name": "example",
      "config": {}
    }]
  }"""
    sql = f"""
CREATE TABLE Input (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
);
CREATE VIEW v1 WITH (
  'connectors' = '[{connector % "out1"}]'
) AS SELECT * FROM Input;
CREATE VIEW v2 WITH (
  'connectors' = '[{connector % "out2"}]'
) AS SELECT * FROM Input;
"""
    udf_rust = """
use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_types::postprocess::PostprocessorConfig;

pub struct ExamplePostprocessor;

impl Postprocessor for ExamplePostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(ExamplePostprocessor)
    }
}

pub struct ExamplePostprocessorFactory;

impl PostprocessorFactory for ExamplePostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(ExamplePostprocessor))
    }
}
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        udf_rust=udf_rust,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING
    pipeline.stop(force=True)


@gen_pipeline_name
def test_listen_to_streams_without_connectors(pipeline_name):
    """
    A multihost pipeline gathers each output stream to one host.  It
    gathers a stream that no output connector reads only while a client
    listens to it, so a listener must still get every host's rows.  That
    holds for a view and a table, for a listener that starts before any
    input, for one that starts while the pipeline runs, and for one that
    starts after the last listener of its stream went away.

    In the last case, every host stops gathering the stream and later
    starts again.  The test observes this through the debug log lines that
    each host writes when it starts or stops a gather.  They also show that
    no host gathers a stream that nothing reads, or one that a connector
    reads (which it gathers from the start, without such a line).

    (This test passes with single-host also, of course.  A single host
    gathers nothing, so it skips the log checks.)
    """
    sql = """
CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY);
CREATE VIEW connected WITH (
  'connectors' = '[{
    "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
    "format": { "name": "json" }
  }]'
) AS SELECT * FROM t;
CREATE VIEW unread AS SELECT id * 2 AS id FROM t;
CREATE VIEW brief AS SELECT id * 3 AS id FROM t;
CREATE VIEW never AS SELECT id * 4 AS id FROM t;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
            logging="object_store=warn,buoyant_kernel=warn,info,"
            "dbsp_adapters::static_compile::catalog=debug",
        ),
    ).create_or_replace()

    # Each host writes each gather line once.
    hosts = FELDERA_TEST_NUM_HOSTS
    multihost = hosts > 1
    next_id = iter(range(1_000_000, 2_000_000))

    def input_one_row():
        """Starts a transaction, which is when a host applies gather changes."""
        pipeline.input_json("t", [{"id": next(next_id)}])

    pipeline.start()
    logs = _LogWatcher(pipeline)
    early_view = pipeline.listen("unread")
    early_table = pipeline.listen("t")
    connected = pipeline.listen("connected")

    # Enough rows that every worker on every host owns some of them.
    first = range(0, 1000)
    pipeline.input_json("t", [{"id": i} for i in first])
    for handler in [early_view, early_table, connected]:
        wait_for_records(handler, len(first))
    assert _ids(early_view) == [2 * i for i in first]
    assert _ids(early_table) == list(first)
    assert _ids(connected) == list(first)

    # A listener that starts now gets only the later rows, but all of them.
    late_view = pipeline.listen("unread")
    second = range(1000, 2000)
    pipeline.input_json("t", [{"id": i} for i in second])
    for handler in [early_view, late_view]:
        wait_for_records(handler, len(second))
    assert _ids(early_view) == [2 * i for i in second]
    assert _ids(late_view) == [2 * i for i in second]

    # A listener that comes and goes makes every host start and then stop
    # gathering `brief`.  A host applies a change only when a transaction
    # starts, so feed it input until it does.
    with http_request(
        "POST",
        f"{API_PREFIX}/pipelines/{pipeline_name}/egress/brief?format=json",
        stream=True,
    ) as response:
        assert response.status_code == 200
        if multihost:
            logs.wait_for(_started("brief"), hosts, step=input_one_row)
    if multihost:
        logs.wait_for(_stopped("brief"), hosts, step=input_one_row)

    # A new listener restarts the gather and gets every host's rows again.
    restarted = pipeline.listen("brief")
    third = range(2500, 3000)
    pipeline.input_json("t", [{"id": i} for i in third])
    wait_for_records(restarted, len(third))
    assert _ids(restarted) == [3 * i for i in third]

    if multihost:
        # The restart logged a second start on every host.  By now, every
        # host has also logged the starts for `unread` and `t`.
        logs.wait_for(_started("brief"), 2 * hosts)
        logs.wait_for(_started("unread"), hosts)
        logs.wait_for(_started("t"), hosts)

        # Nothing read `never`, and `connected` was gathered from the start.
        for stream in ["never", "connected"]:
            assert logs.count(_started(stream)) == 0, stream

    logs.close()
    pipeline.stop(force=True)
