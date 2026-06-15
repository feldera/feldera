import json
import os
import uuid

from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT, enterprise_only
from .helper import (
    gen_pipeline_name,
    wait_for_condition,
)
from feldera.testutils import (
    FELDERA_TEST_NUM_WORKERS,
    FELDERA_TEST_NUM_HOSTS,
    single_host_only,
)


@enterprise_only
@gen_pipeline_name
def test_bootstrap_enterprise(pipeline_name):
    """
    Enterprise: test backfill avoidance and bootstrapping.
    """

    print("Creating baseline pipeline")

    sql = """CREATE TABLE t1(x int) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.checkpoint(True)

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 3}]

    pipeline.stop(force=True)

    print("Adding new view v2")
    sql += """CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) AS c FROM t1;
    """
    pipeline.modify(sql=sql)

    try:
        pipeline.start(bootstrap_policy=BootstrapPolicy.REJECT)
        # If we reach here, the pipeline started successfully when it should have failed
        assert False, (
            "Expected pipeline.start() to raise an exception with bootstrap_policy='reject', but it succeeded"
        )
    except Exception as e:
        print(f"Expected exception caught: {e}")
        # Reject triggers async stopping.
        # This only guarantees deployment_status is Stopped
        pipeline.wait_for_status(PipelineStatus.STOPPED, timeout=30)
        pass

    print(
        "Starting pipeline with bootstrap_policy='allow' and dismissing deployment error from previous reject start"
    )
    pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW, dismiss_error=True)
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")
    pipeline.wait_for_idle()

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    result = list(pipeline.query("SELECT * FROM v2;"))
    assert result == [{"c": 6}]

    pipeline.stop(force=True)
    pipeline.wait_for_status(PipelineStatus.STOPPED, timeout=30)

    # Since we didn't make a checkpoint during the previous run, the pipeline should be in the AWAITINGAPPROVAL state.
    print("Starting pipeline with bootstrap_policy='await_approval'")
    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL

    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff["program_diff"] == {
        "added_tables": [],
        "added_views": ["v2"],
        "modified_tables": [],
        "modified_views": [],
        "removed_tables": [],
        "removed_views": [],
    }

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")
    pipeline.wait_for_idle()

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    result = list(pipeline.query("SELECT * FROM v2;"))
    assert result == [{"c": 6}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    print("Adding new table t2 and view v3")

    original_sql = sql

    sql_with_new_table = (
        original_sql
        + """CREATE TABLE t2(y int) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v3 AS SELECT MAX(y) AS m FROM t2;
    """
    )
    pipeline.modify(sql=sql_with_new_table)

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL
    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff["program_diff"] == {
        "added_tables": ["t2"],
        "added_views": ["v3"],
        "modified_tables": [],
        "modified_views": [],
        "removed_tables": [],
        "removed_views": [],
    }

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    pipeline.execute("INSERT INTO t2 VALUES (10), (20), (30);")
    pipeline.wait_for_idle()

    result = list(pipeline.query("SELECT * FROM v3;"))
    assert result == [{"m": 30}]

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    print("Modify table t2 and view v3")

    original_sql = sql

    sql_with_new_table = (
        original_sql
        + """CREATE TABLE t2(y int, s string) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v3 AS SELECT MAX(y) AS m FROM t2;
    """
    )
    pipeline.modify(sql=sql_with_new_table)

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL
    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff["program_diff"] == {
        "added_tables": [],
        "added_views": [],
        "modified_tables": ["t2"],
        "modified_views": ["v3"],
        "removed_tables": [],
        "removed_views": [],
    }

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    result = list(pipeline.query("SELECT * FROM v3;"))
    assert result == [{"m": None}]

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    pipeline.execute("INSERT INTO t2 (y) VALUES (40), (50), (60);")
    pipeline.wait_for_idle()

    result = list(pipeline.query("SELECT * FROM v3;"))
    assert result == [{"m": 60}]

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    print("Delete view v3")

    original_sql = sql

    sql_with_new_table = (
        original_sql
        + """CREATE TABLE t2(y int, s string) WITH ('materialized'='true');
    """
    )
    pipeline.modify(sql=sql_with_new_table)

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL
    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff["program_diff"] == {
        "added_tables": [],
        "added_views": [],
        "modified_tables": [],
        "modified_views": [],
        "removed_tables": [],
        "removed_views": ["v3"],
    }

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    pipeline.execute("INSERT INTO t2 (y) VALUES (70), (80), (90);")
    pipeline.wait_for_idle()

    # The table hasn't changed, so the previous 3 rows should still be there.
    result = list(pipeline.query("SELECT count(*) as c FROM t2;"))
    assert result == [{"c": 6}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    print("Delete table t2")

    original_sql = sql

    sql_with_new_table = original_sql
    pipeline.modify(sql=sql_with_new_table)

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL
    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff["program_diff"] == {
        "added_tables": [],
        "added_views": [],
        "modified_tables": [],
        "modified_views": [],
        "removed_views": [],
        "removed_tables": ["t2"],
    }

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)

    result = list(pipeline.query("SELECT count(*) as c FROM t1;"))
    assert result == [{"c": 6}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)


@enterprise_only
@gen_pipeline_name
def test_silent_bootstrap_enterprise(pipeline_name):
    """
    Enterprise: silent bootstrapping must process backfilled records without
    transmitting them to output connectors.
    """

    output_path = os.path.join(
        "/tmp", f"feldera_silent_bootstrap_{uuid.uuid4().hex}.json"
    )

    def sql_for_view(view_expr: str) -> str:
        connectors = json.dumps(
            [
                {
                    "name": "out",
                    "transport": {
                        "name": "file_output",
                        "config": {"path": output_path},
                    },
                    "format": {"name": "json"},
                }
            ]
        )
        return f"""
CREATE TABLE t1(x int) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v1
WITH ('connectors' = '{connectors}')
AS SELECT {view_expr} AS x FROM t1;
"""

    def output_metrics():
        return pipeline.output_connector_stats("v1", "out").metrics

    def processed_records() -> int:
        return output_metrics().total_processed_input_records or 0

    def transmitted_records() -> int:
        return output_metrics().transmitted_records or 0

    def wait_for_output_progress(
        min_processed_records: int, expected_transmitted_records: int
    ):
        wait_for_condition(
            f"output connector reaches {min_processed_records} processed records",
            lambda: processed_records() >= min_processed_records,
            timeout_s=120.0,
            poll_interval_s=1.0,
        )
        assert transmitted_records() == expected_transmitted_records

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql_for_view("x"),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    pipeline.input_json("t1", [{"x": 1}, {"x": 2}, {"x": 3}])
    # Three records ingested, three records sent.
    wait_for_output_progress(min_processed_records=3, expected_transmitted_records=3)
    expected_processed_records = 3
    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    pipeline.modify(sql=sql_for_view("x + 1"))
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        silent_bootstrap=True,
        timeout_s=300,
    )
    # Modified connector keeps its transmitted record count and doesn't send any new records
    # in silent mode.
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_processed_records,
    )

    pipeline.input_json("t1", [{"x": 5}])
    # One more record ingested, and one more record sent.
    expected_processed_records += 1
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_processed_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [{"c": 4}]
    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    pipeline.modify(sql=sql_for_view("x + 2"))
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        silent_bootstrap=True,
        timeout_s=300,
    )
    # Modified connector keeps its transmitted record count and doesn't send any new records
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_processed_records,
    )
    pipeline.input_json("t1", [{"x": 6}])
    expected_processed_records += 1
    # One more record ingested, and one more record sent.
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_processed_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [{"c": 5}]
    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    # Final round with silent bootstrap disabled should produce all accumulated records.
    pipeline.modify(sql=sql_for_view("x + 3"))
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        silent_bootstrap=False,
        timeout_s=300,
    )

    # Non-silent bootstrap should send all accumulated records again.
    expected_transmitted_records = 2 * expected_processed_records
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )

    pipeline.input_json("t1", [{"x": 7}])
    expected_processed_records += 1
    expected_transmitted_records += 1
    # One more record ingested, and one more record sent.
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [{"c": 6}]
    pipeline.checkpoint(True)
    pipeline.stop(force=True)


@enterprise_only
@gen_pipeline_name
def test_concurrent_bootstrap_enterprise(pipeline_name):
    """
    Enterprise: concurrent bootstrapping keeps the pre-existing view live while
    the modified view backfills in the background, and emits the modified view's
    full contents to its output connector at cutover.

    This is the complement of silent bootstrapping: instead of suppressing the
    backfilled output, concurrent bootstrapping transmits the full snapshot of
    the modified view (the backfilled rows plus any live rows) once the new view
    takes over.
    """

    output_path = os.path.join(
        "/tmp", f"feldera_concurrent_bootstrap_{uuid.uuid4().hex}.json"
    )

    def sql_for_view(view_expr: str) -> str:
        connectors = json.dumps(
            [
                {
                    "name": "out",
                    "transport": {
                        "name": "file_output",
                        "config": {"path": output_path},
                    },
                    "format": {"name": "json"},
                }
            ]
        )
        return f"""
CREATE TABLE t1(x int) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v1
WITH ('connectors' = '{connectors}')
AS SELECT {view_expr} AS x FROM t1;
"""

    def output_metrics():
        return pipeline.output_connector_stats("v1", "out").metrics

    def processed_records() -> int:
        return output_metrics().total_processed_input_records or 0

    def transmitted_records() -> int:
        return output_metrics().transmitted_records or 0

    def wait_for_output_progress(
        min_processed_records: int, expected_transmitted_records: int
    ):
        wait_for_condition(
            f"output connector reaches {min_processed_records} processed records",
            lambda: processed_records() >= min_processed_records,
            timeout_s=120.0,
            poll_interval_s=1.0,
        )
        assert transmitted_records() == expected_transmitted_records

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql_for_view("x"),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    pipeline.input_json("t1", [{"x": 1}, {"x": 2}, {"x": 3}])
    # Three records ingested, three records sent.
    wait_for_output_progress(min_processed_records=3, expected_transmitted_records=3)
    expected_processed_records = 3
    # The view holds the full backfilled contents (three rows so far).
    backfilled_records = 3
    expected_transmitted_records = 3
    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    # Concurrent bootstrap: the modified view backfills in the background while
    # the pre-existing view stays live, then emits its full contents at cutover.
    pipeline.modify(sql=sql_for_view("x + 1"))
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        concurrent_bootstrap=True,
        timeout_s=300,
    )
    # At cutover the modified view re-emits its full contents (all backfilled
    # rows), unlike silent bootstrap which suppresses them.
    expected_transmitted_records += backfilled_records
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [
        {"c": backfilled_records}
    ]

    pipeline.input_json("t1", [{"x": 5}])
    # One more record ingested, and one more record sent.
    expected_processed_records += 1
    expected_transmitted_records += 1
    backfilled_records += 1
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [
        {"c": backfilled_records}
    ]
    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    # A second concurrent bootstrap round re-emits the full (now larger) view
    # contents again at cutover.
    pipeline.modify(sql=sql_for_view("x + 2"))
    pipeline.start(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        concurrent_bootstrap=True,
        timeout_s=300,
    )
    expected_transmitted_records += backfilled_records
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [
        {"c": backfilled_records}
    ]

    pipeline.input_json("t1", [{"x": 6}])
    expected_processed_records += 1
    expected_transmitted_records += 1
    backfilled_records += 1
    wait_for_output_progress(
        min_processed_records=expected_processed_records,
        expected_transmitted_records=expected_transmitted_records,
    )
    assert list(pipeline.query("SELECT COUNT(*) AS c FROM v1;")) == [
        {"c": backfilled_records}
    ]
    pipeline.checkpoint(True)
    pipeline.stop(force=True)


@enterprise_only
@gen_pipeline_name
def test_bootstrap_non_materialized_table_enterprise(pipeline_name):
    """
    Enterprise: bootstrapping non-materialized table that hasn't changed since the last
    checkpoint, but there is a view that depends on it that has changed, should fail.
    """

    sql = """CREATE TABLE t1(x int);
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    sql += """CREATE MATERIALIZED VIEW v2 AS SELECT MAX(x) AS m FROM t1;
    """
    pipeline.modify(sql=sql)

    try:
        pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)
        assert False, (
            "Expected pipeline.start() to raise an exception with bootstrap_policy='reject', but it succeeded"
        )
    except Exception as e:
        print(f"Expected exception caught: {e}")
        assert "not materialized" in str(e)
        pass


@enterprise_only
@gen_pipeline_name
def test_bootstrap_table_lateness_enterprise(pipeline_name):
    """
    Enterprise: bootstrapping a table with lateness must fail.
    """

    sql = """CREATE TABLE t1(x int lateness 0) with ('materialized'='true');
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    sql += """CREATE MATERIALIZED VIEW v2 AS SELECT MAX(x) AS m FROM t1;
    """
    pipeline.modify(sql=sql)

    try:
        pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)
        assert False, (
            "Expected pipeline.start() to raise an exception for a program with lateness, but it succeeded"
        )
    except Exception as e:
        print(f"Expected exception caught: {e}")
        assert "lateness" in str(e)
        pass


@enterprise_only
@gen_pipeline_name
def test_bootstrap_view_lateness_enterprise(pipeline_name):
    """
    Enterprise: bootstrapping a view with lateness must fail.
    """

    sql = """CREATE TABLE t1(x int) with ('materialized'='true');
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
LATENESS v1.c 0;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    sql += """CREATE MATERIALIZED VIEW v2 AS SELECT MAX(x) AS m FROM t1;
    """
    pipeline.modify(sql=sql)

    try:
        pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)
        assert False, (
            "Expected pipeline.start() to raise an exception for a program with lateness, but it succeeded"
        )
    except Exception as e:
        print(f"Expected exception caught: {e}")
        assert "lateness" in str(e)
        pass


# Add/remove connectors.
@enterprise_only
@single_host_only
@gen_pipeline_name
def test_bootstrap_connectors(pipeline_name):
    """
    Enterprise: add/remove connectors should require user approval.
    """

    # Start pipeline with no connectors, feed 3 rows, and checkpoint its state.
    def gen_sql(connectors):
        return f"""CREATE TABLE t1(x int)
with (
    'materialized' = 'true',
    'connectors' = '[{connectors}]'
);
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=gen_sql(""),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.wait_for_idle()
    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 3}]

    pipeline.checkpoint(True)

    pipeline.stop(force=True)

    # Add two input connector. This should require user approval.
    # The new connectors should start running from scratch and feed 10 rows each to the table.
    pipeline.modify(
        sql=gen_sql("""{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
          "limit": 10
        }]
      }
    }
},{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
          "limit": 10
        }]
      }
    }
}""")
    )

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL

    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff == {
        "added_input_connectors": ["t1.unnamed-0", "t1.unnamed-1"],
        "added_output_connectors": [],
        "modified_input_connectors": [],
        "modified_output_connectors": [],
        "program_diff": {
            "added_tables": [],
            "added_views": [],
            "modified_tables": [],
            "modified_views": [],
            "removed_tables": [],
            "removed_views": [],
        },
        "program_diff_error": None,
        "removed_input_connectors": [],
        "removed_output_connectors": [],
    }

    pipeline.approve()

    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)

    pipeline.wait_for_completion(timeout_s=300)
    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 23}]

    pipeline.checkpoint(True)
    pipeline.stop(force=True)

    # Delete one connector.
    pipeline.modify(
        sql=gen_sql("""
    {
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
          "limit": 10
        }]
      }
    }
}""")
    )

    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert pipeline.status() == PipelineStatus.AWAITINGAPPROVAL

    diff = pipeline.deployment_runtime_status_details()
    print(f"Pipeline diff: {diff}")
    assert diff == {
        "added_input_connectors": [],
        "added_output_connectors": [],
        "modified_input_connectors": [],
        "modified_output_connectors": [],
        "program_diff": {
            "added_tables": [],
            "added_views": [],
            "modified_tables": [],
            "modified_views": [],
            "removed_tables": [],
            "removed_views": [],
        },
        "program_diff_error": None,
        "removed_input_connectors": ["t1.unnamed-1"],
        "removed_output_connectors": [],
    }

    pipeline.approve()

    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    pipeline.wait_for_completion(timeout_s=300)
    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 23}]

    pipeline.checkpoint(True)
    pipeline.stop(force=True)
