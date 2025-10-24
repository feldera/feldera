from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT, enterprise_only
from .helper import (
    gen_pipeline_name,
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
        pass

    print("Starting pipeline with bootstrap_policy='allow'")
    pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    result = list(pipeline.query("SELECT * FROM v2;"))
    assert result == [{"c": 6}]

    pipeline.stop(force=True)

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
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
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
