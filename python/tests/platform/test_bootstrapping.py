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

    sql = """CREATE TABLE t1(x int) WITH ('materialized'='true');
CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            fault_tolerance_model=None,  # We will make manual checkpoints in this test.
            dev_tweaks={
                "backfill_avoidance": True
            },  # This should not be necessary once it is enabled by default.
        ),
    ).create_or_replace()

    pipeline.start()

    pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")
    pipeline.checkpoint(True)

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 3}]

    pipeline.stop(force=True)

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
        # This is expected - the pipeline should fail to start with bootstrap_policy='reject'
        print(f"Expected exception caught: {e}")
        pass

    print("Starting pipeline with bootstrap_policy='allow'")
    status = pipeline.start(bootstrap_policy=BootstrapPolicy.ALLOW)
    assert status == PipelineStatus.RUNNING

    pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    result = list(pipeline.query("SELECT * FROM v2;"))
    assert result == [{"c": 6}]

    status = pipeline.stop(force=True)

    print("Starting pipeline with bootstrap_policy='await_approval'")

    status = pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    assert status == PipelineStatus.AWAITINGAPPROVAL

    print(f"Pipeline diff: {pipeline.deployment_runtime_status_details()}")

    pipeline.approve()

    # Wait for the pipeline to reach RUNNING status (up to 300 seconds)
    pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)
    pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")

    result = list(pipeline.query("SELECT * FROM v1;"))
    assert result == [{"c": 6}]

    result = list(pipeline.query("SELECT * FROM v2;"))
    assert result == [{"c": 6}]

    pipeline.stop(force=True)
