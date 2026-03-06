from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT

from .helper import cleanup_pipeline, gen_pipeline_name


@gen_pipeline_name
def test_pipeline_runtime_env_is_visible_in_udf(pipeline_name):
    # First verify that reserved env vars are rejected.
    cleanup_pipeline(pipeline_name)
    try:
        PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql="CREATE TABLE t(i INT);",
            runtime_config=RuntimeConfig(workers=1, env={"RUST_LOG": "debug"}),
        ).create_or_replace()
        assert False, "Expected pipeline creation to fail for reserved env variable"
    except Exception as e:
        error_text = str(e)
        assert (
            "InvalidRuntimeConfig" in error_text
            or "reserved and cannot be overridden" in error_text
        ), error_text

    # Then verify allowed env vars are injected and visible to UDFs.
    sql = """
        CREATE TABLE t(i INT NOT NULL);
        CREATE FUNCTION read_env(i INT NOT NULL) RETURNS VARCHAR;
        CREATE MATERIALIZED VIEW v AS
            SELECT read_env(i) AS env_value FROM t;
    """

    udf_rust = """
        use crate::*;

        pub fn read_env(_i: i32) -> Result<Option<SqlString>, Box<dyn std::error::Error>> {
            Ok(std::env::var("PIPELINE_E2E_ENV").ok().map(SqlString::from))
        }
    """

    pipeline = None
    cleanup_pipeline(pipeline_name)
    try:
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            udf_rust=udf_rust,
            runtime_config=RuntimeConfig(
                workers=1, env={"PIPELINE_E2E_ENV": "expected-value"}
            ),
        ).create_or_replace()

        pipeline.start()
        pipeline.input_json("t", [{"i": 1}])

        rows = list(pipeline.query("SELECT env_value FROM v ORDER BY env_value"))
        assert rows == [{"env_value": "expected-value"}], rows
    finally:
        if pipeline is not None:
            pipeline.stop(force=True)
            pipeline.clear_storage()
        cleanup_pipeline(pipeline_name)
