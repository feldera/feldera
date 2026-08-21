"""Crucible runtime tests, driven through the pipeline manager.

A pipeline whose program config sets ``runtime_version: "crucible"`` compiles to
a circuit IR rather than a Rust binary, and the runner launches the crucible
engine to execute it.

The API never returns the circuit IR: it is stripped from ``program_info``
alongside the generated Rust and the dataflow graph, and reaches the pipeline
through the program info artifact instead. So these tests assert the effects of
the IR rather than its contents. That a crucible pipeline ingests a row and
reads it back is the strongest evidence the IR was captured and delivered,
since the pipeline cannot run at all otherwise.

Disabled by default. They need a manager started with the ``runtime_version``
unstable feature and a crucible engine the runner can launch, so nothing here
runs unless ``FELDERA_TEST_CRUCIBLE`` is set:

    FELDERA_TEST_CRUCIBLE=1 uv run pytest tests/crucible -vv

No CI job collects this directory.
"""

import pytest

from feldera import PipelineBuilder
from feldera.enums import ProgramStatus
from feldera.pipeline import Pipeline
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT, env_truthy
from tests.platform.helper import gen_pipeline_name, wait_for_program_success
from tests.utils import wait_for_records

pytestmark = pytest.mark.skipif(
    not env_truthy("FELDERA_TEST_CRUCIBLE"),
    reason="crucible tests need a crucible engine and the runtime_version unstable feature; set FELDERA_TEST_CRUCIBLE=1",
)

SQL = """
CREATE TABLE t(id BIGINT, s VARCHAR);
CREATE MATERIALIZED VIEW v AS SELECT * FROM t;
""".strip()

# Crucible does not support multihost pipelines yet, so pin one host. One worker
# keeps the engine's runtime shape the same as the compiler-output fixture.
CRUCIBLE_RUNTIME = RuntimeConfig(workers=1, hosts=1)


def build(pipeline_name: str, sql: str = SQL) -> Pipeline:
    """Creates a stopped, compiled pipeline on the crucible runtime."""
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_version="crucible",
        runtime_config=CRUCIBLE_RUNTIME,
    ).create_or_replace()

    # PipelineBuilder lets FELDERA_RUNTIME_VERSION override its argument, so pin
    # what the manager actually stored rather than what we asked for.
    selected = pipeline.program_config().get("runtime_version")
    assert selected == "crucible", (
        f"expected runtime_version 'crucible', got {selected!r}"
    )
    return pipeline


def recompile(pipeline: Pipeline, **modify_kwargs) -> None:
    """Applies a program change and waits for the resulting compilation."""
    before = pipeline.program_version()
    pipeline.modify(**modify_kwargs)
    wait_for_program_success(pipeline.name, before + 1)


@gen_pipeline_name
def test_crucible_completes_without_rust_compilation(pipeline_name):
    """Crucible reaches Success at the SQL stage, never entering Rust compilation.

    The SQL compiler delivers the program info artifact and marks the program
    Success itself, so a crucible program carries a SQL compilation log and no
    Rust one.
    """
    pipeline = build(pipeline_name)

    assert pipeline.program_status() == ProgramStatus.Success

    error = pipeline.program_error() or {}
    assert error.get("sql_compilation") is not None, (
        "a crucible program carries its SQL log"
    )
    assert error.get("rust_compilation") is None, (
        f"crucible must not run Rust compilation, got {error.get('rust_compilation')}"
    )

    pipeline.delete()


@gen_pipeline_name
def test_crucible_pipeline_runs_end_to_end(pipeline_name):
    """A crucible pipeline runs: ingest a row and read it back from the view."""
    pipeline = build(pipeline_name)

    pipeline.start_paused()
    out = pipeline.listen("v")
    pipeline.resume()

    pipeline.input_json("t", {"id": 1, "s": "hello"})

    wait_for_records(out, 1)
    rows = out.to_dict()
    assert rows == [{"id": 1, "s": "hello", "insert_delete": 1}], (
        f"ingested row did not round-trip through the view: {rows}"
    )

    pipeline.stop(force=True)
    # A pipeline that ran holds storage, which must be cleared before deletion.
    pipeline.delete(True)


@gen_pipeline_name
def test_recompiles_after_schema_edit(pipeline_name):
    """Adding a column and recompiling reaches Success with the new schema."""
    pipeline = build(pipeline_name)
    assert len(pipeline.program_info()["schema"]["inputs"][0]["fields"]) == 2

    recompile(
        pipeline,
        sql="CREATE TABLE t(id BIGINT, s VARCHAR, n INT);\n"
        "CREATE MATERIALIZED VIEW v AS SELECT * FROM t;",
    )

    assert pipeline.program_status() == ProgramStatus.Success
    fields = pipeline.program_info()["schema"]["inputs"][0]["fields"]
    assert len(fields) == 3, (
        f"recompiled schema must reflect the added column: {fields}"
    )

    pipeline.delete()
