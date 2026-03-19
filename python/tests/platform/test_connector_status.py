from typing import Any

from feldera import Pipeline
from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from tests import TEST_CLIENT, enterprise_only

from .helper import gen_pipeline_name, wait_for_condition


def _http_connector_stats(pipeline: Pipeline) -> tuple[list[Any], list[Any]]:
    stats = pipeline.stats()
    inputs = [
        status
        for status in stats.inputs
        if status.endpoint_name is not None and ".api-ingress-" in status.endpoint_name
    ]
    outputs = [
        status
        for status in stats.outputs
        if status.endpoint_name is not None and ".api-" in status.endpoint_name
    ]
    return inputs, outputs


def _http_connector_names(pipeline: Pipeline) -> tuple[list[str], list[str]]:
    input_names, output_names = _http_connector_stats(pipeline)
    return (
        [status.endpoint_name.rsplit(".", 1)[1] for status in input_names],
        [status.endpoint_name.rsplit(".", 1)[1] for status in output_names],
    )


def _single_input_status(pipeline: Pipeline, table_name: str) -> Any | None:
    input_names, _ = _http_connector_names(pipeline)
    if len(input_names) != 1:
        return None
    return pipeline.input_connector_stats(table_name, input_names[0])


def _single_output_status(pipeline: Pipeline, view_name: str) -> Any | None:
    _, output_names = _http_connector_names(pipeline)
    if len(output_names) != 1:
        return None
    return pipeline.output_connector_stats(view_name, output_names[0])


def _start_with_bootstrap_approval(pipeline: Pipeline) -> None:
    pipeline.start(bootstrap_policy=BootstrapPolicy.AWAIT_APPROVAL)
    if pipeline.status() == PipelineStatus.AWAITINGAPPROVAL:
        pipeline.approve()
        pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)


@enterprise_only
@gen_pipeline_name
def test_http_connector_status_across_restart_and_sql_changes(pipeline_name):
    """
    HTTP status lifecycle test. HTTP connectors are ephemeral and require special care
    to ensure they survive pipeline restarts.

    1) Create a one-table/one-view pipeline with no SQL-defined connectors.
    2) Subscribe to the view (creates one HTTP output connector), ingest 5 valid JSON
       records to the table (creates one HTTP input connector), then submit invalid
       JSON data and verify the input reports 5 records + 1 parse error and exactly
       one HTTP output connector.
    3) Stop with checkpoint and restart: verify the input connector state persists
       (5 records + 1 error) while HTTP output connectors do not survive restart.
    4) Ingest 5 more records and verify one input connector now reports 10 records.
    5) Stop, rename the view, restart with bootstrapping approval, and verify the
       same single input connector still reports 10 records + 1 error.
    6) Ingest 5 more records and verify still one input connector with 15 records.
    7) Stop, modify the table schema, restart with approval, and verify there are
       no remaining HTTP connectors.
    """

    sql = """
    CREATE TABLE input_t(c1 INT);
    CREATE MATERIALIZED VIEW output_v AS SELECT * FROM input_t;
    """.strip()

    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
    ).create_or_replace()
    pipeline.start()

    # Create output HTTP connector by subscribing to a view.
    listener = pipeline.listen("output_v")

    # Create input HTTP connector by writing data over HTTP ingress.
    valid_records_1 = [{"c1": i} for i in range(5)]
    pipeline.input_json("input_t", valid_records_1)

    # Feed one schema-invalid JSON record to trigger one parse error.
    try:
        TEST_CLIENT.push_to_pipeline(
            pipeline.name,
            "input_t",
            "json",
            [{"c1": "not_an_int"}],
            array=True,
            update_format="raw",
        )
    except FelderaAPIError:
        # HTTP ingress returns 400 for parse failures; still validate connector stats below.
        pass

    wait_for_condition(
        "one http input with 5 records, one parse error, and one output connector",
        lambda: (
            _single_input_status(pipeline, "input_t") is not None
            and _single_input_status(pipeline, "input_t").metrics.total_records == 5
            and _single_input_status(pipeline, "input_t").metrics.num_parse_errors == 1
            and _single_output_status(pipeline, "output_v") is not None
        ),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    # Keep an explicit reference until test end to ensure the listener isn't GC'd early.
    assert listener is not None

    pipeline.stop(force=False)
    pipeline.start()

    wait_for_condition(
        "input connector survives restart and output connector does not",
        lambda: (
            _single_input_status(pipeline, "input_t") is not None
            and _single_input_status(pipeline, "input_t").metrics.total_records == 5
            and _single_input_status(pipeline, "input_t").metrics.num_parse_errors == 1
            and len(_http_connector_names(pipeline)[1]) == 0
        ),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    valid_records_2 = [{"c1": i} for i in range(5, 10)]
    pipeline.input_json("input_t", valid_records_2)

    wait_for_condition(
        "input connector has 10 records and one parse error",
        lambda: (
            _single_input_status(pipeline, "input_t") is not None
            and _single_input_status(pipeline, "input_t").metrics.total_records == 10
            and _single_input_status(pipeline, "input_t").metrics.num_parse_errors == 1
        ),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    pipeline.stop(force=False)
    pipeline.modify(
        sql="""
        CREATE TABLE input_t(c1 INT);
        CREATE MATERIALIZED VIEW output_v_renamed AS SELECT * FROM input_t;
        """.strip()
    )
    _start_with_bootstrap_approval(pipeline)

    wait_for_condition(
        "after view rename connector stats keep one input connector with 10 records",
        lambda: (
            _single_input_status(pipeline, "input_t") is not None
            and _single_input_status(pipeline, "input_t").metrics.total_records == 10
            and _single_input_status(pipeline, "input_t").metrics.num_parse_errors == 1
        ),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    valid_records_3 = [{"c1": i} for i in range(10, 15)]
    pipeline.input_json("input_t", valid_records_3)
    wait_for_condition(
        "still one input connector with 15 records",
        lambda: (
            _single_input_status(pipeline, "input_t") is not None
            and _single_input_status(pipeline, "input_t").metrics.total_records == 15
            and _single_input_status(pipeline, "input_t").metrics.num_parse_errors == 1
        ),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    pipeline.stop(force=False)
    pipeline.modify(
        sql="""
        CREATE TABLE input_t(c1 INT, c2 INT);
        CREATE MATERIALIZED VIEW output_v_renamed AS SELECT c1 FROM input_t;
        """.strip()
    )
    _start_with_bootstrap_approval(pipeline)

    wait_for_condition(
        "after table modification no http connectors remain",
        lambda: len(_http_connector_names(pipeline)[0]) == 0
        and len(_http_connector_names(pipeline)[1]) == 0,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )
