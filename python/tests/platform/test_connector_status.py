from http import HTTPStatus
from typing import Any

from feldera import Pipeline
from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from tests import TEST_CLIENT, enterprise_only

from .helper import api_url, gen_pipeline_name, get, wait_for_condition


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


def _fetch_stats_with_errors(name: str) -> dict:
    """Call `/stats?include_connector_errors=true` and return the parsed JSON."""
    resp = get(api_url(f"/pipelines/{name}/stats?include_connector_errors=true"))
    assert resp.status_code == HTTPStatus.OK, (
        f"/stats returned {resp.status_code}: {resp.text}"
    )
    return resp.json()


def _input_parse_error_messages(stats_json: dict, table_name: str) -> list[str]:
    """Pluck parse-error messages for any input connector targeting `table_name`.

    The bundle selector returns error arrays inline on each endpoint; counts
    without the selector would instead leave the arrays absent. Sorted for
    order-independent comparison.
    """
    messages: list[str] = []
    for entry in stats_json.get("inputs", []):
        config = entry.get("config") or {}
        if config.get("stream", "").split(".")[-1].lower() != table_name.lower():
            continue
        for err in entry.get("parse_errors") or []:
            messages.append(err.get("message", ""))
    return sorted(messages)


@enterprise_only
@gen_pipeline_name
def test_parse_error_messages_survive_restart(pipeline_name):
    """
    Recent parse-error messages are persisted in the checkpoint and exposed
    through /stats?include_connector_errors=true after a stop+start cycle.

    Covers the core behaviour guarded by the checkpoint-persistence change:
    not only the parse-error *count* but the message payload must be recovered.
    """

    sql = "CREATE TABLE input_t(c1 INT); CREATE MATERIALIZED VIEW v AS SELECT * FROM input_t;"
    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
    ).create_or_replace()
    pipeline.start()

    # Seed one valid record so the input connector exists, then two distinct
    # malformed records so we produce two parse errors with different messages.
    pipeline.input_json("input_t", [{"c1": 1}])

    for bad in ([{"c1": "not_an_int"}], [{"c1": [1, 2, 3]}]):
        try:
            TEST_CLIENT.push_to_pipeline(
                pipeline.name,
                "input_t",
                "json",
                bad,
                array=True,
                update_format="raw",
            )
        except FelderaAPIError:
            # HTTP ingress surfaces 400 for parse failures; the connector
            # still records the error, which is what we assert below.
            pass

    def two_parse_errors_with_messages() -> bool:
        status = _single_input_status(pipeline, "input_t")
        if status is None or status.metrics.num_parse_errors < 2:
            return False
        stats_json = _fetch_stats_with_errors(pipeline.name)
        return len(_input_parse_error_messages(stats_json, "input_t")) >= 2

    wait_for_condition(
        "two parse errors with messages recorded before restart",
        two_parse_errors_with_messages,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    # Snapshot messages before restart so we can assert exact-match survival.
    before = _input_parse_error_messages(
        _fetch_stats_with_errors(pipeline.name), "input_t"
    )
    assert len(before) >= 2, (
        f"expected >=2 parse-error messages pre-restart, got {before}"
    )

    # Stop with checkpoint, then start again; the restored endpoint status
    # must carry the same error messages, not just the counts.
    pipeline.stop(force=False)
    pipeline.start()

    def errors_restored() -> bool:
        status = _single_input_status(pipeline, "input_t")
        if status is None or status.metrics.num_parse_errors < 2:
            return False
        after = _input_parse_error_messages(
            _fetch_stats_with_errors(pipeline.name), "input_t"
        )
        return after == before

    wait_for_condition(
        "parse-error messages restored from checkpoint after restart",
        errors_restored,
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    # Guardrail for the non-selector path: without the query param, the
    # error-message arrays must not appear in the /stats response — this keeps
    # the hot polling endpoint lightweight for the web console.
    plain = get(api_url(f"/pipelines/{pipeline.name}/stats")).json()
    for entry in plain.get("inputs", []):
        assert entry.get("parse_errors") is None, (
            f"/stats without selector leaked parse_errors: {entry}"
        )
        assert entry.get("transport_errors") is None, (
            f"/stats without selector leaked transport_errors: {entry}"
        )

    pipeline.stop(force=True)
