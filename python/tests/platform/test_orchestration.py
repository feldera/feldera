import uuid

from feldera.enums import PipelineStatus
from http import HTTPStatus

from .helper import (
    create_pipeline,
    post_no_body,
    post_json,
    api_url,
    start_pipeline,
    start_pipeline_as_paused,
    resume_pipeline,
    pause_pipeline,
    gen_pipeline_name,
    cleanup_pipeline,
    stop_pipeline,
    reset_pipeline,
    connector_action,
    output_connector_action,
    pipeline_stats,
    connector_paused,
    output_connector_stats,
    output_connector_paused,
    wait_for_condition,
    wait_for_pipeline_reachable,
    get,
)
from feldera.testutils import FELDERA_TEST_NUM_HOSTS


def _basic_orchestration_info(pipeline: str, table: str, connector: str):
    stats = pipeline_stats(pipeline)
    pipeline_paused = (
        PipelineStatus.from_str(stats["global_metrics"]["state"])
        == PipelineStatus.PAUSED
    )
    processed = stats["global_metrics"]["total_processed_records"]
    return pipeline_paused, connector_paused(pipeline, table, connector), processed


@gen_pipeline_name
def test_pipeline_orchestration_basic(pipeline_name):
    """
    Tests the orchestration of the pipeline, which means the starting and pausing of the
    pipeline itself as well as its connectors individually. This tests the basic processing
    of data and handling of case sensitivity and special characters.
    """
    scenarios = [
        # Case-insensitive table name
        ("numbers", "c1"),
        # Case-insensitive table name (with some non-alphanumeric characters that do not need to be encoded)
        ("numbersC0_", "aA0_-"),
        # Case-sensitive table name
        ('"Numbers"', "c1"),
        # Case-sensitive table name with special characters that need to be encoded
        ('"numbers +C0_-,.!%()&/"', "aA0_-"),
    ]

    for idx, (table_name, connector_name) in enumerate(scenarios):
        cur_pipeline_name = f"{pipeline_name}-{idx}"
        cleanup_pipeline(cur_pipeline_name)

        sql = f"""
        CREATE TABLE {table_name} (
            num DOUBLE
        ) WITH (
            'connectors' = '[{{
                "name": "{connector_name}",
                "transport": {{
                    "name": "datagen",
                    "config": {{"plan": [{{ "rate": 100, "fields": {{ "num": {{ "range": [0, 1000], "strategy": "uniform" }} }} }}]}}
                }}
            }}]'
        );
        """.strip()

        create_pipeline(cur_pipeline_name, sql)
        start_pipeline_as_paused(cur_pipeline_name)
        wait_for_pipeline_reachable(cur_pipeline_name)

        if FELDERA_TEST_NUM_HOSTS > 1:
            # The multihost coordinator can report that it is ready
            # before some of the hosts are individually ready, but the
            # coordinator only reports statistics when all of them are
            # ready.  This might be a bug in the coordinator; it is
            # hard to tell.  For now, waiting for statistics to be
            # available is a compromise that allows this otherwise
            # valuable test to pass.
            wait_for_condition(
                f"pipeline stats for {cur_pipeline_name} are available",
                lambda: (
                    get(api_url(f"/pipelines/{cur_pipeline_name}/stats")).status_code
                    == HTTPStatus.OK
                ),
                timeout_s=30.0,
                poll_interval_s=1.0,
            )

        # Initial: pipeline paused, connector running, processed=0
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert p_paused
        assert not c_paused
        assert processed == 0

        # Pause connector
        resp = connector_action(cur_pipeline_name, table_name, connector_name, "pause")
        assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
        wait_for_condition(
            "connector pause observed",
            lambda: _basic_orchestration_info(
                cur_pipeline_name, table_name, connector_name
            )[1],
            timeout_s=10.0,
            poll_interval_s=0.5,
        )
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert p_paused
        assert c_paused
        assert processed == 0

        # Start pipeline
        resume_pipeline(cur_pipeline_name)
        wait_for_pipeline_reachable(cur_pipeline_name)
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert not p_paused
        assert c_paused
        assert processed == 0

        # Start connector
        resp = connector_action(cur_pipeline_name, table_name, connector_name, "start")
        assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
        wait_for_condition(
            "connector start observed",
            lambda: (
                not _basic_orchestration_info(
                    cur_pipeline_name, table_name, connector_name
                )[1]
            ),
            timeout_s=10.0,
            poll_interval_s=0.5,
        )
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert not p_paused
        assert not c_paused
        assert processed >= 0  # Some records likely processed quickly
        reset_pipeline(cur_pipeline_name)


@gen_pipeline_name
def test_pipeline_orchestration_errors(pipeline_name):
    """
    Port of Rust pipeline_orchestration_errors:
    - Validate return codes for valid/invalid pipeline & connector actions.
    """
    sql = """
    CREATE TABLE numbers1 (
        num DOUBLE
    ) WITH (
        'connectors' = '[{
            "name": "c1",
            "transport": {
                "name": "datagen",
                "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
            }
        }]'
    );
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline_as_paused(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)

    # ACCEPTED endpoints
    for endpoint in [
        f"/pipelines/{pipeline_name}/resume",
        f"/pipelines/{pipeline_name}/pause",
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.ACCEPTED, (endpoint, resp.status_code)

    # OK endpoints (connector start/pause, case variations)
    for endpoint in [
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/c1/start",
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/c1/pause",
        f"/pipelines/{pipeline_name}/tables/Numbers1/connectors/c1/pause",
        f"/pipelines/{pipeline_name}/tables/NUMBERS1/connectors/c1/pause",
        f"/pipelines/{pipeline_name}/tables/%22numbers1%22/connectors/c1/pause",
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.OK, (endpoint, resp.status_code)

    # BAD REQUEST endpoints (invalid connector action)
    for endpoint in [
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/c1/action2",  # Invalid connector action
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/c1/START",  # Invalid connector action (case-sensitive)
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.BAD_REQUEST, (endpoint, resp.status_code)

    # NOT FOUND endpoints
    for endpoint in [
        f"/pipelines/{pipeline_name}/action2",  # Invalid pipeline action
        f"/pipelines/{pipeline_name}/Start",  # Invalid pipeline action (case-sensitive)
        f"/pipelines/{pipeline_name}X/start",  # Pipeline not found
        f"/pipelines/{pipeline_name}X/tables/numbers1/connectors/c1/start",  # Pipeline not found
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/c2/start",  # Connector not found
        f"/pipelines/{pipeline_name}/tables/numbers1/connectors/C1/start",  # Connector not found (case-sensitive)
        f"/pipelines/{pipeline_name}/tables/numbers2/connectors/c1/start",  # Table not found
        f"/pipelines/{pipeline_name}/tables/numbers2/connectors/c2/start",  # Table and connector not found
        f"/pipelines/{pipeline_name}/tables/%22Numbers1%22/connectors/c1/pause",  # Table not found (case-sensitive due to double quotes)
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.NOT_FOUND, (endpoint, resp.status_code)


@gen_pipeline_name
def test_pipeline_orchestration_scenarios(pipeline_name):
    """
    Tests for orchestration that the effects (i.e., pipeline and connector state) are
    indeed as expected after each scenario consisting of various start and pause steps.
    """
    sql = """
    CREATE TABLE numbers (
        num DOUBLE
    ) WITH (
        'connectors' = '[
            {
                "name": "c1",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [0, 1000], "strategy": "uniform" } } }]}
                }
            },
            {
                "name": "c2",
                "transport": {
                    "name": "datagen",
                    "config": {"plan": [{ "rate": 100, "fields": { "num": { "range": [1000, 2000], "strategy": "uniform" } } }]}
                }
            }
        ]'
    );
    """.strip()
    create_pipeline(pipeline_name, sql)
    stop_pipeline(pipeline_name, force=True)

    class Step:
        START_PIPELINE = "start_pipeline"
        START_PIPELINE_AS_PAUSED = "start_pipeline_as_paused"
        PAUSE_PIPELINE = "pause_pipeline"
        START_CONNECTOR_1 = "start_connector_1"
        PAUSE_CONNECTOR_1 = "pause_connector_1"
        START_CONNECTOR_2 = "start_connector_2"
        PAUSE_CONNECTOR_2 = "pause_connector_2"

    scenarios = [
        # Paused pipeline combinations
        ([Step.START_PIPELINE_AS_PAUSED], True, False, False),
        ([Step.START_PIPELINE_AS_PAUSED, Step.PAUSE_CONNECTOR_1], True, True, False),
        ([Step.START_PIPELINE_AS_PAUSED, Step.PAUSE_CONNECTOR_2], True, False, True),
        (
            [
                Step.START_PIPELINE_AS_PAUSED,
                Step.PAUSE_CONNECTOR_1,
                Step.PAUSE_CONNECTOR_2,
            ],
            True,
            True,
            True,
        ),
        # Running pipeline combinations
        ([Step.START_PIPELINE], False, False, False),
        (
            [Step.START_PIPELINE, Step.PAUSE_CONNECTOR_1],
            False,
            True,
            False,
        ),
        (
            [Step.START_PIPELINE, Step.PAUSE_CONNECTOR_2],
            False,
            False,
            True,
        ),
        (
            [
                Step.START_PIPELINE,
                Step.PAUSE_CONNECTOR_1,
                Step.PAUSE_CONNECTOR_2,
            ],
            False,
            True,
            True,
        ),
        # Start then pause pipeline
        ([Step.START_PIPELINE, Step.PAUSE_PIPELINE], True, False, False),
        # Pause connector then start it again
        (
            [
                Step.START_PIPELINE,
                Step.PAUSE_CONNECTOR_1,
                Step.START_CONNECTOR_1,
            ],
            False,
            False,
            False,
        ),
    ]

    def apply_step(step: str):
        if step == Step.START_PIPELINE:
            start_pipeline(pipeline_name)
            wait_for_pipeline_reachable(pipeline_name)
        elif step == Step.START_PIPELINE_AS_PAUSED:
            start_pipeline_as_paused(pipeline_name)
            wait_for_pipeline_reachable(pipeline_name)
        elif step == Step.PAUSE_PIPELINE:
            pause_pipeline(pipeline_name)
        elif step == Step.START_CONNECTOR_1:
            resp = connector_action(pipeline_name, "numbers", "c1", "start")
            assert resp.status_code == HTTPStatus.OK
        elif step == Step.PAUSE_CONNECTOR_1:
            resp = connector_action(pipeline_name, "numbers", "c1", "pause")
            assert resp.status_code == HTTPStatus.OK
        elif step == Step.START_CONNECTOR_2:
            resp = connector_action(pipeline_name, "numbers", "c2", "start")
            assert resp.status_code == HTTPStatus.OK
        elif step == Step.PAUSE_CONNECTOR_2:
            resp = connector_action(pipeline_name, "numbers", "c2", "pause")
            assert resp.status_code == HTTPStatus.OK
        else:
            raise AssertionError(f"Unknown step {step}")

    for steps, exp_pipe_paused, exp_c1_paused, exp_c2_paused in scenarios:
        # Apply steps
        for s in steps:
            apply_step(s)

        st = pipeline_stats(pipeline_name)
        pipeline_paused = (
            PipelineStatus.from_str(st["global_metrics"]["state"])
            == PipelineStatus.PAUSED
        )
        inputs = st["inputs"]
        c1_paused = next(i for i in inputs if i["endpoint_name"] == "numbers.c1")[
            "paused"
        ]
        c2_paused = next(i for i in inputs if i["endpoint_name"] == "numbers.c2")[
            "paused"
        ]
        actual = (pipeline_paused, c1_paused, c2_paused)
        expected = (exp_pipe_paused, exp_c1_paused, exp_c2_paused)
        assert actual == expected, f"Steps {steps} => {actual} expected {expected}"

        reset_pipeline(pipeline_name)


@gen_pipeline_name
def test_output_connector_orchestration(pipeline_name):
    """
    A paused output connector stops writing to its sink while the pipeline runs
    on, and picks up again when it is started.
    """
    # A real file: the file connector fsyncs after every write, and fsync fails
    # on /dev/null.
    sink_path = f"/tmp/feldera_output_pause_{uuid.uuid4().hex}.json"
    sql = f"""
    CREATE TABLE numbers (
        num DOUBLE
    ) WITH (
        'connectors' = '[{{
            "name": "gen",
            "transport": {{
                "name": "datagen",
                "config": {{"plan": [{{ "rate": 100, "fields": {{ "num": {{ "range": [0, 1000], "strategy": "uniform" }} }} }}]}}
            }}
        }}]'
    );

    CREATE MATERIALIZED VIEW v
    WITH (
        'connectors' = '[{{
            "name": "sink",
            "transport": {{ "name": "file_output", "config": {{ "path": "{sink_path}" }} }},
            "format": {{ "name": "json" }}
        }}]'
    )
    AS SELECT * FROM numbers;
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)

    def transmitted() -> int:
        return output_connector_stats(pipeline_name, "v", "sink")["metrics"][
            "transmitted_records"
        ]

    def transmitting() -> bool:
        metrics = output_connector_stats(pipeline_name, "v", "sink")["metrics"]
        # Without this the test would just time out if the sink rejected every
        # write, which says nothing about why.
        assert metrics["num_transport_errors"] == 0, (
            f"sink is failing writes: {output_connector_stats(pipeline_name, 'v', 'sink')}"
        )
        return metrics["transmitted_records"] > 0

    # The connector starts out running and writing.
    assert not output_connector_paused(pipeline_name, "v", "sink")
    wait_for_condition(
        "output connector transmits records",
        transmitting,
        timeout_s=30.0,
        poll_interval_s=0.5,
    )

    # Pause it: the records the pipeline keeps producing no longer reach the
    # sink, but it does report them as processed.
    resp = output_connector_action(pipeline_name, "v", "sink", "pause")
    assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
    wait_for_condition(
        "output connector pause observed",
        lambda: output_connector_paused(pipeline_name, "v", "sink"),
        timeout_s=10.0,
        poll_interval_s=0.5,
    )

    # Pausing bounds the connector's queue rather than emptying it: the output
    # it was handed before the pause is still owed to the sink, and the step in
    # flight when the pause landed had already decided to hand it that step's
    # output too. This sink fsyncs every write, so that backlog is real and
    # takes time to drain.
    #
    # Both are behind us once the connector has processed a step that started
    # after the pause was observed: from that step on it is handed empty
    # batches, and it processes the steps in order.
    steps_at_pause = pipeline_stats(pipeline_name)["global_metrics"][
        "total_initiated_steps"
    ]
    wait_for_condition(
        "paused connector drains the output it was handed before the pause",
        lambda: (
            output_connector_stats(pipeline_name, "v", "sink")["metrics"][
                "total_processed_steps"
            ]
            > steps_at_pause
        ),
        timeout_s=30.0,
        poll_interval_s=0.5,
    )
    metrics = output_connector_stats(pipeline_name, "v", "sink")["metrics"]
    assert metrics["queued_records"] == 0
    paused_at = metrics["transmitted_records"]

    # The connector is now quiet, and stays quiet however much the pipeline
    # produces. It does not hold the pipeline back either: it reports the
    # output it discards as processed, so its own progress counter passes the
    # pipeline's count at the moment it went quiet.
    processed_at_pause = pipeline_stats(pipeline_name)["global_metrics"][
        "total_processed_records"
    ]
    wait_for_condition(
        "paused connector reports input processed after the pause",
        lambda: (
            output_connector_stats(pipeline_name, "v", "sink")["metrics"][
                "total_processed_input_records"
            ]
            > processed_at_pause
        ),
        timeout_s=30.0,
        poll_interval_s=0.5,
    )
    metrics = output_connector_stats(pipeline_name, "v", "sink")["metrics"]
    assert metrics["transmitted_records"] == paused_at
    assert metrics["queued_records"] == 0

    # Start it again: it resumes with the output produced from now on.
    resp = output_connector_action(pipeline_name, "v", "sink", "start")
    assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
    wait_for_condition(
        "output connector transmits again",
        lambda: transmitted() > paused_at,
        timeout_s=30.0,
        poll_interval_s=0.5,
    )
    assert not output_connector_paused(pipeline_name, "v", "sink")

    reset_pipeline(pipeline_name)


@gen_pipeline_name
def test_output_connector_orchestration_errors(pipeline_name):
    """
    Return codes of the output connector action endpoint, and the `command`
    endpoint that shares its URL shape.
    """
    sql = """
    CREATE TABLE numbers (
        num DOUBLE
    );

    CREATE MATERIALIZED VIEW v
    WITH (
        'connectors' = '[{
            "name": "sink",
            "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
            "format": { "name": "json" }
        }]'
    )
    AS SELECT * FROM numbers;
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline_as_paused(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)

    for view_name in ["v", "V", "%22v%22"]:
        for action in ["start", "pause"]:
            resp = post_no_body(
                api_url(
                    f"/pipelines/{pipeline_name}/views/{view_name}/connectors/sink/{action}"
                )
            )
            assert resp.status_code == HTTPStatus.OK, (view_name, action, resp.text)

    for endpoint in [
        f"/pipelines/{pipeline_name}/views/v/connectors/sink/action2",  # Invalid action
        f"/pipelines/{pipeline_name}/views/v/connectors/sink/START",  # Case-sensitive
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.BAD_REQUEST, (endpoint, resp.status_code)

    for endpoint in [
        f"/pipelines/{pipeline_name}X/views/v/connectors/sink/start",  # Pipeline not found
        f"/pipelines/{pipeline_name}/views/v/connectors/sink2/start",  # Connector not found
        f"/pipelines/{pipeline_name}/views/v2/connectors/sink/start",  # View not found
        f"/pipelines/{pipeline_name}/views/%22V%22/connectors/sink/pause",  # View not found (case-sensitive due to double quotes)
    ]:
        resp = post_no_body(api_url(endpoint))
        assert resp.status_code == HTTPStatus.NOT_FOUND, (endpoint, resp.status_code)

    # `command` is a literal path segment, so it must still reach the command
    # endpoint rather than being read as an action. The file connector does not
    # support commands, which is a different error than an invalid action.
    resp = post_json(
        api_url(f"/pipelines/{pipeline_name}/views/v/connectors/sink/command"),
        {"command": "flush"},
    )
    assert resp.json()["error_code"] == "CommandError", (resp.status_code, resp.text)

    reset_pipeline(pipeline_name)
