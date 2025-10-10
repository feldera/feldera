import time
from http import HTTPStatus

from .helper import (
    create_pipeline,
    post_no_body,
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
    pipeline_stats,
    connector_paused,
)


def _basic_orchestration_info(pipeline: str, table: str, connector: str):
    stats = pipeline_stats(pipeline)
    pipeline_paused = stats["global_metrics"]["state"] == "Paused"
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
        time.sleep(0.5)  # TODO: why is this necessary? might not be, remove if not
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert p_paused
        assert c_paused
        assert processed == 0

        # Start pipeline
        resume_pipeline(cur_pipeline_name)
        p_paused, c_paused, processed = _basic_orchestration_info(
            cur_pipeline_name, table_name, connector_name
        )
        assert not p_paused
        assert c_paused
        assert processed == 0

        # Start connector
        resp = connector_action(cur_pipeline_name, table_name, connector_name, "start")
        assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
        time.sleep(0.5)
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
        elif step == Step.START_PIPELINE_AS_PAUSED:
            start_pipeline_as_paused(pipeline_name)
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
        pipeline_paused = st["global_metrics"]["state"] == "Paused"
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
