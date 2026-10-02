"""A configuration written in the superseded duration spellings still runs.

Every duration setting was renamed, and the name it had is now an alias that
also accepts the bare number it always took. Unit tests pin down the parser;
these pin down the promise that matters to somebody upgrading, which is that a
pipeline stored before the rename starts, runs, and behaves as it did.
"""

from feldera.enums import PipelineStatus
from feldera.pipeline import Pipeline
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase, get_pipeline

# A runtime configuration in the spellings a pre-rename release wrote: the unit
# lives in the key name, and the value is a bare number.
LEGACY_RUNTIME_CONFIG = {
    "workers": FELDERA_TEST_NUM_WORKERS,
    "hosts": FELDERA_TEST_NUM_HOSTS,
    # 100 milliseconds, written in microseconds as the old key required.
    "clock_resolution_usecs": 100_000,
    "max_buffering_delay_usecs": 0,
    "provisioning_timeout_secs": 300,
}

# The same three settings under the names that replaced them.
EQUIVALENT_CURRENT_CONFIG = {
    "clock_resolution": "100ms",
    "max_buffering_delay": "0s",
    "provisioning_timeout": "5m",
}

SQL = "CREATE MATERIALIZED VIEW v AS SELECT NOW() as X;"


def _create_raw(pipeline_name: str, runtime_config: dict) -> Pipeline:
    """Creates the pipeline with `runtime_config` exactly as given.

    Through :class:`RuntimeConfig` the SDK would move the older keys to the
    current names before sending them, and the point of these tests is what
    the server does with the older keys.
    """
    TEST_CLIENT.http.put(
        path=f"/pipelines/{pipeline_name}",
        body={
            "name": pipeline_name,
            "description": "",
            "program_code": SQL,
            "program_config": {},
            "runtime_config": runtime_config,
        },
    )
    TEST_CLIENT._wait_for_compilation(pipeline_name)
    return Pipeline.get(pipeline_name, TEST_CLIENT)


def _now_from(pipeline) -> str:
    """The single `NOW()` value the test view holds."""
    result = list(pipeline.query("SELECT * FROM v;"))
    assert len(result) == 1, f"expected one row, got {result}"
    return result[0]["x"]


class TestDeprecatedDurationSpellings(PipelineTestCase):
    def test_legacy_spellings_still_drive_the_pipeline(self):
        """A pipeline configured in the old spellings starts and runs, and the
        configuration it was deployed with carries each setting at the value
        the old spelling meant, under its current name."""
        pipeline_name = self.register_for_cleanup("test_legacy_durations")
        pipeline = _create_raw(pipeline_name, dict(LEGACY_RUNTIME_CONFIG))

        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        _now_from(pipeline)

        deployed = pipeline.deployment_config()
        for name, expected in EQUIVALENT_CURRENT_CONFIG.items():
            assert deployed.get(name) == expected, (
                f"the pipeline was deployed with `{name}` = {deployed.get(name)!r}, "
                f"expected {expected!r}"
            )
        pipeline.stop(force=True)

    def test_legacy_spellings_are_stored_under_the_current_names(self):
        """The manager reads the old keys and writes the current ones back, so
        a configuration converts itself the first time it is stored and stops
        warning thereafter."""
        pipeline_name = self.register_for_cleanup("test_legacy_durations_stored")
        _create_raw(pipeline_name, dict(LEGACY_RUNTIME_CONFIG))

        stored = get_pipeline(pipeline_name, "all").json()["runtime_config"]
        for name, expected in EQUIVALENT_CURRENT_CONFIG.items():
            assert stored.get(name) == expected, (
                f"`{name}` came back as {stored.get(name)!r}, expected {expected!r}"
            )
        for legacy in (
            "clock_resolution_usecs",
            "max_buffering_delay_usecs",
            "provisioning_timeout_secs",
        ):
            assert legacy not in stored, (
                f"`{legacy}` survived the round trip; the superseded key should "
                "not be written back"
            )

    def test_writing_both_spellings_of_one_setting_is_rejected(self):
        """Both keys reach one field, so a configuration carrying both is an
        error rather than a contest one of them silently wins."""
        pipeline_name = self.register_for_cleanup("test_both_duration_spellings")
        config = dict(LEGACY_RUNTIME_CONFIG)
        config["clock_resolution"] = "1s"

        # The duplicate itself must be the reason: the error body also echoes
        # the submitted configuration, so matching the key name alone would
        # pass for any rejection.
        with self.assertRaisesRegex(Exception, "duplicate field `clock_resolution`"):
            _create_raw(pipeline_name, config)
