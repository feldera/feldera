"""A configuration written in the superseded size spellings still runs.

Every size setting was renamed, and the name it had is now an alias that also
accepts the bare number of megabytes or bytes it always took. Unit tests pin
down the parser; these pin down the promise that matters to somebody
upgrading, which is that a pipeline stored before the rename starts, runs, and
is deployed with the sizes the old spellings meant.
"""

from feldera.enums import PipelineStatus
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests.platform.helper import PipelineTestCase, get_pipeline
from tests.platform.test_deprecated_duration_spellings import (
    _create_raw,
    _deployment_config,
    _now_from,
)

# A runtime configuration in the spellings a pre-rename release wrote: the unit
# lives in the key name, and the value is a bare number.
LEGACY_RUNTIME_CONFIG = {
    "workers": FELDERA_TEST_NUM_WORKERS,
    "hosts": FELDERA_TEST_NUM_HOSTS,
    # 2 gigabytes, written in megabytes as the old key required.
    "max_rss_mb": 2000,
    "storage": {
        # 256 mebibytes and 10 mebibytes.
        "cache_mib": 256,
        "min_storage_bytes": 10 * 1024 * 1024,
    },
}

# The same settings under the names that replaced them.
EQUIVALENT_CURRENT_CONFIG = {
    "max_rss": "2G",
    "storage": {"cache_size": "256Mi", "min_spill_size": "10Mi"},
}


def _assert_current_spelling(config: dict, what: str) -> None:
    """Asserts that `config` carries each setting under its current name, at
    the value the old spelling meant, and nothing under the old name."""
    assert config.get("max_rss") == "2G", f"{what}: max_rss = {config.get('max_rss')!r}"
    storage = config.get("storage") or {}
    for name, expected in EQUIVALENT_CURRENT_CONFIG["storage"].items():
        assert storage.get(name) == expected, (
            f"{what}: storage.{name} = {storage.get(name)!r}, expected {expected!r}"
        )
    for legacy in ("max_rss_mb",):
        assert legacy not in config, f"{what}: `{legacy}` survived the round trip"
    for legacy in ("cache_mib", "min_storage_bytes"):
        assert legacy not in storage, f"{what}: `{legacy}` survived the round trip"


class TestDeprecatedSizeSpellings(PipelineTestCase):
    def test_legacy_spellings_still_drive_the_pipeline(self):
        """A pipeline configured in the old spellings starts and runs, and the
        configuration it was deployed with carries each setting at the value
        the old spelling meant, under its current name."""
        pipeline_name = self.register_for_cleanup("test_legacy_sizes")
        pipeline = _create_raw(pipeline_name, dict(LEGACY_RUNTIME_CONFIG))

        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        _now_from(pipeline)

        _assert_current_spelling(_deployment_config(pipeline), "deployed")
        pipeline.stop(force=True)

    def test_legacy_spellings_are_stored_under_the_current_names(self):
        """The manager reads the old keys and writes the current ones back, so
        a configuration converts itself the first time it is stored."""
        pipeline_name = self.register_for_cleanup("test_legacy_sizes_stored")
        _create_raw(pipeline_name, dict(LEGACY_RUNTIME_CONFIG))

        stored = get_pipeline(pipeline_name, "all").json()["runtime_config"]
        _assert_current_spelling(stored, "stored")

    def test_writing_both_spellings_of_one_setting_is_rejected(self):
        """Both keys reach one field, so a configuration carrying both is an
        error rather than a contest one of them silently wins."""
        pipeline_name = self.register_for_cleanup("test_both_size_spellings")
        config = dict(LEGACY_RUNTIME_CONFIG)
        config["max_rss"] = "1G"

        with self.assertRaisesRegex(Exception, "duplicate field `max_rss`"):
            _create_raw(pipeline_name, config)
