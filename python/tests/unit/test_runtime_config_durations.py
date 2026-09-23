"""Duration settings of :class:`RuntimeConfig`.

Every duration-shaped runtime setting has two spellings: a string with a unit,
and a deprecated integer under the older name. These tests pin down what the shim
produces for each spelling, because the conversion is the only place that
decides which unit an integer meant.
"""

import warnings

import pytest

from feldera.enums import FaultToleranceModel
from feldera.runtime_config import (
    Resources,
    RuntimeConfig,
    Storage,
    _duration_setting,
)

# Every duration setting RuntimeConfig accepts, as
# (deprecated argument, replacement argument, unit, sample integer).
DURATION_SETTINGS = [
    ("max_buffering_delay_usecs", "max_buffering_delay", "us", 1500),
    ("clock_resolution_usecs", "clock_resolution", "us", 1_000_000),
    ("provisioning_timeout_secs", "provisioning_timeout", "s", 300),
    ("checkpoint_interval_secs", "checkpoint_interval", "s", 60),
]

# The checkpoint interval lives under `fault_tolerance`, and the pipeline only
# accepts it once a model is chosen.
NESTED_SETTINGS = {"checkpoint_interval": "fault_tolerance"}


def _setting(config: dict, name: str):
    """The value `name` holds in `config`, wherever the schema nests it."""
    parent = NESTED_SETTINGS.get(name)
    return config[parent][name] if parent else config[name]


def _build(**kwargs) -> dict:
    """A config dictionary, with a fault tolerance model so that the
    checkpoint interval survives."""
    return RuntimeConfig(
        fault_tolerance_model=FaultToleranceModel.AtLeastOnce, **kwargs
    ).to_dict()


@pytest.mark.parametrize(
    "new_name", [new_name for _, new_name, _, _ in DURATION_SETTINGS]
)
def test_duration_argument_reaches_the_dictionary_unchanged(new_name):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        config = _build(**{new_name: "1h30m"})
    assert _setting(config, new_name) == "1h30m"


@pytest.mark.parametrize(
    "old_name,new_name,unit,value",
    DURATION_SETTINGS,
    ids=[s[0] for s in DURATION_SETTINGS],
)
def test_deprecated_integer_becomes_a_duration_string(old_name, new_name, unit, value):
    with pytest.warns(DeprecationWarning):
        config = _build(**{old_name: value})
    assert _setting(config, new_name) == f"{value}{unit}"


@pytest.mark.parametrize(
    "old_name,new_name,unit,value",
    DURATION_SETTINGS,
    ids=[s[0] for s in DURATION_SETTINGS],
)
def test_deprecated_integer_warns_and_names_its_replacement(
    old_name, new_name, unit, value
):
    with pytest.warns(DeprecationWarning) as caught:
        _build(**{old_name: value})
    messages = [str(w.message) for w in caught]
    assert messages == [
        f"'{old_name}' is deprecated; use {new_name}='{value}{unit}' instead"
    ]


@pytest.mark.parametrize(
    "old_name,new_name,unit,value",
    DURATION_SETTINGS,
    ids=[s[0] for s in DURATION_SETTINGS],
)
def test_duration_argument_wins_over_the_deprecated_one_and_says_so(
    old_name, new_name, unit, value
):
    """Giving both spellings keeps the current one and drops the integer.
    The caller is told, because the dropped argument otherwise looks applied."""
    with pytest.warns(DeprecationWarning) as caught:
        config = _build(**{new_name: "42ms", old_name: value})
    assert _setting(config, new_name) == "42ms"
    assert old_name not in config
    assert old_name not in config.get("fault_tolerance", {})
    assert [str(w.message) for w in caught] == [
        f"'{old_name}' is deprecated and was ignored because "
        f"{new_name}='42ms' is also set; drop "
        f"{old_name}={value} (it would mean '{value}{unit}')"
    ]


def test_deprecated_integer_zero_converts_rather_than_dropping_out():
    """Zero is a length of time, not an absent setting."""
    with pytest.warns(DeprecationWarning):
        config = _build(max_buffering_delay_usecs=0)
    assert config["max_buffering_delay"] == "0us"


def test_unset_durations_stay_out_of_the_dictionary():
    config = RuntimeConfig(workers=8).to_dict()
    assert "max_buffering_delay" not in config
    assert "clock_resolution" not in config
    assert "provisioning_timeout" not in config
    assert "fault_tolerance" not in config


def test_checkpoint_interval_needs_a_fault_tolerance_model():
    """Without a model there is no `fault_tolerance` object to hold the
    interval, so both spellings are dropped."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        config = RuntimeConfig(
            checkpoint_interval_secs=60, checkpoint_interval="60s"
        ).to_dict()
    assert "fault_tolerance" not in config
    assert "checkpoint_interval" not in config


def test_a_model_without_an_interval_sends_an_explicit_null():
    """A null interval disables periodic checkpoints, which differs from
    leaving the key out, so `to_dict` must keep it."""
    config = RuntimeConfig(
        fault_tolerance_model=FaultToleranceModel.AtLeastOnce
    ).to_dict()
    assert config["fault_tolerance"] == {
        "model": "at_least_once",
        "checkpoint_interval": None,
    }


def test_nested_storage_and_resources_keep_their_own_shape():
    config = _build(
        storage=Storage(min_storage_bytes=1024),
        resources=Resources(cpu_cores_max=4),
        checkpoint_interval="30s",
    )
    assert config["storage"]["min_storage_bytes"] == 1024
    assert config["resources"]["cpu_cores_max"] == 4
    assert config["fault_tolerance"]["checkpoint_interval"] == "30s"


def test_round_trip_preserves_both_spellings():
    """`from_dict` is a passthrough, so a dictionary written against the old
    field names survives a round trip untouched."""
    original = {
        "workers": 8,
        "max_buffering_delay_usecs": 0,
        "clock_resolution": "1s",
        "fault_tolerance": {
            "model": "at_least_once",
            "checkpoint_interval_secs": 60,
            "checkpoint_interval": "60s",
        },
    }
    assert RuntimeConfig.from_dict(dict(original)).to_dict() == original


def test_storage_rejects_a_value_that_is_neither_flag_nor_storage():
    with pytest.raises(ValueError, match="Unknown value"):
        RuntimeConfig(storage=1024)


@pytest.mark.parametrize(
    "unit,value,expected",
    [("us", 1500, "1500us"), ("ms", 500, "500ms"), ("s", 30, "30s"), ("d", 7, "7d")],
)
def test_helper_appends_the_unit_it_is_given(unit, value, expected):
    """The helper serves fields in every unit the configuration uses, so cover
    the units no RuntimeConfig field happens to take yet."""
    with pytest.warns(DeprecationWarning):
        assert _duration_setting("new", None, "old", value, unit) == expected


def test_helper_passes_an_absent_value_through_without_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert _duration_setting("new", None, "old", None, "s") is None
        assert _duration_setting("new", "30s", "old", None, "s") == "30s"
