"""Duration settings of :class:`RuntimeConfig`.

Every duration-shaped runtime setting has two spellings: a string with a unit,
and a deprecated integer under the older name. The SDK sends each setting in the
spelling it was given, so code written against the older argument keeps working
against a server that knows only that spelling. These tests pin down what goes
out for each spelling and what the attributes read back.
"""

import warnings

import pytest

from feldera.enums import FaultToleranceModel
from feldera.runtime_config import (
    Resources,
    RuntimeConfig,
    Storage,
    _duration_setting,
    _parse_duration_nanos,
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
NESTED_SETTINGS = {
    "checkpoint_interval": "fault_tolerance",
    "checkpoint_interval_secs": "fault_tolerance",
}


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
def test_deprecated_integer_is_sent_under_its_own_key(old_name, new_name, unit, value):
    """A server from before the current names reads only the older key, so the
    integer goes out as it came in rather than translated."""
    with pytest.warns(DeprecationWarning):
        config = _build(**{old_name: value})
    assert _setting(config, old_name) == value
    assert new_name not in config
    assert new_name not in config.get("fault_tolerance", {})


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
    assert config["max_buffering_delay_usecs"] == 0


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


def test_round_trip_keeps_every_key_in_the_spelling_it_was_read_in():
    """`from_dict` then `to_dict` changes nothing, so a configuration read from
    a server goes back to it in the spelling that server knows."""
    original = {
        "workers": 8,
        "max_buffering_delay_usecs": 0,
        "clock_resolution": "1s",
        "fault_tolerance": {"model": "at_least_once", "checkpoint_interval_secs": 60},
    }
    assert RuntimeConfig.from_dict(original).to_dict() == original


def test_storage_rejects_a_value_that_is_neither_flag_nor_storage():
    with pytest.raises(ValueError, match="Unknown value"):
        RuntimeConfig(storage=1024)


@pytest.mark.parametrize(
    "unit,value,expected",
    [("us", 1500, "1500us"), ("ms", 500, "500ms"), ("s", 30, "30s"), ("d", 7, "7d")],
)
def test_helper_names_the_replacement_in_the_unit_it_is_given(unit, value, expected):
    """The helper serves fields in every unit the configuration uses, so cover
    the units no RuntimeConfig field happens to take yet. The unit only shows
    in the advice: the value itself goes out under the older key."""
    with pytest.warns(DeprecationWarning, match=f"new='{expected}'"):
        assert _duration_setting("new", None, "old", value, unit) == ("old", value)


def test_helper_passes_an_absent_value_through_without_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert _duration_setting("new", None, "old", None, "s") == ("new", None)
        assert _duration_setting("new", "30s", "old", None, "s") == ("new", "30s")


# The three deprecated attributes, as (attribute, replacement, unit, nanoseconds
# per unit). The checkpoint interval lives in a dictionary and has no attribute.
LEGACY_ATTRIBUTES = [
    ("max_buffering_delay_usecs", "max_buffering_delay", "us", 1_000),
    ("clock_resolution_usecs", "clock_resolution", "us", 1_000),
    ("provisioning_timeout_secs", "provisioning_timeout", "s", 1_000_000_000),
]


@pytest.mark.parametrize("old_name, new_name, unit, unit_nanos", LEGACY_ATTRIBUTES)
def test_deprecated_attribute_reads_back_the_old_integer(
    old_name, new_name, unit, unit_nanos
):
    """A caller who reads the old attribute gets the whole number of the old
    unit, whichever spelling set the value, and is told to move on."""
    config = RuntimeConfig(**{new_name: f"1500{unit}"})
    with pytest.warns(DeprecationWarning, match=f"read '{new_name}'"):
        assert getattr(config, old_name) == 1500

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        config = RuntimeConfig(**{old_name: 42})
        assert getattr(config, old_name) == 42


@pytest.mark.parametrize("old_name, new_name, unit, unit_nanos", LEGACY_ATTRIBUTES)
def test_deprecated_attribute_rounds_to_the_nearest_old_unit(
    old_name, new_name, unit, unit_nanos
):
    """The old unit cannot hold a fraction of itself, so the attribute rounds
    to the nearest whole unit, halves up."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert getattr(RuntimeConfig(**{new_name: f"2.5{unit}"}), old_name) == 3
        assert getattr(RuntimeConfig(**{new_name: f"2.4{unit}"}), old_name) == 2
        assert getattr(RuntimeConfig(**{new_name: "0"}), old_name) == 0
        assert getattr(RuntimeConfig(), old_name) is None


@pytest.mark.parametrize("old_name, new_name, unit, unit_nanos", LEGACY_ATTRIBUTES)
def test_deprecated_attribute_can_still_be_assigned(
    old_name, new_name, unit, unit_nanos
):
    """Assigning the old attribute stores the integer under the old key, which
    every server reads, and the current attribute reads it back as a duration.
    The dictionary sent to the server carries one spelling."""
    config = RuntimeConfig(**{new_name: "1s"})
    with pytest.warns(DeprecationWarning, match=f"set '{new_name}'"):
        setattr(config, old_name, 7)
    assert config.to_dict()[old_name] == 7
    assert new_name not in config.to_dict()
    assert getattr(config, new_name) == f"7{unit}"


@pytest.mark.parametrize("old_name, new_name, unit, unit_nanos", LEGACY_ATTRIBUTES)
def test_a_bare_number_under_the_current_name_reads_back_as_it_is(
    old_name, new_name, unit, unit_nanos
):
    """The server reads a bare number under the current name in the old unit,
    so the deprecated attribute hands it back unconverted rather than failing
    to parse it as a duration string."""
    config = RuntimeConfig.from_dict({new_name: 1000})
    with pytest.warns(DeprecationWarning):
        assert getattr(config, old_name) == 1000


def test_from_dict_reads_an_older_key_under_both_names():
    """A configuration stored before the current names carries the older keys.
    Both the current and the deprecated attribute read such a key, and it goes
    back out as it came in."""
    config = RuntimeConfig.from_dict({"clock_resolution_usecs": 250})
    assert config.clock_resolution == "250us"
    with pytest.warns(DeprecationWarning):
        assert config.clock_resolution_usecs == 250
    assert config.to_dict() == {"clock_resolution_usecs": 250}


# A runtime configuration as a release before the rename stored it: every
# duration spelled out under its older key, including the explicit nulls.
PRE_RENAME_STORED = {
    "workers": 4,
    "max_buffering_delay_usecs": 0,
    "clock_resolution_usecs": None,
    "provisioning_timeout_secs": 600,
    "fault_tolerance": {"model": "at_least_once", "checkpoint_interval_secs": None},
}


def test_read_modify_write_of_a_stored_pre_rename_config_sends_one_spelling():
    """Reading a configuration stored before the current names, changing one
    setting under its current name and writing it back must send that setting
    once, because the server rejects a configuration that carries both
    spellings, and must leave the other settings as they were read."""
    config = RuntimeConfig.from_dict(PRE_RENAME_STORED)
    config.max_buffering_delay = "5ms"
    sent = config.to_dict()
    assert sent == {
        "workers": 4,
        "max_buffering_delay": "5ms",
        "provisioning_timeout_secs": 600,
        "fault_tolerance": {"model": "at_least_once", "checkpoint_interval_secs": None},
    }
    assert "max_buffering_delay_usecs" not in sent


def test_from_dict_leaves_the_caller_dictionary_alone():
    stored = dict(PRE_RENAME_STORED)
    RuntimeConfig.from_dict(stored)
    assert stored == PRE_RENAME_STORED


@pytest.mark.parametrize(
    "text, nanos",
    [
        ("0", 0),
        ("5ns", 5),
        ("5us", 5_000),
        ("5\u00b5s", 5_000),
        ("5ms", 5_000_000),
        ("1h30m", 5_400 * 1_000_000_000),
        ("1h1m1s500ms", 3_661 * 1_000_000_000 + 500_000_000),
        ("0.5s", 500_000_000),
        ("+30s", 30 * 1_000_000_000),
        ("1.5us", 1_500),
        ("0.0000000005s", 1),
        ("30d", 30 * 86_400 * 1_000_000_000),
        # A point with digits on one side only, as Go and the runtime take it.
        ("5.s", 5 * 1_000_000_000),
        (".5s", 500_000_000),
        # The runtime reads 18 digits after the point; the 19th would round up.
        ("0.0000000000000057879d", 0),
        # The largest duration the runtime holds.
        ("18446744073709551615s", (2**64 - 1) * 1_000_000_000),
    ],
)
def test_parser_agrees_with_the_pipeline(text, nanos):
    assert _parse_duration_nanos(text) == nanos


@pytest.mark.parametrize(
    "text, message",
    [
        ("", "empty"),
        ("-5s", "negative"),
        ("30", "has no unit"),
        ("1s0", "has no unit"),
        ("30sec", "unknown unit 'sec'"),
        ("s", "no number in front"),
        ("1..2s", "has no unit"),
        ("18446744073709551616s", "too large"),
        # Digits from other scripts are not digits to the runtime.
        ("\u0661s", "unknown unit"),
        ("1 s", "unknown unit ' s'"),
    ],
)
def test_parser_rejects_what_the_pipeline_rejects(text, message):
    with pytest.raises(ValueError, match=message):
        _parse_duration_nanos(text)
