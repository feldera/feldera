"""Size settings of :class:`RuntimeConfig`, :class:`Resources` and :class:`Storage`.

Every size-shaped setting has two spellings: a string with a unit, and a
deprecated integer under the older name that counts megabytes or bytes. As with
the durations, the SDK sends each setting in the spelling it was given, so
code written against the older argument keeps working against a server that
knows only that spelling. These tests pin down what goes out for each spelling
and what the attributes read back.
"""

import re
import warnings

import pytest

from feldera.runtime_config import (
    Resources,
    RuntimeConfig,
    Storage,
    _parse_size_bytes,
    _size_setting,
)

KIB = 1 << 10
MIB = 1 << 20
GIB = 1 << 30


def _from_config(owner, config: dict):
    """An `owner` holding `config` as read back from a server."""
    if owner is RuntimeConfig:
        return RuntimeConfig.from_dict(config)
    return owner(config=config)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("0", 0),
        ("4096", 4096),
        ("4096B", 4096),
        ("+4096", 4096),
        ("1k", 1_000),
        ("1K", 1_000),
        ("1kB", 1_000),
        ("1Ki", KIB),
        ("1KiB", KIB),
        ("500M", 500_000_000),
        ("512Mi", 512 * MIB),
        ("1G", 1_000_000_000),
        ("1Gi", GIB),
        ("1Ei", 1 << 60),
        ("1.5Gi", GIB + GIB // 2),
        ("1.Gi", GIB),
        (".5Mi", MIB // 2),
        # Rounded to the nearest byte, halves away from zero.
        ("0.5", 1),
        ("0.4", 0),
        ("1.0005k", 1_001),
        ("18446744073709551615", 2**64 - 1),
    ],
)
def test_parse_size_bytes(text, expected):
    assert _parse_size_bytes(text) == expected


@pytest.mark.parametrize(
    "text, message",
    [
        ("", "size is empty"),
        ("-1Gi", "cannot be negative"),
        ("1m", "unknown unit 'm' in size '1m'"),
        ("1g", "unknown unit 'g'"),
        ("1 Mi", "unknown unit ' Mi'"),
        ("Mi", "'Mi' in size 'Mi' has no number in front of it"),
        (".", "size '.' has no digits"),
        ("lots", "unknown unit 'lots' in size 'lots'; expected one of 'k', 'M'"),
        ("18446744073709551616", "too large"),
        ("16Ei", "too large"),
    ],
)
def test_parse_size_bytes_rejects(text, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        _parse_size_bytes(text)


# Every size setting, as
# (owner, deprecated argument, replacement argument, unit, sample integer).
SIZE_SETTINGS = [
    (RuntimeConfig, "max_rss_mb", "max_rss", "M", 16_000),
    (RuntimeConfig, "datafusion_memory_mb", "datafusion_memory", "M", 1_000),
    (Resources, "memory_mb_min", "memory_min", "M", 4_000),
    (Resources, "memory_mb_max", "memory_max", "M", 16_000),
    (Resources, "storage_mb_min", "storage_min", "M", 100_000),
    (Resources, "storage_mb_max", "storage_max", "M", 400_000),
    (Storage, "min_storage_bytes", "min_spill_size", "", 10 * MIB),
]


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_size_argument_reaches_the_dictionary_unchanged(
    owner, old_name, new_name, unit, sample
):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        config = owner(**{new_name: "1.5Gi"}).__dict__
    assert config[new_name] == "1.5Gi"
    assert old_name not in config


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_deprecated_integer_goes_out_under_its_own_key_and_warns(
    owner, old_name, new_name, unit, sample
):
    with pytest.warns(DeprecationWarning, match=f"use {new_name}='{sample}{unit}'"):
        config = owner(**{old_name: sample}).__dict__
    # The older key, which every server reads; nothing under the current one.
    assert config[old_name] == sample
    assert config.get(new_name) is None


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_the_current_name_wins_when_both_are_given(
    owner, old_name, new_name, unit, sample
):
    with pytest.warns(DeprecationWarning, match="was ignored"):
        config = owner(**{old_name: sample, new_name: "2Gi"}).__dict__
    assert config[new_name] == "2Gi"
    assert old_name not in config


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_the_size_attribute_reads_either_spelling(
    owner, old_name, new_name, unit, sample
):
    # Read in the older spelling, as a server from before the rename returns it.
    legacy = _from_config(owner, {old_name: sample})
    assert getattr(legacy, new_name) == f"{sample}{unit}"
    current = _from_config(owner, {new_name: "2Gi"})
    assert getattr(current, new_name) == "2Gi"
    # Assigning the current name replaces both spellings.
    setattr(legacy, new_name, "3Gi")
    assert legacy.__dict__[new_name] == "3Gi"
    assert old_name not in legacy.__dict__


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_the_deprecated_attribute_converts_and_warns(
    owner, old_name, new_name, unit, sample
):
    current = _from_config(owner, {new_name: "1.5Gi"})
    with pytest.warns(DeprecationWarning, match=f"read '{new_name}'"):
        legacy_value = getattr(current, old_name)
    # A whole number of the old unit, rounded up as the manager rounds for an
    # older runtime.
    unit_bytes = 1_000_000 if unit == "M" else 1
    assert legacy_value == -(-(GIB + GIB // 2) // unit_bytes)
    with pytest.warns(DeprecationWarning, match=f"set '{new_name}'"):
        setattr(current, old_name, sample)
    assert current.__dict__[old_name] == sample
    assert new_name not in current.__dict__


@pytest.mark.parametrize("owner, old_name, new_name, unit, sample", SIZE_SETTINGS)
def test_a_unitless_string_is_refused_where_the_old_argument_counted_megabytes(
    owner, old_name, new_name, unit, sample
):
    if unit == "":
        # A byte-counting setting reads "4096" and 4096 alike.
        assert owner(**{new_name: "4096"}).__dict__[new_name] == "4096"
        return
    with pytest.raises(ValueError, match=f"'{sample}{unit}'"):
        owner(**{new_name: str(sample)})


def test_size_setting_without_either_spelling_sends_nothing():
    assert _size_setting("memory_max", None, "memory_mb_max", None, "M") == (
        "memory_max",
        None,
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        resources = Resources().__dict__
    for name in ("memory_max", "memory_mb_max", "storage_min", "storage_max"):
        assert name not in resources, name


@pytest.mark.parametrize(
    "owner, old_name, new_name, unit, sample",
    [setting for setting in SIZE_SETTINGS if setting[0] is not RuntimeConfig],
)
def test_a_config_in_the_older_spelling_is_sent_as_read(
    owner, old_name, new_name, unit, sample
):
    """A `config` read back from a server from before the rename holds the
    older key; nothing is added under the current one beside it, which the
    server would reject as the setting written twice."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        config = owner(config={old_name: sample}).__dict__
    assert config[old_name] == sample
    assert new_name not in config


def test_runtime_config_stores_sizes_in_its_dictionary():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        config = RuntimeConfig(
            max_rss="16G",
            datafusion_memory="1Gi",
            resources=Resources(memory_max="16Gi", storage_max="400Gi"),
            storage=Storage(min_spill_size="10Mi"),
        ).to_dict()
    assert config["max_rss"] == "16G"
    assert config["datafusion_memory"] == "1Gi"
    assert config["resources"]["memory_max"] == "16Gi"
    assert config["resources"]["storage_max"] == "400Gi"
    assert config["storage"]["min_spill_size"] == "10Mi"
    # The deprecated keys travel under their own names.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        legacy = RuntimeConfig(
            max_rss_mb=16_000, resources=Resources(memory_mb_max=16_000)
        ).to_dict()
    assert legacy["max_rss_mb"] == 16_000
    assert legacy["resources"]["memory_mb_max"] == 16_000
    assert "max_rss" not in legacy


def test_from_dict_keeps_the_spelling_it_was_read_in():
    config = RuntimeConfig.from_dict(
        {"max_rss_mb": 16_000, "resources": {"memory_max": "16Gi"}}
    )
    assert config.max_rss == "16000M"
    assert config.to_dict() == {
        "max_rss_mb": 16_000,
        "resources": {"memory_max": "16Gi"},
    }


@pytest.mark.parametrize(
    "owner, old_name, new_name, unit, sample",
    [setting for setting in SIZE_SETTINGS if setting[0] is not RuntimeConfig],
)
def test_config_wins_over_an_argument_under_the_other_name(
    owner, old_name, new_name, unit, sample
):
    """`config` overrides the arguments for a whole setting, so the object
    never carries both spellings, which the server rejects as the setting
    written twice."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        current = owner(config={new_name: "4Gi"}, **{old_name: sample}).__dict__
        legacy = owner(config={old_name: sample}, **{new_name: "4Gi"}).__dict__
    assert current[new_name] == "4Gi"
    assert old_name not in current
    assert legacy[old_name] == sample
    assert new_name not in legacy
