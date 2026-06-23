"""Unit tests for the pipeline tag conventions in :mod:`feldera.tags`."""

from feldera.tags import normalize_tags, tag_display_name


def test_display_name_strips_color_suffix():
    # A trailing "|rrggbb" suffix is removed; everything else is the visible name.
    assert tag_display_name("prod") == "prod"
    assert tag_display_name("prod|ef4444") == "prod"
    assert tag_display_name("team billing|22c55e") == "team billing"
    # Uppercase hex digits are a valid color.
    assert tag_display_name("prod|ABCDEF") == "prod"
    # A name containing "|" but no valid trailing color is returned whole.
    assert tag_display_name("a|b") == "a|b"
    assert tag_display_name("env|staging") == "env|staging"
    # Wrong-length or malformed suffixes are not colors.
    assert tag_display_name("prod|fff") == "prod|fff"
    assert tag_display_name("prod|gggggg") == "prod|gggggg"
    # Only the final "|" segment is the color; an inner "|" stays in the name.
    assert tag_display_name("a|b|ef4444") == "a|b"
    assert tag_display_name("") == ""


def test_normalize_sorts_lexicographically():
    assert normalize_tags(["prod", "dev", "staging"]) == ["dev", "prod", "staging"]


def test_normalize_is_idempotent():
    once = normalize_tags(["beta", "alpha", "alpha|ef4444"])
    assert normalize_tags(once) == once


def test_normalize_empty():
    assert normalize_tags([]) == []


def test_normalize_collapses_color_variants_keeping_last():
    # "prod" and "prod|ef4444" share a display name, so only one survives, and it
    # is the last one supplied -- mirroring the console, where assigning a tag
    # drops the other color variants of the same name.
    assert normalize_tags(["prod", "prod|ef4444"]) == ["prod|ef4444"]
    assert normalize_tags(["prod|ef4444", "prod"]) == ["prod"]


def test_normalize_preserves_color_of_survivor():
    # The surviving variant keeps its color suffix, so its color is not lost.
    result = normalize_tags(["dev", "prod|ef4444", "qa"])
    assert result == ["dev", "prod|ef4444", "qa"]


def test_normalize_distinct_names_with_colors_all_kept():
    # Different display names never collide, regardless of color.
    assert normalize_tags(["b|ef4444", "a|22c55e", "c"]) == [
        "a|22c55e",
        "b|ef4444",
        "c",
    ]


def test_normalize_accepts_any_iterable():
    assert normalize_tags(iter(["b", "a"])) == ["a", "b"]
    assert normalize_tags(("staging", "prod", "staging|22c55e")) == [
        "prod",
        "staging|22c55e",
    ]
