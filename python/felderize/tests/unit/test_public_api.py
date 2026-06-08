"""The minimal public API is importable straight from the `felderize` package.

Keeps the documented programmatic entry point (`from felderize import ...`)
working, independent of internal module layout.
"""

from __future__ import annotations

import felderize


def test_all_lists_the_public_api():
    assert set(felderize.__all__) == {
        "translate_spark_to_feldera",
        "Config",
        "TranslationResult",
        "Status",
    }


def test_public_names_are_importable():
    from felderize import (
        Config,
        Status,
        TranslationResult,
        translate_spark_to_feldera,
    )

    assert callable(translate_spark_to_feldera)
    assert Config.from_env() is not None
    assert TranslationResult().status is Status.SUCCESS
    assert {s.value for s in Status} == {"success", "unsupported", "error"}
