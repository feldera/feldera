"""Every pipeline name a test builds must fit the platform's limit.

A name that overflows fails at pipeline creation, in whichever CI suite stamps
the longest tag, long after the test that chose the name was written. Catch it
here instead, by measuring the names statically against the longest tag any
suite prepends.
"""

import ast
from pathlib import Path

import pytest

from feldera.testutils import PIPELINE_NAME_MAX_LEN, variant_pipeline_name

# The longest prefix `unique_pipeline_name` prepends: five characters of the
# commit SHA, the one-character `python-multihost` suite tag, and the separator.
LONGEST_TAG_LEN = len("abcde") + len("m") + len("_")

TESTS_DIR = Path(__file__).resolve().parents[1]


def _variants(function: ast.FunctionDef) -> list[str]:
    """The literal variants `function` names itself's pipelines after."""
    variants = []
    for node in ast.walk(function):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "variant_pipeline_name"
            and len(node.args) == 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            variants.append(node.args[1].value)
    return variants


def _pipeline_names() -> list[tuple[str, str]]:
    """Every (source file, pipeline name) pair a test names statically.

    The name a test runs under is `unique_pipeline_name(<test name>)`, so it is
    the test's own name behind the tag, plus a variant where the test creates
    more than one pipeline.
    """
    names = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            if not node.name.startswith("test_"):
                continue
            if "pipeline_name" not in (arg.arg for arg in node.args.args):
                continue
            names.append((str(path), node.name))
            for variant in _variants(node):
                names.append((str(path), f"{node.name}_{variant}"))
    return names


def test_test_pipeline_names_fit_the_limit():
    budget = PIPELINE_NAME_MAX_LEN - LONGEST_TAG_LEN
    too_long = [
        f"{path}: '{name}' is {len(name)} chars, {len(name) - budget} over"
        for path, name in _pipeline_names()
        if len(name) > budget
    ]
    assert not too_long, (
        f"these test names leave no room for the {LONGEST_TAG_LEN}-char CI tag within "
        f"the {PIPELINE_NAME_MAX_LEN}-char pipeline name limit:\n" + "\n".join(too_long)
    )


def test_variant_pipeline_name_appends_the_variant():
    assert variant_pipeline_name("abcde_test_delta", "auto") == "abcde_test_delta_auto"


def test_variant_pipeline_name_rejects_an_overlong_name():
    base = "a" * (PIPELINE_NAME_MAX_LEN - len("_auto"))
    # One character of headroom is all the limit leaves, and the variant fills it.
    assert variant_pipeline_name(base, "auto") == f"{base}_auto"
    with pytest.raises(AssertionError, match="exceeding"):
        variant_pipeline_name(f"{base}a", "auto")
