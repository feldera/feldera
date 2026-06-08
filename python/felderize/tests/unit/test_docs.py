"""Unit tests for doc/category detection, section parsing, and example loading.

All network access (`_fetch_doc`) is mocked; these tests are hermetic.
"""

from __future__ import annotations

from felderize import docs


# ---------------------------------------------------------------------------
# _detect_sql_functions / _detect_categories
# ---------------------------------------------------------------------------


def test_detect_sql_functions_uppercases_and_dedupes():
    funcs = docs._detect_sql_functions("SELECT upper(a), Upper(b), LEN(c) FROM t")
    assert funcs == {"UPPER", "LEN"}


def test_detect_sql_functions_none():
    assert docs._detect_sql_functions("SELECT a FROM t") == set()


def test_detect_categories_always_includes_baseline():
    matched = docs._detect_categories("SELECT 1", categories={})
    assert matched == docs._ALWAYS_INCLUDED


def test_detect_categories_matches_pattern():
    cats = {"string": [r"\bUPPER\b"], "array": [r"\bEXPLODE\b"]}
    matched = docs._detect_categories("SELECT UPPER(x) FROM t", categories=cats)
    assert "string" in matched
    assert "array" not in matched


def test_detect_categories_skips_empty_pattern_lists():
    matched = docs._detect_categories("SELECT 1", categories={"string": []})
    assert "string" not in matched


# ---------------------------------------------------------------------------
# _parse_doc_sections
# ---------------------------------------------------------------------------


def test_parse_doc_sections_splits_preamble_and_anchors():
    content = (
        "intro text\n"
        '## UPPER\n<a id="upper"></a>\nupper docs\n'
        '## LOWER\n<a id="lower"></a>\nlower docs\n'
    )
    preamble, sections, anchors = docs._parse_doc_sections(content)
    assert preamble.strip() == "intro text"
    assert set(anchors) == {"upper", "lower"}
    assert anchors["upper"] == "## UPPER"
    assert "upper docs" in sections["## UPPER"]
    assert "lower docs" in sections["## LOWER"]


def test_parse_doc_sections_no_headings_is_all_preamble():
    preamble, sections, anchors = docs._parse_doc_sections("just text\nmore text")
    assert "just text" in preamble
    assert sections == {} and anchors == {}


# ---------------------------------------------------------------------------
# _load_relevant_sections (mocking the parsed-section cache)
# ---------------------------------------------------------------------------


def _fake_sections(monkeypatch, preamble, sections, anchors):
    monkeypatch.setattr(
        docs, "_get_doc_sections", lambda fn: (preamble, sections, anchors)
    )


def test_load_relevant_sections_plain_file_returns_preamble(monkeypatch):
    _fake_sections(monkeypatch, "plain body", {}, {})
    assert docs._load_relevant_sections("x.md", set()) == "plain body"


def test_load_relevant_sections_filters_to_matching_anchor(monkeypatch):
    sections = {"## UPPER": "## UPPER\nupper\n", "## LOWER": "## LOWER\nlower\n"}
    anchors = {"upper": "## UPPER", "lower": "## LOWER"}
    _fake_sections(monkeypatch, "", sections, anchors)
    out = docs._load_relevant_sections("string.md", {"upper"})
    assert "upper" in out
    assert "lower" not in out


def test_load_relevant_sections_no_match_returns_everything(monkeypatch):
    sections = {"## UPPER": "## UPPER\nupper\n", "## LOWER": "## LOWER\nlower\n"}
    anchors = {"upper": "## UPPER"}
    _fake_sections(monkeypatch, "pre", sections, anchors)
    out = docs._load_relevant_sections("string.md", {"missing"})
    assert "upper" in out and "lower" in out and "pre" in out


# ---------------------------------------------------------------------------
# _build_categories_from_index (mocking _fetch_doc)
# ---------------------------------------------------------------------------


def test_build_categories_from_index_parses_funcs_and_anchors(monkeypatch):
    index = "* `UPPER` [string](string.md#upper)\n* `ABS` [integer](integer.md#abs)\n"
    monkeypatch.setattr(docs, "_fetch_doc", lambda fn: index)
    categories, func_anchors = docs._build_categories_from_index()
    assert r"\bUPPER\b" in categories["string"]
    assert ("string.md", "upper") in func_anchors["UPPER"]
    assert ("integer.md", "abs") in func_anchors["ABS"]


def test_build_categories_from_index_empty_on_fetch_failure(monkeypatch):
    monkeypatch.setattr(docs, "_fetch_doc", lambda fn: "")
    categories, func_anchors = docs._build_categories_from_index()
    assert func_anchors == {}
    assert all(v == [] for v in categories.values())


# ---------------------------------------------------------------------------
# load_docs (end to end, fully mocked)
# ---------------------------------------------------------------------------


def test_load_docs_selects_relevant_category(monkeypatch):
    categories = {"string": [r"\bUPPER\b"]}
    func_anchors = {"UPPER": [("string.md", "upper")]}
    monkeypatch.setattr(
        docs, "_get_categories_and_anchors", lambda: (categories, func_anchors)
    )
    sections = {"## UPPER": "## UPPER\nupper docs\n"}
    monkeypatch.setattr(
        docs, "_get_doc_sections", lambda fn: ("", sections, {"upper": "## UPPER"})
    )
    out = docs.load_docs("SELECT UPPER(x) FROM t")
    assert "### string" in out
    assert "upper docs" in out


# ---------------------------------------------------------------------------
# Example loading: _load_example_file / _load_examples_from_dir / load_examples
# ---------------------------------------------------------------------------


def test_load_example_file_matches_category(tmp_path):
    f = tmp_path / "ex.md"
    f.write_text("---\ncategories: [string]\n---\nSTRING EXAMPLE")
    assert docs._load_example_file(f, {"string"}) == "STRING EXAMPLE"


def test_load_example_file_non_matching_returns_none(tmp_path):
    f = tmp_path / "ex.md"
    f.write_text("---\ncategories: [array]\n---\nARRAY EXAMPLE")
    assert docs._load_example_file(f, {"string"}) is None


def test_load_example_file_no_categories_always_matches(tmp_path):
    f = tmp_path / "ex.md"
    f.write_text("---\n---\nALWAYS EXAMPLE")
    assert docs._load_example_file(f, {"string"}) == "ALWAYS EXAMPLE"


def test_load_example_file_no_frontmatter_is_none(tmp_path):
    f = tmp_path / "ex.md"
    f.write_text("no frontmatter")
    assert docs._load_example_file(f, {"string"}) is None


def test_load_examples_combines_dir_and_files(tmp_path, monkeypatch):
    monkeypatch.setattr(docs, "_detect_categories", lambda sql: {"datetime"})
    d = tmp_path / "exdir"
    d.mkdir()
    (d / "a.md").write_text("---\ncategories: [datetime]\n---\nDIR EXAMPLE")
    (d / "skip.md").write_text("---\ncategories: [array]\n---\nSKIP")
    standalone = tmp_path / "b.md"
    standalone.write_text("---\ncategories: [datetime]\n---\nFILE EXAMPLE")
    out = docs.load_examples(
        "SELECT now()", extra_examples_dirs=[d], extra_examples_files=[standalone]
    )
    assert "DIR EXAMPLE" in out and "FILE EXAMPLE" in out
    assert "SKIP" not in out


def test_load_examples_from_dir_missing_dir(tmp_path):
    assert docs._load_examples_from_dir(tmp_path / "nope", {"string"}) == []
