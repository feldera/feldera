from __future__ import annotations

import re
from pathlib import Path

import yaml

# Map each category to its doc file.
_DOC_FILES: dict[str, str] = {
    "types": "types.md",
    "string": "string.md",
    "datetime": "datetime.md",
    "json": "json.md",
    "aggregates": "aggregates.md",
    "array": "array.md",
    "map": "map.md",
    "decimal": "decimal.md",
    "float": "float.md",
    "casts": "casts.md",
    "comparisons": "comparisons.md",
}

# SQL construct patterns that cannot be derived from the function index
# (keywords, operators, syntax forms rather than named functions).
_EXTRA_PATTERNS: dict[str, list[str]] = {
    "types":       [],                              # always matched — no keywords needed
    "datetime":    [r"\bDATE\b", r"\bTIMESTAMP\b", r"\bINTERVAL\b"],
    "aggregates":  [r"\bGROUP\s+BY\b", r"\bHAVING\b", r"\bOVER\s*\("],
    "array":       [r"\bARRAY\b", r"\bEXPLODE\b", r"\bUNNEST\b", r"\bsize\s*\("],
    "map":         [r"\bMAP\s*<", r"\bMAP\s*\("],
    "json":        [r"\bJSON\b", r"\bVARIANT\b"],
    "casts":       [r"\bCAST\s*\(", r"::"],
    "comparisons": [r"\bCASE\s+WHEN\b"],
}

# Spark function names that appear in SQL but are not in the Feldera index.
_SPARK_ALIASES: dict[str, list[str]] = {
    "json":    [r"\bget_json_object\b", r"\bfrom_json\b", r"\bjson_tuple\b"],
    "array":   [r"\barray_contains\b", r"\bsort_array\b", r"\barray_distinct\b"],
    "decimal": [r"\bNUMERIC\b"],
    "float":   [r"\bFLOAT\b"],
}


def _build_categories_from_index(index_path: Path) -> dict[str, list[str]]:
    """Parse function-index.md and return category → \\bFUNC\\b pattern lists.

    Only populates categories that appear in _DOC_FILES.  Falls back to an
    empty dict if the index file is not found.
    """
    known = set(_DOC_FILES) - {"types"}
    cats: dict[str, list[str]] = {cat: [] for cat in _DOC_FILES}

    if not index_path.is_file():
        return cats

    func_re = re.compile(r"^\* `([A-Z_][A-Z_0-9 ]*)`", re.IGNORECASE)
    link_re = re.compile(r"\[([a-z]+)\]\([^)]+\)")

    for line in index_path.read_text().splitlines():
        m = func_re.match(line)
        if not m:
            continue
        func_name = m.group(1).strip()
        for link_m in link_re.finditer(line):
            cat = link_m.group(1)
            if cat in known:
                keyword = rf"\b{re.escape(func_name)}\b"
                if keyword not in cats[cat]:
                    cats[cat].append(keyword)

    return cats


def _make_categories() -> dict[str, list[str]]:
    index_path = (
        Path(__file__).resolve().parents[3]
        / "docs.feldera.com" / "docs" / "sql" / "function-index.md"
    )
    cats = _build_categories_from_index(index_path)
    for source in (_EXTRA_PATTERNS, _SPARK_ALIASES):
        for cat, patterns in source.items():
            seen = set(cats.get(cat, []))
            for p in patterns:
                if p not in seen:
                    cats.setdefault(cat, []).append(p)
                    seen.add(p)
    return cats


# Categories used for selecting relevant docs and examples.
# Built automatically from the Feldera function index, supplemented by
# _EXTRA_PATTERNS (SQL construct keywords) and _SPARK_ALIASES (Spark names
# not in the Feldera index).
_CATEGORIES: dict[str, list[str]] = _make_categories()

_doc_cache: dict[Path, str] = {}


def _detect_categories(sql: str) -> set[str]:
    """Return set of category names whose trigger patterns match the SQL."""
    matched = {"types"}  # Always include types
    for category, patterns in _CATEGORIES.items():
        if not patterns:
            continue
        for pattern in patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                matched.add(category)
                break
    return matched


def load_docs(sql: str, docs_dir: Path | None = None) -> str:
    """Load relevant Feldera doc files based on SQL content."""
    if docs_dir is None:
        # Use the canonical docs from the repo root (docs.feldera.com/docs/sql/).
        docs_dir = Path(__file__).resolve().parents[3] / "docs.feldera.com" / "docs" / "sql"

    if not docs_dir.is_dir():
        return ""

    categories = _detect_categories(sql)
    sections: list[str] = []

    for category in sorted(categories):
        if category not in _DOC_FILES:
            continue
        filepath = docs_dir / _DOC_FILES[category]

        if filepath not in _doc_cache:
            if filepath.is_file():
                _doc_cache[filepath] = filepath.read_text()
            else:
                _doc_cache[filepath] = ""

        content = _doc_cache[filepath]
        if content:
            sections.append(f"### {category}\n\n{content}")

    return "\n\n---\n\n".join(sections)


_example_cache: dict[Path, tuple[set[str], str]] = {}


def load_examples(sql: str, examples_dir: Path | None = None) -> str:
    """Return validated translation examples relevant to the SQL input."""
    if examples_dir is None:
        examples_dir = Path(__file__).resolve().parent / "data" / "samples"

    if not examples_dir.is_dir():
        return ""

    categories = _detect_categories(sql)
    sections: list[str] = []

    for filepath in sorted(examples_dir.glob("*.md")):
        if filepath not in _example_cache:
            raw = filepath.read_text()
            cats: set[str] = set()
            body = raw
            if raw.startswith("---"):
                parts = raw.split("---", 2)
                if len(parts) >= 3:
                    try:
                        meta = yaml.safe_load(parts[1])
                        if isinstance(meta, dict):
                            cats = set(meta.get("categories", []))
                    except yaml.YAMLError:
                        pass
                    body = parts[2].strip()
            _example_cache[filepath] = (cats, body)

        cats, body = _example_cache[filepath]
        if cats & categories:
            sections.append(body)

    return "\n\n---\n\n".join(sections)
