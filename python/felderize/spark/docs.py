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
# Keep these specific — broad patterns like \bDATE\b match almost every query.
_EXTRA_PATTERNS: dict[str, list[str]] = {
    "datetime": [r"\bINTERVAL\b"],  # DATE/TIMESTAMP covered by index function names
    "aggregates": [r"\bGROUP\s+BY\b", r"\bHAVING\b", r"\bOVER\s*\("],
    "array": [r"\bEXPLODE\b", r"\bUNNEST\b", r"\bsize\s*\("],
    "map": [r"\bMAP\s*<"],  # MAP( covered by index; MAP< is type syntax
    "json": [r"\bVARIANT\b"],  # JSON covered by index function names
    "casts": [r"::"],  # CAST covered by index; :: is operator syntax
    "comparisons": [r"\bCASE\s+WHEN\b"],
}

# Spark function names that appear in SQL but are not in the Feldera index.
_SPARK_ALIASES: dict[str, list[str]] = {
    "json": [r"\bget_json_object\b", r"\bfrom_json\b", r"\bjson_tuple\b"],
    "array": [r"\barray_contains\b", r"\bsort_array\b", r"\barray_distinct\b"],
    "decimal": [r"\bNUMERIC\b"],
    "float": [r"\bFLOAT\b"],
}

# Regex to find HTML anchor IDs embedded in doc files: <a id="name">
_ANCHOR_ID_RE = re.compile(r'<a\s+id="([^"]+)"', re.IGNORECASE)

# Regex to detect function calls in SQL: FUNC_NAME(
_SQL_FUNC_RE = re.compile(r"\b([A-Z_][A-Z_0-9]*)\s*\(", re.IGNORECASE)


def _build_categories_from_index(
    index_path: Path,
) -> tuple[dict[str, list[str]], dict[str, list[tuple[str, str]]]]:
    """Parse function-index.md.

    Returns:
        categories:   category → [\\bFUNC\\b, ...] trigger patterns
        func_anchors: FUNC_NAME_UPPER → [(doc_filename, anchor_id), ...]
    """
    known = set(_DOC_FILES) - {"types"}
    categories: dict[str, list[str]] = {cat: [] for cat in _DOC_FILES}
    func_anchors: dict[str, list[tuple[str, str]]] = {}

    if not index_path.is_file():
        return categories, func_anchors

    func_re = re.compile(r"^\* `([A-Z_][A-Z_0-9 ]*)`", re.IGNORECASE)
    link_re = re.compile(r"\[([a-z]+)\]\(([^)#]+)(?:#([^)]+))?\)")

    for line in index_path.read_text().splitlines():
        m = func_re.match(line)
        if not m:
            continue
        func_name = m.group(1).strip()
        func_upper = func_name.upper()
        for link_m in link_re.finditer(line):
            cat = link_m.group(1)
            doc_file = link_m.group(2)  # e.g. "string.md"
            anchor = link_m.group(3)  # e.g. "upper" (may be None)
            if cat in known:
                keyword = rf"\b{re.escape(func_name)}\b"
                if keyword not in categories[cat]:
                    categories[cat].append(keyword)
            if anchor:
                func_anchors.setdefault(func_upper, []).append((doc_file, anchor))

    return categories, func_anchors


_DEFAULT_DOCS_DIR = (
    Path(__file__).resolve().parents[3] / "docs.feldera.com" / "docs" / "sql"
)

# Cache: docs_dir → (categories, func_anchors)
_categories_cache: dict[
    Path, tuple[dict[str, list[str]], dict[str, list[tuple[str, str]]]]
] = {}


def _get_categories_and_anchors(
    docs_dir: Path,
) -> tuple[dict[str, list[str]], dict[str, list[tuple[str, str]]]]:
    """Return (categories, func_anchors) for the given docs_dir, cached per path."""
    if docs_dir not in _categories_cache:
        categories, func_anchors = _build_categories_from_index(
            docs_dir / "function-index.md"
        )
        for source in (_EXTRA_PATTERNS, _SPARK_ALIASES):
            for cat, patterns in source.items():
                seen = set(categories.get(cat, []))
                for p in patterns:
                    if p not in seen:
                        categories.setdefault(cat, []).append(p)
                        seen.add(p)
        _categories_cache[docs_dir] = (categories, func_anchors)
    return _categories_cache[docs_dir]


# Module-level categories for load_examples() (which has no docs_dir).
# Built from the default docs location; func_anchors not needed for examples.
_CATEGORIES, _ = _get_categories_and_anchors(_DEFAULT_DOCS_DIR)

# ── Section-level doc parsing ────────────────────────────────────────────────

# Cache: filepath → (preamble, {heading: content}, {anchor_id: heading})
_section_cache: dict[Path, tuple[str, dict[str, str], dict[str, str]]] = {}


def _parse_doc_sections(
    content: str,
) -> tuple[str, dict[str, str], dict[str, str]]:
    """Split a doc file into (preamble, sections, anchor_map).

    preamble    — text before the first ## heading
    sections    — ordered dict: ## heading text → section content (includes heading line)
    anchor_map  — <a id="x"> → ## heading text for every anchor in the file
    """
    sections: dict[str, str] = {}
    anchor_map: dict[str, str] = {}
    preamble_lines: list[str] = []
    current_heading: str | None = None
    current_lines: list[str] = []

    for line in content.splitlines(keepends=True):
        if line.startswith("## "):
            if current_heading is not None:
                body = "".join(current_lines)
                sections[current_heading] = body
                for am in _ANCHOR_ID_RE.finditer(body):
                    anchor_map[am.group(1)] = current_heading
            else:
                preamble_lines = current_lines[:]
            current_heading = line.rstrip()
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_heading is not None:
        body = "".join(current_lines)
        sections[current_heading] = body
        for am in _ANCHOR_ID_RE.finditer(body):
            anchor_map[am.group(1)] = current_heading
    elif current_lines:
        preamble_lines = current_lines

    return "".join(preamble_lines), sections, anchor_map


def _get_doc_sections(
    doc_path: Path,
) -> tuple[str, dict[str, str], dict[str, str]]:
    """Return parsed sections for a doc file (cached)."""
    if doc_path not in _section_cache:
        if doc_path.is_file():
            _section_cache[doc_path] = _parse_doc_sections(doc_path.read_text())
        else:
            _section_cache[doc_path] = ("", {}, {})
    return _section_cache[doc_path]


def _load_relevant_sections(doc_path: Path, relevant_anchors: set[str]) -> str:
    """Return preamble + only the ## sections that contain a relevant anchor.

    Falls back to the full file content when no anchor information is available
    (e.g., the file has no <a id> tags) so that we never return empty docs for
    a matched category.
    """
    preamble, sections, anchor_map = _get_doc_sections(doc_path)

    if not sections:
        # Plain file with no ## headings — return as-is.
        return preamble

    # Determine which headings are needed.
    needed: set[str] = set()
    for anchor in relevant_anchors:
        if anchor in anchor_map:
            needed.add(anchor_map[anchor])

    if not needed:
        # No specific functions detected or none matched → include everything.
        return preamble + "".join(sections.values())

    parts = [preamble] if preamble.strip() else []
    for heading, body in sections.items():
        if heading in needed:
            parts.append(body)
    return "".join(parts)


# ── Category detection ───────────────────────────────────────────────────────


def _detect_categories(
    sql: str,
    categories: dict[str, list[str]] | None = None,
) -> set[str]:
    """Return set of category names whose trigger patterns match the SQL."""
    matched = {"types"}  # Always include types
    for category, patterns in (categories if categories is not None else _CATEGORIES).items():
        if not patterns:
            continue
        for pattern in patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                matched.add(category)
                break
    return matched


def _detect_sql_functions(sql: str) -> set[str]:
    """Return uppercase names of all function calls found in the SQL."""
    return {m.group(1).upper() for m in _SQL_FUNC_RE.finditer(sql)}


# ── Public API ───────────────────────────────────────────────────────────────


def load_docs(sql: str, docs_dir: Path | None = None) -> str:
    """Load relevant Feldera doc sections based on SQL content.

    Only sections whose <a id> anchors correspond to functions actually present
    in the SQL are included.  Falls back to full file content for categories
    matched by keyword patterns (e.g., GROUP BY) with no specific function match.
    """
    if docs_dir is None:
        docs_dir = _DEFAULT_DOCS_DIR

    if not docs_dir.is_dir():
        return ""

    categories, func_anchors = _get_categories_and_anchors(docs_dir)
    detected = _detect_categories(sql, categories)
    sql_funcs = _detect_sql_functions(sql)

    result_sections: list[str] = []

    for category in sorted(detected):
        if category not in _DOC_FILES:
            continue
        doc_filename = _DOC_FILES[category]
        doc_path = docs_dir / doc_filename

        # Collect anchors for functions in this doc file that appear in the SQL.
        relevant_anchors: set[str] = set()
        for func in sql_funcs:
            for fname, anchor in func_anchors.get(func, []):
                if fname == doc_filename:
                    relevant_anchors.add(anchor)

        content = _load_relevant_sections(doc_path, relevant_anchors)
        if content.strip():
            result_sections.append(f"### {category}\n\n{content}")

    return "\n\n---\n\n".join(result_sections)


_example_cache: dict[Path, tuple[set[str], str]] = {}


def load_examples(sql: str, examples_dir: Path | None = None) -> str:
    """Return validated translation examples relevant to the SQL input."""
    if examples_dir is None:
        examples_dir = Path(__file__).resolve().parent / "data" / "samples"

    if not examples_dir.is_dir():
        return ""

    detected = _detect_categories(sql)
    sections: list[str] = []

    for filepath in sorted(examples_dir.glob("*.md")):
        if filepath not in _example_cache:
            raw = filepath.read_text()
            file_categories: set[str] = set()
            body = raw
            if raw.startswith("---"):
                parts = raw.split("---", 2)
                if len(parts) >= 3:
                    try:
                        meta = yaml.safe_load(parts[1])
                        if isinstance(meta, dict):
                            file_categories = set(meta.get("categories", []))
                    except yaml.YAMLError:
                        pass
                    body = parts[2].strip()
            _example_cache[filepath] = (file_categories, body)

        file_categories, body = _example_cache[filepath]
        if file_categories & detected:
            sections.append(body)

    return "\n\n---\n\n".join(sections)
