from __future__ import annotations

import os
import re
import sys
import urllib.request
from pathlib import Path

import yaml

from felderize.constants import DEFAULT_DOCS_BASE_URL, HTTP_TIMEOUT

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
    "boolean": "boolean.md",
    "integer": "integer.md",
    "binary": "binary.md",
    "operators": "operators.md",
    "streaming": "streaming.md",
    "unsupported": "unsupported-operations.md",
    "uuid": "uuid.md",
}

# SQL construct patterns that cannot be derived from the function index
# (keywords, operators, syntax forms rather than named functions).
# Keep these specific — broad patterns like \bDATE\b match almost every query.
_EXTRA_PATTERNS: dict[str, list[str]] = {
    "datetime": [r"\bINTERVAL\b"],  # DATE/TIMESTAMP covered by index function names
    "aggregates": [
        r"\bGROUP\s+BY\b",
        r"\bHAVING\b",
        r"\bOVER\s*\(",
        r"\bFIRST_VALUE\b",
        r"\bLAST_VALUE\b",  # in doc but not function index
        r"\bVARIANCE\b",
        r"\bVAR_POP\b",
        r"\bVAR_SAMP\b",
        r"\bPERCENT_RANK\b",
        r"\bCUME_DIST\b",
        r"\bNTILE\b",
    ],
    "array": [r"\bEXPLODE\b", r"\bUNNEST\b", r"\bsize\s*\("],
    "map": [r"\bMAP\s*<"],  # MAP( covered by index; MAP< is type syntax
    "json": [r"\bVARIANT\b"],  # JSON covered by index function names
    "casts": [r"::"],  # CAST covered by index; :: is operator syntax
    "comparisons": [r"\bCASE\s+WHEN\b"],
    "boolean": [r"\bIS\s+(NOT\s+)?(?:TRUE|FALSE)\b", r"\bBOOLEAN\b", r"\bBOOL\b"],
    "decimal": [r"\bDECIMAL\b"],
    "integer": [
        r"\bTINYINT\b",
        r"\bSMALLINT\b",
        r"\bBIGINT\b",
        r"\bDIV_NULL\b",
        r"\bSEQUENCE\b",
    ],
    "binary": [r"\bVARBINARY\b", r"\bBINARY\b", r"\bXXHASH\b"],
    "float": [r"\bFINITE_OR_NULL\b"],
    "operators": [r"\bBETWEEN\b", r"<=>", r"\bCONTAINS\b", r"\bOVERLAPS\b"],
    "streaming": [r"\bLATENESS\b", r"\bWATERMARK\b", r"\bTUMBLE\b", r"\bHOP\b"],
    "uuid": [r"\bUUID\b"],
}

# Spark function names that appear in SQL but are not in the Feldera index.
_SPARK_ALIASES: dict[str, list[str]] = {
    "json": [r"\bget_json_object\b", r"\bfrom_json\b", r"\bjson_tuple\b"],
    "array": [
        r"\barray_contains\b",
        r"\bsort_array\b",
        r"\barray_distinct\b",
        r"\belement_at\b",
    ],
    "map": [r"\belement_at\b", r"\bstr_to_map\b", r"\bmap_from_arrays\b"],
    "datetime": [
        r"\bto_date\b",
        r"\bto_timestamp\b",
        r"\bunix_timestamp\b",
        r"\bfrom_unixtime\b",
        r"\bdate_format\b",
        r"\badd_months\b",
        r"\bmonths_between\b",
        r"\blast_day\b",
        r"\bdayofweek\b",
        r"\bdayofmonth\b",
        r"\bdayofyear\b",
        r"\bweekofyear\b",
    ],
    "string": [
        r"\bLPAD\b",
        r"\bRPAD\b",
        r"\bLTRIM\b",
        r"\bRTRIM\b",
        r"\bBTRIM\b",
        r"\bTRANSLATE\b",
        r"\bINSTR\b",
        r"\bLOCATE\b",
        r"\bSPACE\b",
    ],
    "decimal": [r"\bNUMERIC\b"],
    "float": [r"\bFLOAT\b", r"\bPOW\b", r"\bLOG2\b", r"\bHYPOT\b"],
    "comparisons": [r"\bNVL\b", r"\bNVL2\b", r"\bDECODE\b"],
}

# Regex to find HTML anchor IDs embedded in doc files: <a id="name">
_ANCHOR_ID_RE = re.compile(r'<a\s+id="([^"]+)"', re.IGNORECASE)

# Regex to detect function calls in SQL: FUNC_NAME(
_SQL_FUNC_RE = re.compile(r"\b([A-Z_][A-Z_0-9]*)\s*\(", re.IGNORECASE)

# Regexes for parsing function-index.md lines.
_INDEX_FUNC_RE = re.compile(r"^\* `([A-Z_][A-Z_0-9 ]*)`", re.IGNORECASE)
_INDEX_LINK_RE = re.compile(r"\[([a-z]+)\]\(([^)#]+)(?:#([^)]+))?\)")

# Categories that are always included — no trigger patterns needed.
_ALWAYS_INCLUDED = {"types", "unsupported"}


def _fetch_doc(filename: str) -> str:
    """Fetch a SQL doc file from the Feldera docs repo.

    The base URL is read from FELDERA_DOCS_BASE_URL (default: main branch on GitHub).
    """
    base = os.environ.get("FELDERA_DOCS_BASE_URL", DEFAULT_DOCS_BASE_URL)
    url = f"{base}/{filename}"
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"docs.py: failed to fetch {url}: {e}", file=sys.stderr)
        return ""


def _build_categories_from_index() -> tuple[
    dict[str, list[str]], dict[str, list[tuple[str, str]]]
]:
    """Fetch and parse function-index.md from the Feldera docs repo.

    Returns:
        categories:   category → [\\bFUNC\\b, ...] trigger patterns
        func_anchors: FUNC_NAME_UPPER → [(doc_filename, anchor_id), ...]
    """
    known = set(_DOC_FILES) - _ALWAYS_INCLUDED
    categories: dict[str, list[str]] = {cat: [] for cat in _DOC_FILES}
    func_anchors: dict[str, list[tuple[str, str]]] = {}

    text = _fetch_doc("function-index.md")
    if not text:
        return categories, func_anchors

    for line in text.splitlines():
        m = _INDEX_FUNC_RE.match(line)
        if not m:
            continue
        func_name = m.group(1).strip()
        func_upper = func_name.upper()
        for link_m in _INDEX_LINK_RE.finditer(line):
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


# Lazy cache — populated on first call to _get_categories_and_anchors().
_categories_cache: (
    tuple[dict[str, list[str]], dict[str, list[tuple[str, str]]]] | None
) = None


def _get_categories_and_anchors() -> tuple[
    dict[str, list[str]], dict[str, list[tuple[str, str]]]
]:
    """Return (categories, func_anchors), fetching function-index.md on first call."""
    global _categories_cache
    if _categories_cache is None:
        categories, func_anchors = _build_categories_from_index()
        for source in (_EXTRA_PATTERNS, _SPARK_ALIASES):
            for cat, patterns in source.items():
                seen = set(categories.get(cat, []))
                for p in patterns:
                    if p not in seen:
                        categories.setdefault(cat, []).append(p)
                        seen.add(p)
        _categories_cache = (categories, func_anchors)
    return _categories_cache


# Module-level categories for load_examples() — populated lazily on first use.
def _get_categories() -> dict[str, list[str]]:
    return _get_categories_and_anchors()[0]


# ── Section-level doc parsing ────────────────────────────────────────────────

# Cache: doc filename → (preamble, {heading: content}, {anchor_id: heading})
_section_cache: dict[str, tuple[str, dict[str, str], dict[str, str]]] = {}


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
    filename: str,
) -> tuple[str, dict[str, str], dict[str, str]]:
    """Return parsed sections for a doc file (fetched once, then cached)."""
    if filename not in _section_cache:
        text = _fetch_doc(filename)
        _section_cache[filename] = _parse_doc_sections(text) if text else ("", {}, {})
    return _section_cache[filename]


def _load_relevant_sections(filename: str, relevant_anchors: set[str]) -> str:
    """Return preamble + only the ## sections that contain a relevant anchor.

    Falls back to the full file content when no anchor information is available
    (e.g., the file has no <a id> tags) so that we never return empty docs for
    a matched category.
    """
    preamble, sections, anchor_map = _get_doc_sections(filename)

    if not sections:
        # Plain file with no ## headings — return as-is.
        return preamble

    needed: set[str] = set()
    for anchor in relevant_anchors:
        if anchor in anchor_map:
            needed.add(anchor_map[anchor])

    if not needed:
        # No specific functions detected or none matched → include everything.
        parts = [preamble] if preamble.strip() else []
        parts.append("".join(sections.values()))
        return "".join(parts)

    parts = [preamble] if preamble.strip() else []
    for heading, body in sections.items():
        if heading in needed:
            parts.append(body)
    return "".join(parts)


# ── Category detection ───────────────────────────────────────────────────────


def _detect_categories(
    sql: str, categories: dict[str, list[str]] | None = None
) -> set[str]:
    """Return set of category names whose trigger patterns match the SQL."""
    matched = _ALWAYS_INCLUDED.copy()
    all_categories = categories if categories is not None else _get_categories()
    for category, patterns in all_categories.items():
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


def load_docs(sql: str) -> str:
    """Load relevant Feldera doc sections based on SQL content.

    Fetches docs from the Feldera GitHub repo on first call (cached per session).
    Only sections whose <a id> anchors correspond to functions present in the SQL
    are included. Falls back to full file content when no specific anchors match.
    """
    categories, func_anchors = _get_categories_and_anchors()
    detected = _detect_categories(sql, categories)
    sql_funcs = _detect_sql_functions(sql)

    result_sections: list[str] = []

    for category in sorted(detected):
        if category not in _DOC_FILES:
            continue
        doc_filename = _DOC_FILES[category]

        relevant_anchors: set[str] = set()
        for func in sql_funcs:
            for fname, anchor in func_anchors.get(func, []):
                if fname == doc_filename:
                    relevant_anchors.add(anchor)

        content = _load_relevant_sections(doc_filename, relevant_anchors)
        if content.strip():
            result_sections.append(f"### {category}\n\n{content}")

    return "\n\n---\n\n".join(result_sections)


_example_cache: dict[Path, tuple[set[str], str] | None] = {}


def _load_example_file(filepath: Path, detected: set[str]) -> str | None:
    """Return the body of one example file if it matches detected categories, else None.

    Returns None for files that are not valid examples (no frontmatter, etc.).
    """
    if filepath not in _example_cache:
        raw = filepath.read_text()
        if not raw.startswith("---"):
            _example_cache[filepath] = None
            return None
        parts = raw.split("---", 2)
        if len(parts) < 3:
            _example_cache[filepath] = None
            return None
        file_categories: set[str] = set()
        try:
            meta = yaml.safe_load(parts[1])
            if isinstance(meta, dict):
                file_categories = set(meta.get("categories", []))
        except yaml.YAMLError:
            pass
        _example_cache[filepath] = (file_categories, parts[2].strip())
    cached = _example_cache[filepath]
    if cached is None:
        return None
    file_categories, body = cached
    return body if (not file_categories or file_categories & detected) else None


def _load_examples_from_dir(examples_dir: Path, detected: set[str]) -> list[str]:
    """Load matching example sections from a directory."""
    if not examples_dir.is_dir():
        return []
    sections: list[str] = []
    for filepath in sorted(examples_dir.glob("*.md")):
        body = _load_example_file(filepath, detected)
        if body is not None:
            sections.append(body)
    return sections


def load_examples(
    sql: str,
    extra_examples_dirs: list[Path] | None = None,
    extra_examples_files: list[Path] | None = None,
) -> str:
    """Return validated translation examples relevant to the SQL input.

    Loads from user-provided directories and/or individual files.
    Directories are auto-discovered from ~/.felderize/examples/ and .felderize/examples/;
    individual files and extra directories are passed explicitly.
    """
    detected = _detect_categories(sql)
    sections: list[str] = []
    for d in extra_examples_dirs or []:
        sections += _load_examples_from_dir(d, detected)
    for f in extra_examples_files or []:
        body = _load_example_file(f, detected)
        if body is not None:
            sections.append(body)
    return "\n\n---\n\n".join(sections)
