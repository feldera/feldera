from __future__ import annotations

import re
from pathlib import Path

import yaml

# Categories used for selecting relevant examples and docs.
_CATEGORIES: dict[str, list[str]] = {
    "types": [],  # Always matched
    "string": [
        r"\bUPPER\b",
        r"\bLOWER\b",
        r"\bTRIM\b",
        r"\bCONCAT\b",
        r"\bSUBSTRING\b",
        r"\bREPLACE\b",
        r"\bLIKE\b",
        r"\bREGEXP\b",
        r"\bLENGTH\b",
        r"\bINITCAP\b",
        r"\bREVERSE\b",
        r"\bREPEAT\b",
        r"\bSPLIT\b",
        r"\bLPAD\b",
        r"\bRPAD\b",
    ],
    "datetime": [
        r"\bDATE\b",
        r"\bTIMESTAMP\b",
        r"\bINTERVAL\b",
        r"\bYEAR\b",
        r"\bMONTH\b",
        r"\bDAY\b",
        r"\bHOUR\b",
        r"\bEXTRACT\b",
        r"\bDATE_ADD\b",
        r"\bDATE_SUB\b",
        r"\bDATEDIFF\b",
        r"\bDATE_TRUNC\b",
        r"\bCURRENT_DATE\b",
        r"\bCURRENT_TIMESTAMP\b",
    ],
    "json": [
        r"\bJSON\b",
        r"\bPARSE_JSON\b",
        r"\bVARIANT\b",
        r"\bget_json_object\b",
        r"\bfrom_json\b",
        r"\bjson_tuple\b",
        r"\bTO_JSON\b",
    ],
    "aggregates": [
        r"\bCOUNT\b",
        r"\bSUM\b",
        r"\bAVG\b",
        r"\bGROUP\s+BY\b",
        r"\bHAVING\b",
        r"\bOVER\s*\(",
        r"\bROW_NUMBER\b",
        r"\bRANK\b",
        r"\bLAG\b",
        r"\bLEAD\b",
        r"\bWINDOW\b",
    ],
    "array": [
        r"\bARRAY\b",
        r"\bEXPLODE\b",
        r"\bUNNEST\b",
        r"\barray_contains\b",
        r"\bsort_array\b",
        r"\barray_distinct\b",
        r"\bCARDINALITY\b",
        r"\bsize\s*\(",
    ],
    "map": [r"\bMAP\s*<", r"\bMAP\s*\(", r"\bmap_keys\b", r"\bmap_values\b"],
    "decimal": [
        r"\bDECIMAL\b",
        r"\bNUMERIC\b",
        r"\bROUND\b",
        r"\bCEIL\b",
        r"\bFLOOR\b",
        r"\bTRUNCATE\b",
    ],
    "float": [
        r"\bFLOAT\b",
        r"\bDOUBLE\b",
        r"\bPOWER\b",
        r"\bSQRT\b",
        r"\bLOG\b",
        r"\bLN\b",
        r"\bSIN\b",
        r"\bCOS\b",
    ],
    "casts": [r"\bCAST\s*\(", r"::"],
    "comparisons": [r"\bBETWEEN\b", r"\bCASE\s+WHEN\b", r"\bCOALESCE\b", r"\bNULLIF\b"],
}

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

_doc_cache: dict[str, str] = {}


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
    """Load relevant Feldera doc files based on SQL content.

    Only loads docs not already covered by skills (currently just types.md).
    """
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
