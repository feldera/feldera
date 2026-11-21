"""Update documentation references in a patched sqlparser 0.64 source tree."""

import argparse
import re
from pathlib import Path

ISO_2016 = "https://www.iso.org/standard/63556.html"
GRAMMAR = r"https://[^/\s]+/sql-overview/sql-(\d+)-foundation-grammar\.html"


def reference(match):
    year, production = match.groups()
    rule = production.strip("_").replace("-", " ").replace("_", " ")
    return f"ISO/IEC 9075-2:{year}, `{rule}`"


def normalize(root):
    readme = root / "README.md"
    original = readme.read_text()
    text = re.sub(
        r"(a traditional, hand-written recursive descent parser\.) [^\n]+\n"
        r"\[tutorial on TDOP parsers\].*?\n\n",
        r"\1\n\n",
        original,
        flags=re.DOTALL,
    )
    text = re.sub(r"^\[(?:tdop-tutorial|LocustDB)\]: .*\n", "", text, flags=re.M)
    text = re.sub(GRAMMAR, ISO_2016, text.replace("[LocustDB],", "LocustDB,"))
    if text != original:
        readme.write_text(text)
    for path in (root / "src").rglob("*.rs"):
        text = path.read_text()
        updated = re.sub("<" + GRAMMAR + r"#([\w-]+)>", reference, text)
        updated = re.sub(
            GRAMMAR + r"#([\w-]+)",
            lambda m: f'{ISO_2016} "{m[2].replace("-", " ")}"',
            updated,
        )
        if updated != text:
            if path.relative_to(root).as_posix() == "src/ast/operator.rs":
                updated = "// Feldera changed the SQL standard citation.\n" + updated
            path.write_text(updated)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="patched sqlparser source directory")
    args = parser.parse_args()
    if not (args.source / "src/ast/operator.rs").is_file():
        parser.error(
            "Cannot find the parser sources. Supply the patched crate directory."
        )
    normalize(args.source)
