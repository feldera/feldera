#!/usr/bin/env python3
"""Find keywords that the SQL compiler reserves and Calcite does not.

The SQL compiler reserves every keyword that does not appear in one of the 2
lists below in config.fmpp.  Both lists are copies of lists in Calcite:

    nonReservedKeywords       copied from core/src/main/codegen/default_config.fmpp
    nonReservedKeywordsToAdd  copied from babel/src/main/codegen/config.fmpp

A reserved keyword cannot be used as the name of a table, view, or column.
Calcite adds words to its lists over time.  A word that is missing from the
copies in config.fmpp becomes incorrectly reserved.
This script detects such words and fails the build.
"""

import re
import sys
from pathlib import Path
from typing import NoReturn

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "SQL-compiler/src/main/codegen/config.fmpp"

WORD = re.compile(r'"([A-Z_0-9-]+)"')

# The comment that precedes the keyword declarations in Calcite's template
KEYWORD_MARKER = "KEYWORDS:  anything in this list"

# Four token names differ from the SQL text.  The lists use the text.
TOKEN_TEXT = {
    "DEFAULT_": "DEFAULT",
    "SKIP_": "SKIP",
    "SET_MINUS": "MINUS",
    "END_EXEC": "END-EXEC",
}


def fail(message: str) -> NoReturn:
    """Report a file that does not have the expected shape, and give up."""
    print(f"keyword check: {message}", file=sys.stderr)
    print(
        "The file format probably changed; this script needs an update.",
        file=sys.stderr,
    )
    sys.exit(1)


def words_in_list(path: Path, list_name: str, include_commented: bool) -> set[str]:
    """Return the words in the list called list_name in a configuration file.

    Includes commented-out words only if include_commented is true.
    """
    text = path.read_text()
    start = text.find(list_name + ":")
    if start < 0:
        fail(f"{path} has no list called {list_name}")

    depth, end = 0, -1
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        fail(f"the list {list_name} in {path} has no closing bracket")

    words = set()
    for line in text[start:end].split("\n")[1:]:
        if include_commented or not line.lstrip().startswith("#"):
            words.update(WORD.findall(line))
    if not words:
        fail(f"the list {list_name} in {path} has no words")
    return words


def declared_keywords(template: Path) -> set[str]:
    """Return every keyword that Calcite's parser template declares."""
    text = template.read_text()
    start = text.find(KEYWORD_MARKER)
    if start < 0:
        fail(f"{template} has no comment reading '{KEYWORD_MARKER}'")
    end = text.find("\n}", start)
    if end < 0:
        fail(f"the keyword declarations in {template} have no closing brace")

    names = re.findall(r"<\s*([A-Z_0-9]+)\s*:", text[start:end])
    if not names:
        fail(f"{template} declares no keywords")
    return {TOKEN_TEXT.get(name, name) for name in names}


def main() -> int:
    program = Path(sys.argv[0]).name
    if len(sys.argv) == 2 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return 0
    if len(sys.argv) != 2:
        print(
            f"usage: {program} [-h][--help] CALCITE_SOURCE_DIRECTORY", file=sys.stderr
        )
        return 1

    calcite = Path(sys.argv[1])
    template = calcite / "core/src/main/codegen/templates/Parser.jj"
    # Each list has the same name as the Calcite list it was copied from
    sources = {
        "nonReservedKeywords": calcite / "core/src/main/codegen/default_config.fmpp",
        "nonReservedKeywordsToAdd": calcite / "babel/src/main/codegen/config.fmpp",
    }

    for path in [CONFIG, template, *sources.values()]:
        if not path.is_file():
            print(f"keyword check: skipped, {path} is missing")
            return 0

    # Only keywords can be reserved.  A word in either list is free.
    keywords = declared_keywords(template) | words_in_list(CONFIG, "keywords", False)
    appears = set()
    for list_name in sources:
        appears |= words_in_list(CONFIG, list_name, True)

    forgotten = {}
    for list_name, source in sources.items():
        missing = words_in_list(source, list_name, False) & keywords - appears
        if missing:
            forgotten[list_name] = sorted(missing)

    if not forgotten:
        print("keyword check: the lists in config.fmpp match Calcite's")
        return 0

    for list_name, words in forgotten.items():
        print(f"{CONFIG}:")
        print(
            f"    the list '{list_name}' is missing {len(words)} word(s) that "
            f"{sources[list_name]} does not reserve:"
        )
        print("    " + " ".join(words))
    print("The SQL compiler reserves each word above.  Add it to the list to allow it")
    print("as a name.  Comment it out to reserve it on purpose.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
