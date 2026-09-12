#!/usr/bin/env python3
"""Move the changelog's `## Unreleased` entries under a `## vX.Y.Z` heading.

A section is a `## ` heading and the lines under it, up to the next heading: the
changelog holds `## Unreleased` and one section per released version.  An entry is
one `- ` bullet in a section, together with the lines that follow it until the next
bullet or heading: one item as its author wrote it.

People write the entries by hand under `## Unreleased`, and this script
moves them under a heading for the released version.

The release workflow builds the release from one commit, and more pull requests
merge into main before the post-release job runs.  Only add the entries that
are in the released commit.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

UNRELEASED = "Unreleased"

# A `## ` section heading.  `indent` is empty in the changelog on main and
# eight spaces in the changelog at an older release tag.
HEADING = re.compile(r"^(?P<indent>[ \t]*)## (?P<title>\S.*?)[ \t]*$")

# A line that opens or closes a fenced code block.  An entry may hold one, and
# its content is example text rather than changelog structure.
FENCE = re.compile(r"^[ \t]*(```|~~~)")


class ChangelogError(Exception):
    """A changelog cannot be read, or cannot be filed as this script expects."""


class Section(NamedTuple):
    """A `## ` heading and the lines under it, up to the next heading."""

    indent: str  # the heading's own indentation, which its entries share
    preamble: list[str]  # the lines between the heading and the first entry
    entries: list[list[str]]  # the source lines of each entry, in file order


class Filing(NamedTuple):
    """What filing one release produced."""

    text: str  # the changelog to write
    filed: int  # entries now under the released version
    pending: int  # entries still under `## Unreleased`


class Changelog(NamedTuple):
    """A changelog's text, and the name that error messages give it."""

    name: str
    text: str


def headings(lines: list[str]) -> Iterator[tuple[int, re.Match]]:
    """Each `## ` heading in `lines`, skipping any inside a fenced code block."""
    fenced = False
    for index, line in enumerate(lines):
        if FENCE.match(line):
            fenced = not fenced
        elif not fenced:
            match = HEADING.match(line)
            if match:
                yield index, match


def heading_index(lines: list[str], title: str) -> int | None:
    """The position in `lines` of the first `## title` heading, or None when there is none."""
    for index, match in headings(lines):
        if match.group("title") == title:
            return index
    return None


def section_end(lines: list[str], start: int) -> int:
    """The position in `lines` just past the section that the heading at `start` opens."""
    for index, _ in headings(lines):
        if index > start:
            return index
    return len(lines)


def trim_blanks(block: list[str]) -> list[str]:
    """A copy of the block without its leading and trailing blank lines."""
    trimmed = list(block)
    while trimmed and not trimmed[0].strip():
        trimmed.pop(0)
    while trimmed and not trimmed[-1].strip():
        trimmed.pop()
    return trimmed


def section_at(lines: list[str], start: int) -> Section:
    """The section that the `## ` heading at position `start` in `lines` opens."""
    indent = HEADING.match(lines[start]).group("indent")
    bullet = re.compile(rf"^{re.escape(indent)}- \S")
    preamble: list[str] = []
    entries: list[list[str]] = []
    current: list[str] | None = None
    fenced = False
    for line in lines[start + 1 : section_end(lines, start)]:
        if FENCE.match(line):
            fenced = not fenced
        if bullet.match(line) and not fenced:
            if current is not None:
                entries.append(current)
            current = [line]
        elif current is None:
            preamble.append(line)
        else:
            current.append(line)
    if current is not None:
        entries.append(current)
    return Section(
        indent, trim_blanks(preamble), [trim_blanks(entry) for entry in entries]
    )


def unreleased_section(changelog: Changelog) -> Section:
    """The changelog's `## Unreleased` section."""
    lines = changelog.text.splitlines()
    start = heading_index(lines, UNRELEASED)
    if start is None:
        raise ChangelogError(f"{changelog.name} has no `## {UNRELEASED}` heading")
    return section_at(lines, start)


def canonical_form(lines: list[str]) -> str:
    """The lines joined into one, with each run of whitespace collapsed to a space."""
    return " ".join(" ".join(lines).split())


def abbreviated(entry: list[str], limit: int = 50) -> str:
    """The start of the entry's canonical form, short enough for an error message."""
    canonical = canonical_form(entry)
    return canonical if len(canonical) <= limit else canonical[:limit] + "..."


def entry_difference(found: list[list[str]], wanted: list[list[str]]) -> str | None:
    """How `found` differs from `wanted`, or None when both hold the same entries.

    Two entries are the same when their canonical forms are equal.
    """
    for position, (left, right) in enumerate(zip(found, wanted), start=1):
        if canonical_form(left) != canonical_form(right):
            return f"entry {position} reads {abbreviated(left)!r}, not {abbreviated(right)!r}"
    if len(found) > len(wanted):
        return f"it holds an extra entry {abbreviated(found[len(wanted)])!r}"
    if len(found) < len(wanted):
        return f"the entry {abbreviated(wanted[len(found)])!r} is missing"
    return None


def git(*arguments: str) -> subprocess.CompletedProcess:
    """Run a git command and capture its output."""
    return subprocess.run(["git", *arguments], capture_output=True, text=True)


def changelog_at_tag(tag: str, path: Path) -> str:
    """The changelog's content at the commit `tag` names."""
    shown = git("show", f"{tag}:{path.as_posix()}")
    if shown.returncode != 0:
        raise ChangelogError(f"cannot read {path} at {tag}: {shown.stderr.strip()}")
    return shown.stdout


def check_filing(
    proposed: Changelog, version: str, filed: list[list[str]], pending: list[list[str]]
) -> None:
    """Raise unless `## version` holds `filed` and `## Unreleased` holds `pending`.

    The filing checks the text it is about to write, so raising here leaves the
    changelog on disk unchanged.
    """
    lines = proposed.text.splitlines()
    start = heading_index(lines, version)
    if start is None:
        raise ChangelogError(
            f"filing `## {version}` in {proposed.name} produced no such heading; "
            f"the changelog is unchanged"
        )
    difference = entry_difference(section_at(lines, start).entries, filed)
    if difference:
        raise ChangelogError(
            f"filing `## {version}` in {proposed.name} would put the wrong entries "
            f"under it: {difference}; the changelog is unchanged"
        )
    difference = entry_difference(unreleased_section(proposed).entries, pending)
    if difference:
        raise ChangelogError(
            f"filing `## {version}` in {proposed.name} would leave the wrong entries "
            f"under `## {UNRELEASED}`: {difference}; the changelog is unchanged"
        )


def file_release(current: Changelog, at_tag: Changelog, version: str) -> Filing:
    """Move the entries `at_tag` holds under `## version`, leaving the rest unreleased.

    `current` is the changelog on main and `at_tag` the same file at the release tag.
    """
    lines = current.text.splitlines()
    if heading_index(lines, version) is not None:
        return Filing(current.text, 0, 0)

    unreleased = unreleased_section(current)
    entries = unreleased.entries
    shipped_entries = unreleased_section(at_tag).entries
    if not shipped_entries:
        return Filing(current.text, 0, len(entries))

    # Entries compare by their canonical forms, so rewrapping or reindenting an
    # entry after the release still files it under the released version.
    released = {canonical_form(entry) for entry in shipped_entries}
    filed = [entry for entry in entries if canonical_form(entry) in released]
    pending = [entry for entry in entries if canonical_form(entry) not in released]
    # Every entry of the release is gone from the Unreleased section: the
    # section was rewritten or filed by hand, and guessing a split would either
    # drop entries or file them under the wrong version.
    if not filed:
        raise ChangelogError(
            f"none of the {len(shipped_entries)} entries in {at_tag.name} appear "
            f"under `## {UNRELEASED}` in {current.name}"
        )

    start = heading_index(lines, UNRELEASED)
    rebuilt = [lines[start], ""]
    if unreleased.preamble:
        rebuilt += unreleased.preamble + [""]
    for entry in pending:
        rebuilt += entry + [""]
    rebuilt += [f"{unreleased.indent}## {version}", ""]
    for entry in filed:
        rebuilt += entry + [""]

    lines[start : section_end(lines, start)] = rebuilt
    text = "\n".join(lines)
    if current.text.endswith("\n"):
        text += "\n"
    check_filing(Changelog(current.name, text), version, filed, pending)
    return Filing(text, len(filed), len(pending))


def main(argv: list[str] | None = None) -> int:
    """Run the filing from the command line, returning the process exit status."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--changelog",
        required=True,
        type=Path,
        help="the changelog to rewrite in place",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify that the changelog has an `## Unreleased` section, and change nothing",
    )
    parser.add_argument(
        "--tag", help="tag of the published release, for example v0.348.0"
    )
    args = parser.parse_args(argv)

    current = Changelog(str(args.changelog), args.changelog.read_text())
    if args.check:
        try:
            entries = unreleased_section(current).entries
        except ChangelogError as error:
            print(f"::error file={args.changelog}::{error}", file=sys.stderr)
            return 1
        print(f"{args.changelog}: `## {UNRELEASED}` holds {len(entries)} entries")
        return 0

    if args.tag is None:
        parser.error("--tag is required unless --check is given")

    version = "v" + args.tag.lstrip("v")
    try:
        at_tag = Changelog(
            f"{args.changelog} at {args.tag}",
            changelog_at_tag(args.tag, args.changelog),
        )
        text, filed, pending = file_release(current, at_tag, version)
    except ChangelogError as error:
        print(f"::error file={args.changelog}::{error}", file=sys.stderr)
        return 1

    if text == current.text:
        print(f"{args.changelog}: nothing to file under {version}")
        return 0
    written = args.changelog.with_name(args.changelog.name + ".new")
    written.write_text(text)
    os.replace(written, args.changelog)
    print(
        f"{args.changelog}: filed {filed} entries under {version}, {pending} stay unreleased"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
