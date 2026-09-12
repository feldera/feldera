"""Tests for scripts/file_changelog_release.py."""

import contextlib
import io
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from file_changelog_release import (  # noqa: E402
    Changelog,
    ChangelogError,
    changelog_at_tag,
    check_filing,
    file_release,
    main,
)

INDENT = ""  # the changelog at an older release tag uses " " * 8


def changelog(unreleased_body, tail="## v0.340.0\n\n- older entry\n", indent=INDENT):
    """A changelog whose Unreleased section holds `unreleased_body`."""
    return (
        "# Changelog\n\n"
        f"{indent}## Unreleased\n\n"
        f"{textwrap.indent(textwrap.dedent(unreleased_body).strip(), indent)}\n\n"
        f"{textwrap.indent(tail, indent)}"
    )


def on_main(body, **rest):
    """The changelog as it stands on main."""
    return Changelog("changelog.md", changelog(body, **rest))


def at_tag(body, **rest):
    """The same changelog at the release tag."""
    return Changelog("changelog.md at v0.348.0", changelog(body, **rest))


class FileReleaseTest(unittest.TestCase):
    def test_splits_shipped_from_later_entries(self):
        shipped = at_tag("- shipped one\n\n- shipped two")
        current = on_main(
            "- merged after the release\n\n- shipped one\n\n- shipped two"
        )

        text, filed, pending = file_release(current, shipped, "v0.348.0")

        self.assertEqual((filed, pending), (2, 1))
        self.assertEqual(
            [line for line in text.splitlines() if line.startswith(("## ", "- "))],
            [
                "## Unreleased",
                "- merged after the release",
                "## v0.348.0",
                "- shipped one",
                "- shipped two",
                "## v0.340.0",
                "- older entry",
            ],
        )

    def test_keeps_multi_line_entries_whole(self):
        entry = "- an entry whose text\n  wraps over three\n  source lines"
        text, filed, _ = file_release(on_main(entry), at_tag(entry), "v0.348.0")

        self.assertEqual(filed, 1)
        self.assertIn(
            f"{INDENT}## v0.348.0\n\n{textwrap.indent(entry, INDENT)}\n", text
        )

    def test_matches_an_entry_that_was_rewrapped(self):
        shipped = at_tag("- one entry that wraps\n  over two lines")
        current = on_main("- one entry\n  that wraps over\n  two lines")

        _, filed, pending = file_release(current, shipped, "v0.348.0")

        self.assertEqual((filed, pending), (1, 0))

    def test_adds_no_heading_when_the_release_shipped_no_entries(self):
        current = on_main("- merged after the release")

        text, filed, pending = file_release(current, at_tag(""), "v0.348.0")

        self.assertEqual((filed, pending), (0, 1))
        self.assertEqual(text, current.text)
        self.assertNotIn("v0.348.0", text)

    def test_fails_when_no_entry_of_the_release_is_still_unreleased(self):
        shipped = at_tag("- shipped one\n\n- shipped two")
        current = on_main("- an entry that replaced both shipped ones")

        with self.assertRaises(ChangelogError) as raised:
            file_release(current, shipped, "v0.348.0")

        self.assertIn("none of the 2 entries", str(raised.exception))

    def test_a_rewrite_that_loses_an_entry_is_an_error(self):
        body = "- shipped one\n\n- shipped two"
        text, _, _ = file_release(on_main(body), at_tag(body), "v0.348.0")
        filed = [[f"{INDENT}- shipped one"], [f"{INDENT}- shipped two"]]
        damaged = text.replace(f"{INDENT}- shipped two\n\n", "")

        check_filing(Changelog("changelog.md", text), "v0.348.0", filed, [])
        with self.assertRaises(ChangelogError):
            check_filing(Changelog("changelog.md", damaged), "v0.348.0", filed, [])

    def test_second_run_changes_nothing(self):
        shipped = at_tag("- shipped one")
        current = on_main("- merged after the release\n\n- shipped one")

        once, _, _ = file_release(current, shipped, "v0.348.0")
        again = Changelog("changelog.md", once)
        twice, filed, pending = file_release(again, shipped, "v0.348.0")

        self.assertEqual(twice, once)
        self.assertEqual((filed, pending), (0, 0))

    def test_ignores_a_heading_or_bullet_inside_a_code_block(self):
        example = (
            "- an entry with an example\n\n  ```\n  ## v0.1.0\n  - not an entry\n  ```"
        )
        current = on_main(f"{example}\n\n- shipped one")

        text, filed, pending = file_release(
            current, at_tag("- shipped one"), "v0.348.0"
        )

        self.assertEqual((filed, pending), (1, 1))
        self.assertIn(example, text)

    def test_matches_an_indented_copy_at_the_tag(self):
        # The changelog at an older release tag indents its entries; comparing
        # canonical forms files them all the same.
        current = on_main("- shipped one\n\n- merged after the release")
        indented = changelog("- shipped one", indent=" " * 8)
        tagged = Changelog("changelog.md at v0.348.0", indented)

        _, filed, pending = file_release(current, tagged, "v0.348.0")

        self.assertEqual((filed, pending), (1, 1))


class ChangelogAtTagTest(unittest.TestCase):
    """The script reads the released copy from git itself."""

    def test_an_unknown_tag_is_an_error(self):
        with self.assertRaises(ChangelogError):
            changelog_at_tag(
                "v0.0.0-no-such-tag", Path("docs.feldera.com/docs/changelog.md")
            )


class CheckModeTest(unittest.TestCase):
    """The pre-commit hook keeps a broken Unreleased section from reaching main."""

    def check(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "changelog.md"
            path.write_text(text)
            quiet = io.StringIO()
            with contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
                return main(["--check", "--changelog", str(path)])

    def test_rejects_a_changelog_without_an_unreleased_heading(self):
        renamed = changelog("- an entry").replace("## Unreleased", "## Next")

        self.assertEqual(self.check(renamed), 1)


if __name__ == "__main__":
    unittest.main()
