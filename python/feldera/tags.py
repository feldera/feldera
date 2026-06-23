"""
Pipeline tag conventions.

The backend stores a tag as an opaque string and validates it against an allowed
character set. The web console encodes a tag's color into the string itself
as a suffix that character set permits: the visible name, then a ``|`` separator,
then a six-digit ``rrggbb`` hex color -- e.g. ``"prod|ef4444"``. The suffix
is optional, so a bare ``"prod"`` is an uncolored tag; ``"prod|ef4444"`` and
``"prod|22c55e"`` are considered a tag collision which can but should not occur.

To stay consistent with the web console, the SDK treats a tag's *display name* --
its name with any color suffix removed -- as the tag's identity, and enforces two
invariants whenever a pipeline's tags are written:

* **Variant exclusivity.** A pipeline carries at most one tag per display name.
  When a list holds several color variants of the same name, the last one wins.
* **Lexicographic order.** Tags are stored sorted,
  so the stored order is stable regardless of the order they were supplied in.
"""

import re
from typing import Iterable, List

# A tag's color suffix: a ``|`` followed by exactly six hex digits at the end of
# the string (no ``#``, which the backend's tag character set disallows).
# Recognized only in this exact form, so a name that itself contains ``|``
# (without a trailing color) keeps its full text.
_COLOR_SUFFIX = re.compile(r"\|[0-9a-fA-F]{6}$")


def tag_display_name(tag: str) -> str:
    """Return a tag's visible text: its name with any color suffix removed."""

    return _COLOR_SUFFIX.sub("", tag)


def normalize_tags(tags: Iterable[str]) -> List[str]:
    """
    Collapse color variants of the same tag and sort the result.

    Tags that share a display name are deduplicated, keeping the last occurrence,
    and the survivors are returned in lexicographic order. The color suffix that
    encodes a tag's color is preserved.
    """

    by_display_name: dict[str, str] = {}
    for tag in tags:
        by_display_name[tag_display_name(tag)] = tag
    return sorted(by_display_name.values())
