//! Pipeline tag conventions shared across Feldera clients.
//!
//! The backend stores a tag as an opaque string, validates it against a fixed
//! character set, and knows nothing about color. A tag's color is encoded into
//! the string itself as a suffix that character set permits: the visible name,
//! then a `|` separator, then a six-digit `rrggbb` hex color (no leading `#`,
//! which the backend disallows) -- e.g. `"prod|ef4444"`. The suffix is optional,
//! so a bare `"prod"` is an uncolored tag; `"prod|ef4444"` and `"prod|22c55e"`
//! are the same tag in two colors.
//!
//! The CLI treats a tag's *display name* -- its name with any color suffix
//! removed -- as the tag's identity, and enforces two invariants whenever a
//! pipeline's tags are written:
//!
//! * **Variant exclusivity.** A pipeline carries at most one tag per display
//!   name. When the input holds several color variants of the same name, the
//!   last one wins.
//! * **Lexicographic order.** Tags are stored sorted, so the stored order is
//!   stable regardless of the order they were supplied in.
//!
//! Both operate on the full string, so a tag's color is preserved; only the
//! display name decides which tags collide. A name alone is enough to refer to a
//! tag: its color is filled in from the same tag elsewhere, so no command
//! requires the user to type the raw color suffix.

use std::collections::HashMap;

/// Split a comma-separated tag list into individual tags.
///
/// Empty segments are dropped, so an empty string yields no tags -- the way
/// `set-tags` clears a pipeline -- and a stray trailing comma is harmless. Only
/// the comma separates tags, so spaces are kept and a quoted tag such as
/// `"team billing"` survives intact.
pub fn split_tag_list(input: &str) -> Vec<String> {
    input
        .split(',')
        .filter(|tag| !tag.is_empty())
        .map(|tag| tag.to_string())
        .collect()
}

/// A tag's visible text: its name, with any trailing `|rrggbb` color suffix
/// removed.
///
/// The suffix is recognized only when the text after the last `|` is exactly six
/// hex digits, so a name that itself contains `|` (without a trailing color) is
/// returned whole.
pub fn tag_display_name(tag: &str) -> &str {
    if let Some(separator) = tag.rfind('|')
        && is_hex_color(&tag[separator + 1..])
    {
        return &tag[..separator];
    }
    tag
}

/// Whether `text` is a six-digit `rrggbb` hex color.
fn is_hex_color(text: &str) -> bool {
    text.len() == 6 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Collapse color variants of the same tag and sort the result.
///
/// Tags that share a display name are deduplicated, keeping the last occurrence,
/// and the survivors are returned in lexicographic order. The color suffix that
/// encodes a tag's color is preserved.
pub fn normalize_tags<I, S>(tags: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut by_display_name: HashMap<String, String> = HashMap::new();
    for tag in tags {
        let tag = tag.as_ref();
        by_display_name.insert(tag_display_name(tag).to_string(), tag.to_string());
    }
    let mut result: Vec<String> = by_display_name.into_values().collect();
    result.sort();
    result
}

/// Index a pool of tags by display name, mapping each name to one colored
/// variant.
///
/// When the pool holds several color variants of the same name -- which can
/// happen across pipelines if some client diverged -- the lexicographically
/// smallest one wins, so the choice is deterministic.
fn variant_index(pool: &[String]) -> HashMap<String, String> {
    let mut sorted = pool.to_vec();
    sorted.sort();
    let mut index: HashMap<String, String> = HashMap::new();
    for tag in sorted {
        index
            .entry(tag_display_name(&tag).to_string())
            .or_insert(tag);
    }
    index
}

/// Choose the colored variant to store for `name`.
///
/// The CLI cannot express a color, so it borrows one rather than inventing it:
/// any variant of this name known across all pipelines (`known`) wins, and only
/// a name seen nowhere is stored uncolored. This keeps a given name one
/// consistent color everywhere.
fn resolve_variant(name: &str, known: &HashMap<String, String>) -> String {
    known
        .get(tag_display_name(name))
        .cloned()
        .unwrap_or_else(|| name.to_string())
}

/// Resolve `requested` against the colors in use across all pipelines and return
/// the normalized result.
///
/// Each requested name borrows a color via [`resolve_variant`]: a variant known
/// across all pipelines (`known`), else uncolored.
pub fn set_tags<I, S>(requested: I, known: &[String]) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let known = variant_index(known);
    normalize_tags(
        requested
            .into_iter()
            .map(|tag| resolve_variant(tag.as_ref(), &known)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_name_strips_color_suffix() {
        assert_eq!(tag_display_name("prod"), "prod");
        assert_eq!(tag_display_name("prod|ef4444"), "prod");
        assert_eq!(tag_display_name("team billing|22c55e"), "team billing");
        // Uppercase hex digits are a valid color.
        assert_eq!(tag_display_name("prod|ABCDEF"), "prod");
        // A name containing '|' but no valid trailing color is returned whole.
        assert_eq!(tag_display_name("a|b"), "a|b");
        assert_eq!(tag_display_name("env|staging"), "env|staging");
        // Wrong-length or malformed suffixes are not colors.
        assert_eq!(tag_display_name("prod|fff"), "prod|fff");
        assert_eq!(tag_display_name("prod|gggggg"), "prod|gggggg");
        // Only the final '|' segment is the color; an inner '|' stays in the name.
        assert_eq!(tag_display_name("a|b|ef4444"), "a|b");
        assert_eq!(tag_display_name(""), "");
    }

    #[test]
    fn sorts_lexicographically() {
        assert_eq!(
            normalize_tags(["prod", "dev", "staging"]),
            vec!["dev", "prod", "staging"]
        );
    }

    #[test]
    fn is_idempotent() {
        let once = normalize_tags(["beta", "alpha", "alpha|ef4444"]);
        assert_eq!(normalize_tags(&once), once);
    }

    #[test]
    fn empty_input() {
        assert_eq!(normalize_tags(Vec::<String>::new()), Vec::<String>::new());
    }

    #[test]
    fn collapses_color_variants_keeping_last() {
        assert_eq!(normalize_tags(["prod", "prod|ef4444"]), vec!["prod|ef4444"]);
        assert_eq!(normalize_tags(["prod|ef4444", "prod"]), vec!["prod"]);
    }

    #[test]
    fn preserves_color_of_survivor() {
        assert_eq!(
            normalize_tags(["dev", "prod|ef4444", "qa"]),
            vec!["dev", "prod|ef4444", "qa"]
        );
    }

    #[test]
    fn distinct_names_with_colors_all_kept() {
        assert_eq!(
            normalize_tags(["b|ef4444", "a|22c55e", "c"]),
            vec!["a|22c55e", "b|ef4444", "c"]
        );
    }

    #[test]
    fn split_drops_empty_segments_and_keeps_spaces() {
        assert_eq!(split_tag_list(""), Vec::<String>::new());
        assert_eq!(split_tag_list("a,b,c"), vec!["a", "b", "c"]);
        // A trailing or doubled comma yields no empty tag.
        assert_eq!(split_tag_list("a,,b,"), vec!["a", "b"]);
        // A quoted tag arrives as one argument with its space intact.
        assert_eq!(
            split_tag_list("team billing,prod"),
            vec!["team billing", "prod"]
        );
    }

    #[test]
    fn set_borrows_color_from_known_pool() {
        // "prod" is requested by name and colored in the known pool; setting it
        // adopts that color, while "dev" is unknown and stored uncolored.
        assert_eq!(
            set_tags(["prod", "dev"], &["prod|ef4444".to_string()]),
            vec!["dev", "prod|ef4444"]
        );
    }

    #[test]
    fn set_color_conflict_is_deterministic() {
        // The known pool colors "prod" two ways; the lexicographically smallest
        // variant ("...|22c55e" < "...|ef4444") wins, so the choice is stable.
        assert_eq!(
            set_tags(
                ["prod"],
                &["prod|ef4444".to_string(), "prod|22c55e".to_string()]
            ),
            vec!["prod|22c55e"]
        );
    }

    #[test]
    fn set_adds_uncolored_name_when_absent() {
        assert_eq!(set_tags(["prod"], &[]), vec!["prod"]);
    }

    #[test]
    fn set_with_no_tags_clears() {
        assert_eq!(set_tags(Vec::<String>::new(), &[]), Vec::<String>::new());
    }
}
