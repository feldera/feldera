/**
 * Pipeline tag colors.
 *
 * The backend stores a tag as an opaque string, validates it against a fixed
 * character set (`PATTERN_VALID_TAG` in the pipeline manager), and knows nothing
 * about color. A tag's color is therefore encoded into the string itself as a
 * suffix drawn only from that character set: the visible name, then a `|`
 * separator, then a six-digit `rrggbb` hex color — no leading `#`, which the
 * backend disallows. For example `"prod|ef4444"`. The suffix is optional; a bare
 * `"prod"` is an uncolored tag, shown in a default color.
 *
 * A tag's *identity* is its name alone — the text before the color suffix — so
 * `"prod|ef4444"` and `"prod|22c55e"` are the same tag in two colors; assigning
 * one to a pipeline replaces the other. The suffix is recognized only when the
 * text after the last `|` is exactly six hex digits, so a name that itself
 * contains `|` (without a trailing color) is preserved intact.
 */

/** The fixed palette of tag label colors (see design). */
export const tagColorPalette = [
  { name: 'Red', color: '#ef4444' },
  { name: 'Purple', color: '#a855f7' },
  { name: 'Pink', color: '#ec4899' },
  { name: 'Orange', color: '#f97316' },
  { name: 'Yellow', color: '#eab308' },
  { name: 'Green', color: '#22c55e' },
  { name: 'Teal', color: '#14b8a6' },
  { name: 'Blue', color: '#3b82f6' },
  { name: 'Dark blue', color: '#1d4ed8' }
] as const

export type TagColor = (typeof tagColorPalette)[number]

/** The color shown for a tag that carries no color suffix. */
export const defaultTagColor = '#9ca3af'

/**
 * The stored color suffix: a `|` followed by exactly six hex digits at the end of
 * the string. No `#`, which the backend's tag character set disallows.
 */
const storedColorSuffix = /\|([0-9a-fA-F]{6})$/

/**
 * Split a tag into its name and optional color. The suffix is recognized only
 * when the text after the last `|` is exactly six hex digits, so a name that
 * contains `|` but no trailing color is returned whole and uncolored. The
 * returned color is a CSS `#rrggbb` value, with the `#` the stored form omits.
 */
export const parseTag = (tag: string): { name: string; color?: string } => {
  const match = storedColorSuffix.exec(tag)
  if (match) {
    return { name: tag.slice(0, match.index), color: `#${match[1]}` }
  }
  return { name: tag }
}

/** The visible text of a tag, with any color suffix removed. */
export const tagDisplayName = (tag: string): string => parseTag(tag).name

/** A tag's color as a CSS `#rrggbb` value, or {@link defaultTagColor} when none. */
export const tagColorOf = (tag: string): string => parseTag(tag).color ?? defaultTagColor

/**
 * Encode `color` (a CSS `#rrggbb` value) into a tag as a `|rrggbb` suffix,
 * dropping the leading `#` the backend disallows and replacing any existing
 * suffix first so re-coloring is idempotent. A null or undefined color yields the
 * bare name.
 */
export const tagWithColor = (tag: string, color?: string | null): string => {
  const name = tagDisplayName(tag)
  return color ? `${name}|${color.replace(/^#/, '')}` : name
}

/** Maximum length of a single stored tag, mirroring the backend. */
export const maximumTagLength = 50

/**
 * Characters a tag may contain, mirroring the backend's `PATTERN_VALID_TAG`
 * (crates/pipeline-manager/src/db/types/utils.rs); kept in sync by hand. The
 * color suffix draws only from this set, so a fully encoded tag is valid whenever
 * its name is.
 */
export const tagPattern = /^[a-zA-Z0-9 ._/|\\:=-]+$/

/**
 * Check a fully encoded tag against the same rules the backend enforces, so the
 * UI can reject a bad tag up front instead of waiting for the API to fail.
 * Returns a human-readable reason, or null when the tag is valid.
 */
export const validateTag = (tag: string): string | null => {
  if (tag.length === 0) {
    return 'A tag cannot be empty.'
  }
  if (tag.length > maximumTagLength) {
    return `A tag may be at most ${maximumTagLength} characters; this one is ${tag.length}.`
  }
  if (!tagPattern.test(tag)) {
    return 'A tag may contain only letters, numbers, spaces, and the characters . _ / | \\ : = -'
  }
  return null
}
