import { describe, expect, it } from 'vitest'
import {
  defaultTagColor,
  maximumTagLength,
  parseTag,
  tagColorOf,
  tagColorPalette,
  tagDisplayName,
  tagWithColor,
  validateTag
} from './tags'

describe('parseTag', () => {
  it('splits a name and its color', () => {
    expect(parseTag('prod|ef4444')).toEqual({ name: 'prod', color: '#ef4444' })
  })

  it('re-adds the "#" the stored form omits', () => {
    expect(parseTag('x|22c55e').color).toBe('#22c55e')
  })

  it('rejects uppercase hex, keeping it as part of the name', () => {
    expect(parseTag('prod|ABCDEF')).toEqual({ name: 'prod|ABCDEF' })
    expect(parseTag('prod|EF4444')).toEqual({ name: 'prod|EF4444' })
  })

  it('returns no color for a bare name', () => {
    expect(parseTag('prod')).toEqual({ name: 'prod' })
  })

  it('keeps a "|" that is not followed by a six-digit hex color', () => {
    expect(parseTag('a|b')).toEqual({ name: 'a|b' })
    expect(parseTag('env|staging')).toEqual({ name: 'env|staging' })
    expect(parseTag('prod|fff')).toEqual({ name: 'prod|fff' })
    expect(parseTag('prod|gggggg')).toEqual({ name: 'prod|gggggg' })
  })

  it('treats only the final "|" segment as the color', () => {
    expect(parseTag('a|b|ef4444')).toEqual({ name: 'a|b', color: '#ef4444' })
  })
})

describe('tagDisplayName', () => {
  it('strips a color suffix', () => {
    expect(tagDisplayName('prod|ef4444')).toBe('prod')
    expect(tagDisplayName('team billing|22c55e')).toBe('team billing')
  })

  it('returns a bare name unchanged', () => {
    expect(tagDisplayName('prod')).toBe('prod')
    expect(tagDisplayName('')).toBe('')
  })

  it('keeps a non-color "|" suffix', () => {
    expect(tagDisplayName('a|b')).toBe('a|b')
  })
})

describe('tagColorOf', () => {
  it('returns the encoded color as a CSS value', () => {
    expect(tagColorOf('prod|ef4444')).toBe('#ef4444')
  })

  it('falls back to the default color when uncolored', () => {
    expect(tagColorOf('prod')).toBe(defaultTagColor)
    expect(tagColorOf('a|b')).toBe(defaultTagColor)
  })
})

describe('tagWithColor', () => {
  it('encodes a color, dropping the leading "#"', () => {
    expect(tagWithColor('prod', '#ef4444')).toBe('prod|ef4444')
  })

  it('replaces an existing suffix so re-coloring is idempotent', () => {
    expect(tagWithColor('prod|ef4444', '#22c55e')).toBe('prod|22c55e')
    expect(tagWithColor(tagWithColor('prod', '#ef4444'), '#ef4444')).toBe('prod|ef4444')
  })

  it('yields a bare name for a null or undefined color', () => {
    expect(tagWithColor('prod|ef4444', null)).toBe('prod')
    expect(tagWithColor('prod', undefined)).toBe('prod')
  })

  it('round-trips through parseTag', () => {
    expect(parseTag(tagWithColor('prod', '#ef4444'))).toEqual({ name: 'prod', color: '#ef4444' })
    // A name that itself contains "|" survives the round-trip.
    expect(parseTag(tagWithColor('a|b', '#ef4444'))).toEqual({ name: 'a|b', color: '#ef4444' })
  })

  it('lowercases an uppercase CSS color so it stays recognizable', () => {
    expect(tagWithColor('prod', '#ABCDEF')).toBe('prod|abcdef')
    expect(parseTag(tagWithColor('prod', '#ABCDEF'))).toEqual({ name: 'prod', color: '#abcdef' })
  })

  it('uses every palette color as a valid encoded tag', () => {
    for (const { color } of tagColorPalette) {
      const tag = tagWithColor('prod', color)
      expect(validateTag(tag)).toBeNull()
      expect(tagColorOf(tag)).toBe(color)
    }
  })
})

describe('validateTag', () => {
  it('accepts a plain name', () => {
    expect(validateTag('prod')).toBeNull()
  })

  it('accepts the allowed punctuation', () => {
    expect(validateTag('Mixed-1_2/3|4.5:6=7 8')).toBeNull()
  })

  it('accepts a fully encoded colored tag', () => {
    expect(validateTag('prod|ef4444')).toBeNull()
  })

  it('accepts a tag exactly at the length limit', () => {
    expect(validateTag('a'.repeat(maximumTagLength))).toBeNull()
  })

  it('rejects an empty tag', () => {
    expect(validateTag('')).not.toBeNull()
  })

  it('rejects a tag over the length limit', () => {
    expect(validateTag('a'.repeat(maximumTagLength + 1))).not.toBeNull()
  })

  it('rejects characters outside the allowed set', () => {
    expect(validateTag('a;b')).not.toBeNull()
    expect(validateTag('a#b')).not.toBeNull()
    expect(validateTag('a,b')).not.toBeNull()
    expect(validateTag('café')).not.toBeNull()
    expect(validateTag('emoji😀')).not.toBeNull()
  })
})

describe('palette', () => {
  it('is non-empty and every entry is a lowercase #rrggbb hex color', () => {
    expect(tagColorPalette.length).toBeGreaterThan(0)
    for (const { color } of tagColorPalette) {
      expect(color).toMatch(/^#[0-9a-f]{6}$/)
    }
  })
})
