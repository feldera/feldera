/**
 * Tests for the shared log-search algorithm (common-ui/logSearch). Runs in the browser
 * project: `applySearchHighlight` drives the CSS Custom Highlight API (needs a real DOM,
 * `Range`, and `CSS.highlights`), and importing the `common-ui` barrel pulls in Monaco, which
 * touches `window` — so the pure functions ride along here rather than in a node project.
 */

import {
  advanceSearch,
  applySearchHighlight,
  compileSearchPattern,
  countOccurrences,
  emptySearchState,
  findMatchOffsets,
  findOccurrence,
  isFindShortcut,
  type SearchPattern,
  searchPatternsEqual,
  positiveMod
} from 'common-ui'
import { afterEach, describe, expect, it } from 'vitest'

const substr = (query: string, caseSensitive = false): SearchPattern => ({
  kind: 'substring',
  query,
  caseSensitive
})

describe('searchPatternsEqual', () => {
  it('treats null/null as equal and null/pattern as different', () => {
    expect(searchPatternsEqual(null, null)).toBe(true)
    expect(searchPatternsEqual(null, substr('a'))).toBe(false)
    expect(searchPatternsEqual(substr('a'), null)).toBe(false)
  })

  it('compares substring query and case-sensitivity', () => {
    expect(searchPatternsEqual(substr('a'), substr('a'))).toBe(true)
    expect(searchPatternsEqual(substr('a'), substr('b'))).toBe(false)
    expect(searchPatternsEqual(substr('a', true), substr('a', false))).toBe(false)
  })

  it('compares regex source and (normalised) flags, and never equates different kinds', () => {
    const re = (source: string, flags?: string): SearchPattern => ({ kind: 'regex', source, flags })
    expect(searchPatternsEqual(re('x'), re('x'))).toBe(true)
    expect(searchPatternsEqual(re('x'), re('x', ''))).toBe(true) // undefined flags ≡ ''
    expect(searchPatternsEqual(re('x', 'i'), re('x'))).toBe(false)
    expect(searchPatternsEqual(re('x'), substr('x'))).toBe(false)
  })
})

describe('positiveMod', () => {
  it('leaves an in-range cursor alone', () => {
    expect(positiveMod(0, 3)).toBe(0)
    expect(positiveMod(2, 3)).toBe(2)
  })

  it('wraps forward past the last match', () => {
    expect(positiveMod(3, 3)).toBe(0)
    expect(positiveMod(7, 3)).toBe(1)
  })

  it('wraps backward before the first match (plain % would give a negative index)', () => {
    expect(-1 % 3).toBe(-1) // the reason the Euclidean form is needed
    expect(positiveMod(-1, 3)).toBe(2)
    expect(positiveMod(-4, 3)).toBe(2)
  })

  it('collapses onto the only match for a single-match search', () => {
    expect(positiveMod(-5, 1)).toBe(0)
    expect(positiveMod(5, 1)).toBe(0)
  })
})

describe('advanceSearch', () => {
  it('clears on an empty (null) pattern', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 3 }
    expect(advanceSearch(state, null)).toEqual(emptySearchState)
  })

  it('advances the cursor when the pattern is unchanged', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 1 }
    expect(advanceSearch(state, substr('a'))).toEqual({ pattern: substr('a'), occurrenceIndex: 2 })
  })

  it('resets the cursor to the first match on a new pattern', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 5 }
    expect(advanceSearch(state, substr('b'))).toEqual({ pattern: substr('b'), occurrenceIndex: 0 })
  })

  it('steps the cursor backward on the same pattern with direction "prev"', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 3 }
    expect(advanceSearch(state, substr('a'), 'prev')).toEqual({
      pattern: substr('a'),
      occurrenceIndex: 2
    })
  })

  it('ignores direction on a new pattern (always the first match)', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 5 }
    expect(advanceSearch(state, substr('b'), 'prev')).toEqual({
      pattern: substr('b'),
      occurrenceIndex: 0
    })
  })

  it('defaults to stepping forward when no direction is given', () => {
    const state = { pattern: substr('a'), occurrenceIndex: 1 }
    expect(advanceSearch(state, substr('a')).occurrenceIndex).toBe(2)
  })
})

describe('countOccurrences', () => {
  const lines = ['alpha', 'beta', 'gamma', 'alpha again']

  it('counts matching lines (one match per line, matching findOccurrence)', () => {
    expect(countOccurrences(lines, substr('alpha'))).toBe(2)
    expect(countOccurrences(lines, substr('a'))).toBe(4) // every line contains "a"
  })

  it('returns 0 for a null, empty, or non-matching pattern', () => {
    expect(countOccurrences(lines, null)).toBe(0)
    expect(countOccurrences(lines, substr(''))).toBe(0)
    expect(countOccurrences(lines, substr('zzz'))).toBe(0)
  })

  it('respects case sensitivity', () => {
    expect(countOccurrences(['ABC', 'abc'], substr('abc', true))).toBe(1)
    expect(countOccurrences(['ABC', 'abc'], substr('abc', false))).toBe(2)
  })
})

describe('isFindShortcut', () => {
  const ev = (init: KeyboardEventInit) => new KeyboardEvent('keydown', init)

  it('matches Ctrl-F and Cmd-F (case-insensitive key)', () => {
    expect(isFindShortcut(ev({ key: 'f', ctrlKey: true }))).toBe(true)
    expect(isFindShortcut(ev({ key: 'F', metaKey: true }))).toBe(true)
  })

  it('rejects plain "f", other keys, and extra modifiers', () => {
    expect(isFindShortcut(ev({ key: 'f' }))).toBe(false)
    expect(isFindShortcut(ev({ key: 'g', ctrlKey: true }))).toBe(false)
    expect(isFindShortcut(ev({ key: 'f', ctrlKey: true, shiftKey: true }))).toBe(false)
    expect(isFindShortcut(ev({ key: 'f', ctrlKey: true, altKey: true }))).toBe(false)
  })
})

describe('compileSearchPattern', () => {
  it('returns null for empty queries/sources so callers skip highlighting', () => {
    expect(compileSearchPattern(substr(''))).toBeNull()
    expect(compileSearchPattern({ kind: 'regex', source: '' })).toBeNull()
  })

  it('returns null for an invalid regex rather than throwing', () => {
    expect(compileSearchPattern({ kind: 'regex', source: '(' })).toBeNull()
  })

  it('matches case-insensitively by default and case-sensitively when asked', () => {
    expect(compileSearchPattern(substr('abc'))!('xxABCxx')).toBe(true)
    expect(compileSearchPattern(substr('abc', true))!('xxABCxx')).toBe(false)
    expect(compileSearchPattern(substr('ABC', true))!('xxABCxx')).toBe(true)
  })

  it('compiles a regex matcher', () => {
    const m = compileSearchPattern({ kind: 'regex', source: 'a.c' })!
    expect(m('ac')).toBe(false) // no char between a and c
    expect(m('xabc')).toBe(true)
  })
})

describe('findMatchOffsets', () => {
  it('finds every non-overlapping substring occurrence', () => {
    expect(findMatchOffsets('ababab', substr('ab'))).toEqual([
      [0, 2],
      [2, 4],
      [4, 6]
    ])
  })

  it('maps case-insensitive matches back to the original line offsets', () => {
    expect(findMatchOffsets('xAbcy', substr('abc'))).toEqual([[1, 4]])
  })

  it('finds every regex occurrence, forcing the global flag', () => {
    expect(findMatchOffsets('a1b2c3', { kind: 'regex', source: '[0-9]' })).toEqual([
      [1, 2],
      [3, 4],
      [5, 6]
    ])
  })

  it('does not loop forever on a zero-length regex match', () => {
    // /(?=b)/ matches at a position without consuming — the impl must advance past it.
    expect(findMatchOffsets('abc', { kind: 'regex', source: '(?=b)' })).toEqual([])
  })

  it('returns [] for empty/invalid patterns and non-matches', () => {
    expect(findMatchOffsets('abc', substr(''))).toEqual([])
    expect(findMatchOffsets('abc', { kind: 'regex', source: '(' })).toEqual([])
    expect(findMatchOffsets('abc', substr('z'))).toEqual([])
  })
})

describe('findOccurrence', () => {
  const lines = ['alpha', 'beta', 'gamma', 'alpha again']

  it('returns the line index of the n-th matching line (one match per line)', () => {
    expect(findOccurrence(lines, substr('alpha'), 0)).toBe(0)
    expect(findOccurrence(lines, substr('alpha'), 1)).toBe(3)
  })

  it('wraps the cursor modulo the match count, including negatives', () => {
    expect(findOccurrence(lines, substr('alpha'), 2)).toBe(0) // wraps back to first
    expect(findOccurrence(lines, substr('alpha'), 3)).toBe(3)
    expect(findOccurrence(lines, substr('alpha'), -1)).toBe(3) // Python-style negative wrap
  })

  it('returns -1 when nothing matches or the pattern is empty', () => {
    expect(findOccurrence(lines, substr('zzz'), 0)).toBe(-1)
    expect(findOccurrence(lines, substr(''), 0)).toBe(-1)
  })
})

const NAME = 'logsearch-test-highlight'

let host: HTMLDivElement | undefined

const mount = (html: string) => {
  host = document.createElement('div')
  host.innerHTML = html
  document.body.appendChild(host)
  return host
}

const highlightedText = () => {
  const hl = CSS.highlights.get(NAME)
  if (!hl) {
    return []
  }
  return [...hl].map((range) => range.toString())
}

afterEach(() => {
  CSS.highlights.delete(NAME)
  host?.remove()
  host = undefined
})

describe('applySearchHighlight', () => {
  it('paints a single text-node range over the matched characters', () => {
    const el = mount('hello world')
    applySearchHighlight(NAME, el, [[0, 5]])
    expect(highlightedText()).toEqual(['hello'])
  })

  it('paints multiple ranges on one element', () => {
    const el = mount('ab ab')
    applySearchHighlight(NAME, el, [
      [0, 2],
      [3, 5]
    ])
    expect(highlightedText()).toEqual(['ab', 'ab'])
  })

  it('spans a range across several text nodes (ANSI-decorated rows have many <span>s)', () => {
    // Offsets are computed against the flattened text "hello", but the DOM splits it into
    // three text nodes — the walker must stitch them back together.
    const el = mount('<span>hel</span><span>l</span><span>o</span>')
    applySearchHighlight(NAME, el, [[0, 5]])
    expect(highlightedText()).toEqual(['hello'])
  })

  it('clears the highlight when offsets is empty', () => {
    const el = mount('hello')
    applySearchHighlight(NAME, el, [[0, 5]])
    expect(CSS.highlights.has(NAME)).toBe(true)
    applySearchHighlight(NAME, el, [])
    expect(CSS.highlights.has(NAME)).toBe(false)
  })

  it('clears the highlight when the element is null', () => {
    const el = mount('hello')
    applySearchHighlight(NAME, el, [[0, 5]])
    applySearchHighlight(NAME, null, [[0, 5]])
    expect(CSS.highlights.has(NAME)).toBe(false)
  })

  it('skips out-of-range offsets without creating a stray highlight', () => {
    const el = mount('hi')
    applySearchHighlight(NAME, el, [[10, 20]])
    expect(CSS.highlights.has(NAME)).toBe(false)
  })
})
