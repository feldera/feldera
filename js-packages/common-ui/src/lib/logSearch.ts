/**
 * Shared search algorithm for line-oriented views (pipeline logs, profiler bundle logs, ...).
 *
 * The host owns a {@link SearchState} and advances it with {@link advanceSearch} on each
 * "next match" interaction. From that state it calls
 * `findOccurrence(lines, state.pattern, state.occurrenceIndex)` to get the line to scroll to,
 * and {@link findMatchOffsets} + {@link applySearchHighlight} to paint the match.
 */

/** Tagged union of supported search modes. Extend with new kinds (fuzzy, word-boundary,
 *  multi-token, ...) by adding a variant here and a branch in {@link compileSearchPattern}. */
export type SearchPattern =
  | { kind: 'substring'; query: string; caseSensitive?: boolean }
  | { kind: 'regex'; source: string; flags?: string }

/** Search state owned by the host of a log view. Pass directly to a view component; mutate
 *  through {@link advanceSearch}. */
export type SearchState = {
  /** Active search; `null` when the search input is empty (nothing highlighted). */
  pattern: SearchPattern | null
  /** Which match to highlight. Wraps modulo the match count, so the host can just increment
   *  it on each "next match" interaction without bounds-checking. */
  occurrenceIndex: number
}

/** The starting state — no pattern, cursor at zero. */
export const emptySearchState: SearchState = { pattern: null, occurrenceIndex: 0 }

/** True when two patterns describe the same search — used by hosts to decide whether
 *  pressing Enter should advance to the next match (same pattern) or jump to the first
 *  match of a new pattern (different pattern). */
export function searchPatternsEqual(a: SearchPattern | null, b: SearchPattern | null): boolean {
  if (a === b) return true
  if (!a || !b) return false
  if (a.kind !== b.kind) return false
  if (a.kind === 'substring' && b.kind === 'substring') {
    return a.query === b.query && !!a.caseSensitive === !!b.caseSensitive
  }
  if (a.kind === 'regex' && b.kind === 'regex') {
    return a.source === b.source && (a.flags ?? '') === (b.flags ?? '')
  }
  return false
}

/**
 * Compute the next {@link SearchState} for a "submit search" interaction (typically Enter
 * on the panel's search input).
 *   - `next = null` (or an empty pattern): clear the search.
 *   - `next` equals the current pattern: advance to the next match (`occurrenceIndex + 1`).
 *   - `next` differs from the current pattern: jump to the first match of the new pattern.
 *
 * Centralising this here keeps every host's submit handler identical and ensures the
 * cursor semantics stay consistent across log views.
 */
export function advanceSearch(state: SearchState, next: SearchPattern | null): SearchState {
  if (!next) return emptySearchState
  if (searchPatternsEqual(state.pattern, next)) {
    return { pattern: state.pattern, occurrenceIndex: state.occurrenceIndex + 1 }
  }
  return { pattern: next, occurrenceIndex: 0 }
}

/** A compiled per-line predicate. `null` means "the pattern is empty or invalid; do not
 *  highlight anything" — keeps callers free of empty-query / bad-regex branching. */
export type LineMatcher = ((line: string) => boolean) | null

/** Half-open `[start, end)` character offsets of a single match within a line. */
export type MatchRange = readonly [start: number, end: number]

export function compileSearchPattern(pattern: SearchPattern): LineMatcher {
  if (pattern.kind === 'substring') {
    if (!pattern.query) return null
    if (pattern.caseSensitive) {
      const q = pattern.query
      return (line) => line.includes(q)
    }
    const q = pattern.query.toLowerCase()
    return (line) => line.toLowerCase().includes(q)
  }
  if (pattern.kind === 'regex') {
    if (!pattern.source) return null
    try {
      const re = new RegExp(pattern.source, pattern.flags)
      return (line) => re.test(line)
    } catch {
      return null
    }
  }
  return null
}

/**
 * Find the `[start, end)` character offsets of every match of `pattern` in `line`.
 *
 * Used by {@link applySearchHighlight} to drive the CSS Custom Highlight API — the host
 * doesn't have to split the line into DOM segments; it renders the row normally and the
 * browser paints the highlight ranges via `::highlight(...)`.
 *
 * Empty / invalid patterns and lines with no matches return `[]`.
 */
export function findMatchOffsets(line: string, pattern: SearchPattern): MatchRange[] {
  const offsets: MatchRange[] = []
  if (pattern.kind === 'substring') {
    if (!pattern.query) return offsets
    const haystack = pattern.caseSensitive ? line : line.toLowerCase()
    const needle = pattern.caseSensitive ? pattern.query : pattern.query.toLowerCase()
    let idx = haystack.indexOf(needle)
    while (idx >= 0) {
      offsets.push([idx, idx + needle.length] as const)
      idx = haystack.indexOf(needle, idx + needle.length)
    }
    return offsets
  }
  if (pattern.kind === 'regex') {
    if (!pattern.source) return offsets
    let re: RegExp
    try {
      const flags = pattern.flags ?? ''
      re = new RegExp(pattern.source, flags.includes('g') ? flags : flags + 'g')
    } catch {
      return offsets
    }
    let m: RegExpExecArray | null
    while ((m = re.exec(line)) !== null) {
      if (!m[0]) {
        // Zero-length match (e.g. /(?=x)/): advance past it so we don't loop forever.
        re.lastIndex++
        continue
      }
      offsets.push([m.index, m.index + m[0].length] as const)
    }
  }
  return offsets
}

/**
 * Paint `offsets` onto `el` using the CSS Custom Highlight API, registered under
 * `highlightName`. Pass `el = null` or `offsets = []` to clear.
 *
 * The CSS Custom Highlight API
 * (https://developer.mozilla.org/docs/Web/API/CSS_Custom_Highlight_API) lets us mark up
 * character ranges in the DOM without ever touching the row's rendered structure — the row
 * stays a single `<ANSIDecoratedText>` and the browser overlays the highlight via the
 * matching `::highlight(<name>)` CSS rule defined by the host. That keeps row rendering
 * uniform between matched and unmatched lines.
 *
 * Offsets are interpreted against `el`'s text content (text-node order). Callers that pass
 * an element rendering ANSI-decorated text must compute offsets against `stripAnsi(line)` —
 * the visible characters match, even though the DOM has extra <span> wrappers.
 *
 * No-op (silently clears) on browsers that don't expose `CSS.highlights` or the global
 * `Highlight` constructor (older Safari/Firefox).
 */
export function applySearchHighlight(
  highlightName: string,
  el: HTMLElement | null,
  offsets: readonly MatchRange[]
): void {
  // Guard for older browsers / SSR. The feature shipped in Chromium 105, Safari 17.2,
  // Firefox 140 — we degrade to "no highlight" elsewhere rather than erroring.
  if (typeof CSS === 'undefined' || !('highlights' in CSS) || typeof Highlight === 'undefined') {
    return
  }
  if (!el || offsets.length === 0) {
    CSS.highlights.delete(highlightName)
    return
  }
  // Walk text nodes once, recording each one's [start, end) span in the element's flattened
  // text. Looking up a Range endpoint is then a linear scan over this list. We re-walk on
  // every paint (rather than caching) because the host re-paints when the log content
  // refreshes, and we only ever walk the single matched row.
  const spans: Array<{ node: Text; start: number; end: number }> = []
  const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT)
  let total = 0
  let node: Node | null
  while ((node = walker.nextNode())) {
    const tn = node as Text
    const len = tn.data.length
    spans.push({ node: tn, start: total, end: total + len })
    total += len
  }
  const locate = (offset: number) => {
    for (const s of spans) {
      if (offset >= s.start && offset <= s.end) {
        return { node: s.node, offset: offset - s.start }
      }
    }
    return null
  }
  const ranges: Range[] = []
  for (const [s, e] of offsets) {
    const startPos = locate(s)
    const endPos = locate(e)
    if (!startPos || !endPos) continue
    const r = new Range()
    r.setStart(startPos.node, startPos.offset)
    r.setEnd(endPos.node, endPos.offset)
    ranges.push(r)
  }
  if (ranges.length === 0) {
    CSS.highlights.delete(highlightName)
    return
  }
  CSS.highlights.set(highlightName, new Highlight(...ranges))
}

/**
 * Find the line index of the `occurrenceIndex`-th match of `pattern` in `lines`.
 *
 * `occurrenceIndex` wraps modulo the total match count, so callers don't need to track the
 * count — incrementing past the end cycles back to the first match. Negative indices wrap
 * the same way (Python-style).
 *
 * Returns `-1` when the pattern is empty/invalid or no lines match. One match per line:
 * multiple substrings on the same line still count as a single occurrence.
 */
export function findOccurrence(
  lines: readonly string[],
  pattern: SearchPattern,
  occurrenceIndex: number
): number {
  const matcher = compileSearchPattern(pattern)
  if (!matcher) return -1
  const matches: number[] = []
  for (let i = 0; i < lines.length; i++) {
    if (matcher(lines[i])) matches.push(i)
  }
  if (matches.length === 0) return -1
  const n = matches.length
  const wrapped = ((occurrenceIndex % n) + n) % n
  return matches[wrapped]
}
