import stripAnsi from 'strip-ansi'

/** A copy selection over a virtualised line list: the whole list, or a row/column range
 *  (endpoints resolved by {@link resolveCopySlice}). */
export type CopySlice =
  | 'all'
  | { start: { row: number; col: number }; end: { row: number; col: number } }

/**
 * Build the clipboard text for a {@link CopySlice} over `lines`, ANSI-stripped and newline-joined.
 *
 * A line is text without its terminator, so producers strip one before it ever reaches here. A row
 * that carried its own newline would need an empty separator, and would also step outside the
 * printable-ASCII grid that {@link isPredictable} tests, emptying the size cache.
 */
export function sliceLinesForCopy(lines: readonly string[], slice: CopySlice): string {
  if (slice === 'all') return lines.map(stripAnsi).join('\n')
  const result = lines.slice(slice.start.row, slice.end.row + 1).map(stripAnsi)
  result[0] = result[0].slice(slice.start.col)
  result[result.length - 1] = result[result.length - 1].slice(
    0,
    slice.end.col - (slice.start.row === slice.end.row ? slice.start.col : 0)
  )
  return result.join('\n')
}

/** Nearest ancestor carrying a `data-line` attribute, or null if the node sits outside any row. */
const findRow = (node: Node | null): HTMLElement | null => {
  let current: Node | null = node
  while (current) {
    if (current instanceof HTMLElement && current.hasAttribute('data-line')) {
      return current
    }
    current = current.parentNode
  }
  return null
}

/**
 * Character offset from the start of `row` to a DOM position inside it.
 *
 * Measured through a Range, so it counts rendered text: ANSI escapes contribute nothing (they are
 * markup by the time they reach the DOM) and neither does generated content such as a line-number
 * gutter drawn with `::before`. That makes the result directly comparable to an offset into the
 * ANSI-stripped source line.
 */
const charOffset = (row: HTMLElement, node: Node, offset: number): number => {
  const range = document.createRange()
  range.setStart(row, 0)
  range.setEnd(node, offset)
  return range.toString().length
}

/**
 * Resolve the current selection to a {@link CopySlice} over the source lines.
 *
 * Reading the range rather than the anchor and focus means the endpoints already arrive in
 * document order, so there is no selection direction to normalise.
 *
 * An endpoint that does not land inside a row means the selection extends past the rows
 * themselves — which is what a select-all does, since it anchors on the container. That degrades
 * to `'all'` rather than guessing at a boundary.
 *
 * Returns null when there is nothing to copy, leaving the event to the browser.
 */
export function resolveCopySlice(root: HTMLElement): CopySlice | null {
  const selection = document.getSelection()
  if (!selection || selection.isCollapsed || selection.rangeCount === 0) {
    return null
  }
  const range = selection.getRangeAt(0)
  if (!root.contains(range.commonAncestorContainer)) {
    return null
  }

  const startRow = findRow(range.startContainer)
  const endRow = findRow(range.endContainer)
  if (!startRow || !endRow) {
    return 'all'
  }

  const start = Number(startRow.dataset.line)
  const end = Number(endRow.dataset.line)
  if (Number.isNaN(start) || Number.isNaN(end)) {
    return 'all'
  }

  return {
    start: { row: start, col: charOffset(startRow, range.startContainer, range.startOffset) },
    end: { row: end, col: charOffset(endRow, range.endContainer, range.endOffset) }
  }
}
