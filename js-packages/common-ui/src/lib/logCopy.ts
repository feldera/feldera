import stripAnsi from 'strip-ansi'

/** A copy selection over a virtualised line list: the whole list, or a row/column range
 *  (endpoints from {@link virtualSelect}'s copy interceptor). */
export type CopySlice =
  | 'all'
  | { start: { row: number; col: number }; end: { row: number; col: number } }

/**
 * Build the clipboard text for a {@link CopySlice} over `lines`, ANSI-stripped and joined
 * with `separator` — `'\n'` for rows without a trailing newline, `''` for rows that already
 * carry one (a raw byte stream).
 */
export function sliceLinesForCopy(
  lines: readonly string[],
  slice: CopySlice,
  separator: string
): string {
  if (slice === 'all') return lines.map(stripAnsi).join(separator)
  const result = lines.slice(slice.start.row, slice.end.row + 1).map(stripAnsi)
  result[0] = result[0].slice(slice.start.col)
  result[result.length - 1] = result[result.length - 1].slice(
    0,
    slice.end.col - (slice.start.row === slice.end.row ? slice.start.col : 0)
  )
  return result.join(separator)
}
