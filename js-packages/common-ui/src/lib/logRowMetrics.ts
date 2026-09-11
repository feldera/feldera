/**
 * Predicts how tall each log chunk will render, so the virtualiser can be seeded with sizes
 * instead of discovering them by measurement.
 *
 * A virtual list caches a size per item and only ever measures what it mounts. Everything it has
 * not reached is a guess, and every guess is wrong in the same direction, so the scroll range and
 * every offset derived from it stay wrong until the whole log has been walked. Worse, the guesses
 * are keyed by position and never invalidated: after the container changes width, chunks that had
 * scrolled out keep the heights they were measured at.
 *
 * Log rows are monospace, so their wrapped height is arithmetic rather than a measurement. The
 * container gives a whole number of character cells per row, and the number of rows a line needs
 * follows from where it is allowed to break.
 *
 * Seeding the cache is what makes the virtualiser's own machinery usable. `scrollToIndex`
 * converges by re-measuring, so on an unmeasured list it takes about a second and cannot be
 * interrupted; against a seeded cache it lands in a frame. Everything the renderer no longer has
 * to do for itself — settling to the bottom, restoring a reading position, withholding the first
 * paint — follows from the offsets being right straight away.
 *
 * The model covers what log output is made of: printable ASCII and tabs, with preserved
 * whitespace, hanging trailing spaces, tab stops, a break after a hyphen, and an overlong token
 * broken at the edge. It deliberately declines anything else — see {@link isPredictable}.
 */

import stripAnsi from 'strip-ansi'
import { CHUNK_SIZE, chunkIndexOfLine, type LogChunk } from './logChunks'

/** Matches the `tab-size` the rows are rendered with. */
export const TAB_SIZE = 8

/** The virtualiser's own marker for "this item has not been measured". */
export const UNMEASURED = -1

const SPACE = 0x20
const TAB = 0x09
const HYPHEN = 0x2d
const ESCAPE = '\u001B'
const LAST_PRINTABLE_ASCII = 0x7e

/** Long enough that per-glyph sub-pixel rounding averages out of the measured cell width. */
const RULER = '0'.repeat(64)

/** The character grid a log row wraps on, read from the rendered container. */
export type RowMetrics = {
  /** Character cells available on one visual row. */
  columns: number
  /** Height of one visual row in pixels. */
  lineHeight: number
}

/**
 * True when `code` occupies exactly one cell of the grid.
 *
 * The grid is a property of the monospace font, and it only describes the glyphs that font has.
 * Anything else is laid out by a fallback font whose advance bears no fixed relation to the cell:
 * measured here, a run of CJK ideographs came to roughly 1.8 cells apiece, so both "one cell" and
 * "two cells" mispredict it, and by different amounts at different widths. Control characters
 * other than tab are no more predictable.
 */
const isGridCharacter = (code: number): boolean =>
  (code >= SPACE && code <= LAST_PRINTABLE_ASCII) || code === TAB

/**
 * True when every character of `text` sits on the grid, so its height can be predicted.
 *
 * A line that steps outside it is left unpredicted rather than guessed at, and the virtualiser
 * measures its chunk the way it always has. Log output is overwhelmingly printable ASCII, so that
 * costs a chunk here and there rather than the benefit of the cache.
 */
export const isPredictable = (text: string): boolean => {
  for (let index = 0; index < text.length; index++) {
    if (!isGridCharacter(text.charCodeAt(index))) {
      return false
    }
  }
  return true
}

/**
 * Visual rows `text` occupies on a grid `columns` cells wide, or undefined when it steps off the
 * grid — see {@link isPredictable}, whose rule this applies as it goes so a line is walked once.
 *
 * Greedy, the way CSS line breaking is: each unbreakable run goes on the current row if it fits,
 * moves to the next row if it fits there, and is broken at the edge only when it fits nowhere —
 * which is what `overflow-wrap: break-word` asks for, and why a wrapped hash stays readable.
 */
export const wrappedRowCount = (text: string, columns: number): number | undefined => {
  if (columns < 1) {
    return 1
  }
  let rows = 1
  let column = 0
  let index = 0
  while (index < text.length) {
    const code = text.charCodeAt(index)
    // Preserved whitespace advances the cursor but never wraps by itself: under `pre-wrap` a run
    // of spaces that reaches the edge hangs past it, and only the next word moves down. That is
    // why a line padded with two hundred trailing spaces still renders as a single row.
    if (code === SPACE || code === TAB) {
      column = code === TAB ? column + TAB_SIZE - (column % TAB_SIZE) : column + 1
      index++
      continue
    }

    // One unbreakable run: up to and including the character that offers the next break.
    const start = index
    while (index < text.length) {
      const character = text.charCodeAt(index)
      if (character === SPACE || character === TAB) {
        break
      }
      if (!isGridCharacter(character)) {
        return undefined
      }
      index++
      // A line may break after a hyphen. No other punctuation offers one — a slash or a dot in a
      // path does not, which is exactly why `overflow-wrap` has to break the token instead.
      if (character === HYPHEN) {
        break
      }
    }
    const width = index - start

    if (column + width <= columns) {
      column += width
    } else if (width <= columns) {
      rows++
      column = width
    } else {
      // Too long for any row: it starts on a row of its own and is broken at the edge.
      if (column > 0) {
        rows++
        column = 0
      }
      const brokenRows = Math.ceil(width / columns) - 1
      rows += brokenRows
      column = width - brokenRows * columns
    }
  }
  return rows
}

/**
 * Predicted height of one rendered log line, or undefined when the line steps outside the grid.
 *
 * An empty line renders no content, so it has no line box and no height, exactly as the DOM
 * produces. A line holding only escape sequences still renders a box, so it counts as one row.
 */
export const predictedLineHeight = (line: string, metrics: RowMetrics): number | undefined => {
  if (line === '') {
    return 0
  }
  // Escape sequences are markup by the time they reach the DOM, so they occupy no cells. The
  // strip is skipped for the overwhelming majority of lines that carry none.
  const text = line.includes(ESCAPE) ? stripAnsi(line) : line
  const rows = wrappedRowCount(text, metrics.columns)
  return rows === undefined ? undefined : rows * metrics.lineHeight
}

/** Predicted heights and offsets for a chunked log at one character grid. */
export type LogLayout = {
  metrics: RowMetrics
  /**
   * Height of each chunk in `chunks` order, or {@link UNMEASURED} for a chunk holding a line the
   * model declines. This is the size array of a virtua `CacheSnapshot`, which uses the same marker.
   */
  sizes: number[]
  /**
   * Size the virtualiser should assume for a chunk this layout says nothing about — one that was
   * declined, or one appended after the cache was built.
   */
  defaultSize: number
  /**
   * Pixel offset of `line` from the top of the scroll content.
   *
   * Valid for as long as the virtualiser's own sizes are still the ones this layout seeded it
   * with, which is exactly the moment after a rebuild. Once chunks have mounted and been measured
   * the virtualiser's offsets are the better source; ask it for the chunk and add
   * {@link LogLayout.offsetWithinChunk}.
   */
  offsetOfLine: (line: number) => number
  /**
   * Pixel offset of `line` from the top of the chunk that holds it, or 0 when that chunk was not
   * predicted — which puts the top of the chunk at the viewport edge instead of the exact line.
   */
  offsetWithinChunk: (line: number) => number
  /** Predicted height of the row for `line`, falling back to one row when it is not predicted. */
  heightOfLine: (line: number) => number
}

/**
 * Predict the height of every chunk on the grid `metrics` describes.
 *
 * Walks each line once. On a hundred-thousand-line log that is a few tens of milliseconds, paid on
 * mount and again only when a resize changes the number of columns.
 */
export const predictLogLayout = (chunks: readonly LogChunk[], metrics: RowMetrics): LogLayout => {
  let predictedTotal = 0
  let predictedChunks = 0
  const sizes = chunks.map((chunk) => {
    let height = 0
    for (const line of chunk.lines) {
      const lineHeight = predictedLineHeight(line, metrics)
      if (lineHeight === undefined) {
        return UNMEASURED
      }
      height += lineHeight
    }
    predictedTotal += height
    predictedChunks++
    return height
  })

  // A chunk of ordinary single-row lines, which is a far better prior than the virtualiser's own
  // 40px until it has measured enough to estimate for itself.
  const defaultSize =
    predictedChunks > 0 ? predictedTotal / predictedChunks : CHUNK_SIZE * metrics.lineHeight

  // Where each chunk starts, so an offset is a lookup rather than a walk. The virtualiser derives
  // its own offsets the same way from the same numbers, so the two agree until it measures.
  const offsets = new Array<number>(sizes.length + 1)
  offsets[0] = 0
  for (let index = 0; index < sizes.length; index++) {
    const size = sizes[index]
    offsets[index + 1] = offsets[index] + (size === UNMEASURED ? defaultSize : size)
  }

  const offsetWithinChunk = (line: number) => {
    const index = chunkIndexOfLine(chunks, line)
    if (index < 0 || sizes[index] === UNMEASURED) {
      return 0
    }
    const chunk = chunks[index]
    let offset = 0
    for (let i = chunk.startLine; i < line; i++) {
      offset += predictedLineHeight(chunk.lines[i - chunk.startLine], metrics) ?? 0
    }
    return offset
  }

  const heightOfLine = (line: number) => {
    const index = chunkIndexOfLine(chunks, line)
    if (index < 0) {
      return metrics.lineHeight
    }
    const chunk = chunks[index]
    return predictedLineHeight(chunk.lines[line - chunk.startLine], metrics) ?? metrics.lineHeight
  }

  const offsetOfLine = (line: number) => {
    const index = chunkIndexOfLine(chunks, line)
    return index < 0 ? 0 : offsets[index] + offsetWithinChunk(line)
  }

  return { metrics, sizes, defaultSize, offsetOfLine, offsetWithinChunk, heightOfLine }
}

/**
 * Read the character grid the rows are actually rendered on.
 *
 * Measured rather than derived from the stylesheet: the font stack falls back differently per
 * platform, so the cell width is only knowable from a rendered glyph. A `white-space: pre` run of
 * identical characters divided by its length gives the cell width free of sub-pixel rounding, and
 * the probe row's own height is the line height whatever the CSS says it is.
 *
 * Returns undefined when nothing can be measured. A container behind an inactive tab has no width,
 * and predicting against that would throw away the grid still in use.
 */
export const measureRowMetrics = (
  container: HTMLElement,
  numbered: boolean
): RowMetrics | undefined => {
  const probe = document.createElement('div')
  probe.className = numbered ? 'log-view-row log-view-numbered' : 'log-view-row'
  probe.setAttribute('aria-hidden', 'true')
  probe.style.visibility = 'hidden'
  const ruler = document.createElement('span')
  ruler.style.whiteSpace = 'pre'
  ruler.textContent = RULER
  probe.appendChild(ruler)

  // Appended and removed inside one synchronous block, so no frame is ever laid out with it in
  // place and no ResizeObserver ever sees it. Appended last, so it cannot become the first child
  // that anything else observes.
  container.appendChild(probe)
  const advance = ruler.getBoundingClientRect().width / RULER.length
  const lineHeight = probe.getBoundingClientRect().height
  const style = getComputedStyle(probe)
  const textWidth =
    probe.clientWidth - Number.parseFloat(style.paddingLeft) - Number.parseFloat(style.paddingRight)
  probe.remove()

  if (!(advance > 0) || !(lineHeight > 0) || !(textWidth > 0)) {
    return undefined
  }
  // A hair of tolerance: the browser fits a run of cells whose total width rounds to the content
  // width, and the division would otherwise drop the last cell to floating-point error.
  const columns = Math.floor((textWidth + 0.05) / advance)
  return columns >= 1 ? { columns, lineHeight } : undefined
}

/** True when two grids wrap every line identically, so a rebuild would change nothing. */
export const sameRowMetrics = (a: RowMetrics | undefined, b: RowMetrics | undefined): boolean =>
  a !== undefined && b !== undefined && a.columns === b.columns && a.lineHeight === b.lineHeight
