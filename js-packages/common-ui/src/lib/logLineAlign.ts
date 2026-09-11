/**
 * Puts a chosen log line in the middle of the viewport.
 *
 * The offset the caller supplies comes from the size cache and is close but not exact: a chunk's
 * measured height can differ from its prediction — by nothing at all for ordinary log text, by a
 * row for a line the model declined. So the jump is one assignment followed by a short correction
 * against the row itself, once the virtualiser has mounted it.
 *
 * `scrollToIndex` is not used for this. It converges by re-measuring, which takes about a second on
 * an unmeasured list and cannot be interrupted, and it aligns a chunk rather than a line.
 */

/** Frames to wait for the target row to render before giving up on alignment. */
const ALIGN_FRAMES = 10

/** Corrections allowed once the row is there. */
const ALIGN_PASSES = 2

/** Pixels of residual offset that count as centred. */
const ALIGN_EPS = 1

export type LineAligner = {
  /**
   * Scroll `line` to the middle of `container`.
   *
   * @param offset Predicted distance of the line from the top of the scroll content.
   * @param height Predicted height of the line's row.
   */
  centre(container: HTMLElement, line: number, offset: number, height: number): Promise<void>
}

/**
 * An aligner that lets the newest jump win.
 *
 * Only one alignment may be in flight: two search jumps a frame apart would otherwise fight over
 * the same container, and the later one has to win. One aligner per view, so two views never
 * cancel each other.
 */
export const createLineAligner = (): LineAligner => {
  let job = 0

  return {
    centre: async (container, line, offset, height) => {
      const mine = ++job
      container.scrollTop = offset - (container.clientHeight - height) / 2

      let passes = 0
      for (let frame = 0; frame < ALIGN_FRAMES && passes < ALIGN_PASSES; frame++) {
        await new Promise(requestAnimationFrame)
        if (mine !== job || !container.isConnected) {
          return
        }
        const row = container.querySelector<HTMLElement>(`[data-line="${line}"]`)
        if (!row) {
          continue
        }
        const rowRect = row.getBoundingClientRect()
        const viewRect = container.getBoundingClientRect()
        const gap = rowRect.top + rowRect.height / 2 - (viewRect.top + viewRect.height / 2)
        if (Math.abs(gap) < ALIGN_EPS) {
          return
        }
        container.scrollTop += gap
        passes++
      }
    }
  }
}
