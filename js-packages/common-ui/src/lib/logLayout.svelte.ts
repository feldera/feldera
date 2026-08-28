/**
 * Owns the virtualiser's size cache: when to predict a fresh one, and how to put the reading
 * position back once the rebuild it forces has landed.
 *
 * Row heights depend on the container width, because the text wraps, and a virtualiser only
 * measures what it mounts. A chunk that has scrolled out keeps the height it had at the old width
 * and nothing ever revisits it: measured on a 600-line log of mixed-length lines, halving the width
 * left the reported scroll range 42.8% short of the truth, so two fifths of the log had become
 * unreachable.
 *
 * Monospace rows make those heights arithmetic rather than a measurement, so the virtualiser is
 * handed the whole set as a `CacheSnapshot` instead of being left to discover them — see
 * `logRowMetrics`. virtua reads a snapshot once, when its layout is constructed, and copies it; it
 * exposes no way to replace one in place. A fresh prediction therefore means a fresh virtualiser,
 * which is what the caller's `{#key}` block is for. What the rebuild buys is that it starts life
 * knowing every offset, which is what lets the reading position be put back with a single
 * assignment rather than a convergence loop.
 *
 * Two things make a cache stale, and both go through the same reseed:
 *
 * - the grid changes, when the container is resized or the line-number gutter is toggled;
 * - the log outgrows what the cache covers, which is what the first load of a long log looks like:
 *   the view mounts before its first line arrives.
 */

import { untrack } from 'svelte'
import type { VirtualizerHandle } from 'virtua/svelte'
import type { LogChunk } from './logChunks'
import {
  type LogLayout,
  measureRowMetrics,
  predictLogLayout,
  type RowMetrics,
  sameRowMetrics
} from './logRowMetrics'
import type { SelectionPin } from './selectionPin.svelte'
import type { StickToBottom } from './stickToBottom.svelte'

/** How long the container must hold a width, and the log a length, before rebuilding. */
const RESEED_SETTLE_MS = 150

/** Growth in the line count that earns a fresh prediction. */
const RESEED_GROWTH = 1.1

export type LogLayoutOptions = {
  /** The scroll container. Read reactively, so the prediction starts as soon as it is bound. */
  container: () => HTMLElement | undefined
  /** The chunks to predict. Read reactively: a change is a log that has grown or been evicted. */
  chunks: () => readonly LogChunk[]
  /** Whether the line-number gutter is on, which narrows the grid. Read reactively. */
  showLineNumbers: () => boolean
  /**
   * Handle of the virtualiser currently mounted around {@link LogLayoutState.current}.
   *
   * Read reactively, because `bind:this` is assigned from an effect of its own: the fresh handle
   * can arrive a step after the layout that caused it, and the restore needs both.
   */
  virtualizer: () => VirtualizerHandle | undefined
  /** The bottom anchor. Consulted before a rebuild and re-settled after one. */
  anchor: StickToBottom
  /** The selection. Rebuilds wait for it, and releasing it is a reason to try again. */
  selection: SelectionPin
}

export type LogLayoutState = {
  /**
   * The prediction the virtualiser is to be seeded with, or undefined before the container has
   * been measured. Rebuild the virtualiser whenever this changes identity.
   */
  readonly current: LogLayout | undefined
}

/** Total lines the chunks cover. The trailing chunk may be partial, so it is not a multiple. */
const lineCountOf = (chunks: readonly LogChunk[]) => {
  const last = chunks[chunks.length - 1]
  return last ? last.startLine + last.lines.length : 0
}

/**
 * Predict and re-predict the chunk sizes for a log view, restoring the view after each rebuild.
 *
 * Must be called during component init: it installs effects of its own.
 */
export const useLogLayout = (options: LogLayoutOptions): LogLayoutState => {
  const { container, chunks, showLineNumbers, virtualizer, anchor, selection } = options

  let layout = $state<LogLayout | undefined>()

  /** What the current layout was predicted for, or undefined before the first prediction. */
  let seeded: { grid: RowMetrics; lineCount: number } | undefined

  /** Line to put back at the top edge once the rebuild has landed. */
  let lineToRestore: number | undefined

  /** Line number of the first row still visible at the top edge. */
  const topmostVisibleLine = (element: HTMLElement) => {
    const edge = element.getBoundingClientRect().top
    let line: number | undefined
    let highest = Infinity
    for (const row of element.querySelectorAll<HTMLElement>('[data-line]')) {
      const rect = row.getBoundingClientRect()
      if (rect.bottom > edge && rect.top < highest) {
        highest = rect.top
        line = Number(row.dataset.line)
      }
    }
    return line
  }

  const reseed = () => {
    const element = container()
    if (!element) {
      return
    }
    // A rebuild replaces every row, which collapses whatever the user had selected. Nothing is
    // urgent enough to be worth that: the cache stays as it is until the selection clears, and
    // the effect below tries again the moment it does.
    if (selection.isHeld()) {
      return
    }
    const grid = measureRowMetrics(element, showLineNumbers())
    // A container behind an inactive tab has no width to measure. Predicting against that would
    // throw away the grid in use and reset the scroll position, so a tab switch must not look
    // like a resize.
    if (!grid) {
      return
    }
    const current = chunks()
    const lineCount = lineCountOf(current)
    // With the grid unchanged, the only reason left to rebuild is a log that has outgrown its
    // cache. The next append schedules another attempt, so nothing is lost by waiting.
    const outgrown = lineCount >= (seeded?.lineCount ?? 0) * RESEED_GROWTH
    if (seeded && sameRowMetrics(grid, seeded.grid) && !outgrown) {
      return
    }
    // The rebuild drops the scroll position, so the line at the top edge is captured to be put
    // back. Sticking wins over restoring a line: the bottom is where the view belongs.
    lineToRestore = seeded && !anchor.stuck ? topmostVisibleLine(element) : undefined
    seeded = { grid, lineCount }
    layout = predictLogLayout(current, grid)
  }

  let reseedTimer: ReturnType<typeof setTimeout> | undefined
  const scheduleReseed = () => {
    clearTimeout(reseedTimer)
    reseedTimer = setTimeout(() => untrack(reseed), RESEED_SETTLE_MS)
  }

  // Predict as soon as there is a container to measure, then re-check whenever it is resized.
  // Debounced, because a drag-resize would otherwise rebuild on every frame of the drag.
  $effect(() => {
    const element = container()
    if (!element) {
      return
    }
    untrack(reseed)

    // A web font arriving after mount changes the width of a cell without changing the width of
    // the container, so nothing else reports it: measured here, the fallback font gave 81 columns
    // where the loaded one gives 78, a 4% error in every offset. Scheduled rather than applied at
    // once, since it is a rebuild like any other and a rebuild takes every row out of the DOM for
    // a frame. Inside a running app the fonts are long since loaded, the grid comes out the same
    // and nothing is rebuilt at all; the cost falls only on a cold load straight into a log.
    let alive = true
    void document.fonts.ready.then(() => {
      if (alive) {
        scheduleReseed()
      }
    })

    // A width change re-wraps every line, so the prediction is stale. Re-anchoring after the
    // resize is the shared anchor's job, not this observer's.
    const observer = new ResizeObserver(() => scheduleReseed())
    observer.observe(element)
    return () => {
      alive = false
      clearTimeout(reseedTimer)
      observer.disconnect()
    }
  })

  // A log that has stopped arriving is worth predicting in full: a cache only ever covers what
  // existed when it was built, and a streaming view is built before its first line. Releasing a
  // selection is the other moment worth re-checking, since a rebuild held back for one is still
  // owed.
  $effect(() => {
    void chunks()
    void showLineNumbers()
    void selection.pinned.length
    void selection.selectAll
    scheduleReseed()
  })

  // Runs after the caller's `{#key}` block has rebuilt the virtualiser around the new cache, which
  // drops the scroll position: either the bottom is re-taken or the reading position is put back.
  $effect(() => {
    const current = layout
    void virtualizer()
    if (!current) {
      return
    }
    const line = lineToRestore
    lineToRestore = undefined
    // Untracked: `stuck` transitions as the anchor works, and tracking it would re-enter here on
    // every one of those transitions.
    if (untrack(() => anchor.stuck)) {
      anchor.refresh()
      return
    }
    const element = container()
    if (line !== undefined && element) {
      // One assignment, ahead of the paint. The rebuilt virtualiser was seeded with this very
      // layout, so its offsets are the layout's offsets — and it sizes its content from an inline
      // style in its first render rather than from an effect, so the range to scroll within is
      // already there by the time this runs. Nothing to converge on, nothing to hide.
      element.scrollTop = current.offsetOfLine(line)
    }
  })

  return {
    get current() {
      return layout
    }
  }
}
