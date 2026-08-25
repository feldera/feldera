/**
 * Resize handling for `LogView`.
 *
 * Wrapped row heights depend on the container width, and the virtualiser only re-measures what is
 * mounted. Without intervention every chunk that had scrolled out keeps the height it had at the
 * old width, and the reported scroll range stays wrong for the rest of the session — measured at
 * 42.8% short on a mixed-length corpus, which puts two fifths of the log out of reach.
 *
 * `LogView` rebuilds the virtualiser on a debounced width change and restores the line that was at
 * the top edge. These tests pin both halves: that the scroll range recovers, and that the reading
 * position survives the rebuild.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import LogViewFixture from './fixtures/LogViewFixture.svelte'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
const frames = async (count: number) => {
  for (let i = 0; i < count; i++) {
    await nextFrame()
  }
}

/** The debounce in LogView is 150ms; clear it with margin. */
const afterResizeSettles = () => new Promise<void>((resolve) => setTimeout(resolve, 400))

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

/**
 * Mixed-length lines, deterministic. Uniform lines would wrap identically and hide the bug; a
 * strictly periodic corpus lets the virtualiser's estimate be right by luck.
 */
let seed = 12345
const nextRandom = () => {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff
  return seed / 0x7fffffff
}
const mixedCorpus = (count: number) => {
  seed = 12345
  return Array.from(
    { length: count },
    (_, i) => `${i}:${'x'.repeat(Math.floor(nextRandom() ** 3 * 1500) + 5)}`
  )
}

const mountFixture = (
  props: { initialLines?: string[]; initialWidth?: number; initialHeight?: number } = {}
) => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(LogViewFixture, { target: mountTarget, props } as any)
  mounted = result
  return {
    component: result.component as any,
    scroll: mountTarget.querySelector<HTMLDivElement>('.log-view-scroll')!
  }
}

/** Walk the whole range so every chunk mounts and measures at the current width. */
const measureEverything = async (scroll: HTMLElement) => {
  let position = 0
  let guard = 0
  while (position < scroll.scrollHeight && guard++ < 4000) {
    scroll.scrollTop = position
    await nextFrame()
    position += scroll.clientHeight * 0.8
  }
  await frames(5)
  return scroll.scrollHeight
}

/**
 * Wait for a rebuild to happen.
 *
 * A rebuild replaces the virtualiser's container element, so a mutation of the scroll container's
 * child list is exactly the event to wait for. Waiting for the view to merely stop moving is no
 * good: before the rebuild starts it is not moving either, so a quiet-period check samples the old
 * position and reports a restore that never happened. A flat frame budget is no good either — how
 * long the debounce plus the rebuild takes depends on the machine.
 *
 * Call it *after* the resize that triggers the rebuild, so the observer is watching in time.
 */
const rebuilt = async (scroll: HTMLElement) => {
  let swapped = false
  const observer = new MutationObserver(() => {
    swapped = true
  })
  observer.observe(scroll, { childList: true })
  for (let frame = 0; frame < 300 && !swapped; frame++) {
    await nextFrame()
  }
  observer.disconnect()
  expect(swapped, 'no rebuild happened').toBe(true)
  await frames(3)
}

/**
 * The topmost line, once it has stopped changing.
 *
 * Jumping the scroll offset lands on an estimate; the virtualiser then mounts what is actually
 * there and corrects, which moves the top edge for a few frames. Reading straight afterwards
 * captures a line the view is about to leave — and when that reading is the baseline a resize is
 * compared against, the test fails on a restore that was perfectly correct.
 */
const stableTopLine = async (scroll: HTMLElement) => {
  let quiet = 0
  let last: number | undefined = -1
  for (let frame = 0; frame < 300 && quiet < 5; frame++) {
    await nextFrame()
    const line = topmostLine(scroll)
    quiet = line === last ? quiet + 1 : 0
    last = line
  }
  return last
}

const topmostLine = (scroll: HTMLElement) => {
  const edge = scroll.getBoundingClientRect().top
  let line: number | undefined
  let highest = Infinity
  for (const row of scroll.querySelectorAll<HTMLElement>('[data-line]')) {
    const rect = row.getBoundingClientRect()
    if (rect.bottom > edge && rect.top < highest) {
      highest = rect.top
      line = Number(row.dataset.line)
    }
  }
  return line
}

describe('LogView resize', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it(
    'recovers the scroll range after a width change instead of keeping stale heights',
    { timeout: 120_000 },
    async () => {
      const { component, scroll } = mountFixture({
        initialLines: mixedCorpus(600),
        initialWidth: 800
      })
      await frames(5)

      // Measure everything at the wide width, so every chunk holds a width-800 height.
      await measureEverything(scroll)
      scroll.scrollTop = 0
      await frames(5)

      component.setWidth(400)
      await afterResizeSettles()
      const afterResize = scroll.scrollHeight

      // Ground truth for the narrow width.
      const truth = await measureEverything(scroll)

      // Without the rebuild this sits ~43% short. The rebuild leaves only the virtualiser's
      // ordinary estimation error, which a fresh mount has too.
      const error = Math.abs(afterResize - truth) / truth
      expect(error).toBeLessThan(0.1)
    }
  )

  /**
   * The cache only ever covers the lines that existed when it was built, and a log view is built
   * before its log arrives. Left at that, the first load of a long log would be almost entirely
   * estimated — the case the prediction is most worth having.
   *
   * Walking the whole range is the oracle: if the prediction were wrong, measuring every chunk
   * would move the reported scroll height.
   */
  it('predicts the scroll range of a log that arrived after mount', { timeout: 120_000 }, async () => {
    const { component, scroll } = mountFixture({ initialLines: [], initialWidth: 800 })
    await frames(3)

    component.append(mixedCorpus(600))
    await afterResizeSettles()
    await frames(10)
    const predicted = scroll.scrollHeight

    const truth = await measureEverything(scroll)
    expect(truth).toBeGreaterThan(10_000)
    expect(Math.abs(predicted - truth) / truth).toBeLessThan(0.01)
  })

  it('keeps the reading position across a width change', { timeout: 120_000 }, async () => {
    const { component, scroll } = mountFixture({
      initialLines: mixedCorpus(600),
      initialWidth: 800
    })
    await frames(5)

    scroll.scrollTop = Math.floor(scroll.scrollHeight * 0.5)
    const before = await stableTopLine(scroll)
    expect(before).toBeGreaterThan(0)

    component.setWidth(400)
    await rebuilt(scroll)

    const after = await stableTopLine(scroll)
    // A couple of lines of slack: the restore lands the captured line at the top edge, but the
    // row above it may still straddle the edge by a pixel once it has re-wrapped.
    expect(Math.abs((after ?? -1) - (before ?? -1))).toBeLessThanOrEqual(2)
  })

  it('rebuilds once for a burst of resizes, not once per step', { timeout: 120_000 }, async () => {
    const { component, scroll } = mountFixture({
      initialLines: mixedCorpus(400),
      initialWidth: 800
    })
    await frames(5)

    scroll.scrollTop = Math.floor(scroll.scrollHeight * 0.4)
    await frames(10)

    // A rebuild replaces the virtualiser's container element, so counting replacements of the
    // scroll container's child counts rebuilds.
    let rebuilds = 0
    const observer = new MutationObserver((records) => {
      for (const record of records) {
        if (record.addedNodes.length > 0) {
          rebuilds++
        }
      }
    })
    observer.observe(scroll, { childList: true })

    // Drag-resize: 40 width changes, one per frame, all inside one debounce window.
    const steps = 40
    for (let step = 1; step <= steps; step++) {
      component.setWidth(800 - step * 10)
      await nextFrame()
    }
    await afterResizeSettles()
    await frames(10)
    observer.disconnect()

    // `offsetWidth`, not `clientWidth`: the container reserves a scrollbar gutter, so the space
    // available to content is narrower than the box it was resized to.
    expect(scroll.offsetWidth).toBe(400)
    expect(scroll.querySelectorAll('[data-line]').length).toBeGreaterThan(0)
    // One rebuild for the whole drag. Undebounced this would be one per step.
    expect(rebuilds).toBeLessThanOrEqual(2)
    expect(rebuilds).toBeLessThan(steps / 2)
  })

  it('holds the scroll range through the rebuild', { timeout: 120_000 }, async () => {
    const { component, scroll } = mountFixture({
      initialLines: mixedCorpus(600),
      initialWidth: 800
    })
    await frames(5)

    scroll.scrollTop = Math.floor(scroll.scrollHeight * 0.6)
    await frames(10)
    const heightBefore = scroll.scrollHeight
    const topBefore = scroll.scrollTop

    // A rebuilt Virtualizer renders nothing until it has observed the scroll element. Sample
    // every frame across the swap: unless the epoch bump starts from a frame callback, the browser
    // paints that empty state, `scrollHeight` collapses to roughly the viewport height and
    // `scrollTop` goes with it. Measured live at 111540 -> 3920 and 111323 -> 3703 — the flick to
    // the top the user sees, which then repeats wherever scrollbars take width because the empty
    // frame removes the scrollbar and that widens `clientWidth` into the next rebuild.
    component.setWidth(400)
    let minHeight = Infinity
    let minTop = Infinity
    for (let frame = 0; frame < 90; frame++) {
      await nextFrame()
      minHeight = Math.min(minHeight, scroll.scrollHeight)
      minTop = Math.min(minTop, scroll.scrollTop)
    }

    expect(minHeight).toBeGreaterThan(heightBefore * 0.5)
    expect(minTop).toBeGreaterThan(topBefore * 0.5)
  })

  /**
   * A rebuild used to flash the head of the log for a single frame.
   *
   * In the field the trigger was the scrollbar: the moment the log overflows, a classic scrollbar
   * takes ~15px out of `clientWidth`, which is indistinguishable from the user resizing the panel.
   * That rebuilt the virtualiser, and a rebuilt virtualiser used to report a scroll height no
   * taller than the viewport for a frame or two before its measurements came back — so it painted
   * at the top and then snapped to the end. Seeding the rebuild with predicted sizes is what
   * removes it: the scroll range is right in the rebuild's first frame, so the view can be put
   * back before anything is painted.
   *
   * The trigger cannot be reproduced here: this harness renders with overlay scrollbars, whose
   * gutter is zero, so the width never changes when content overflows. The consequence can be, and
   * is what these two cover — a width change while anchored must not paint a single frame away
   * from the end, and the container must reserve the gutter so the trigger cannot arise at all.
   */
  describe('rebuilds do not flash', () => {
    /** Where the scrollbar thumb sits: 1 is the bottom, 0 the top. */
    const thumbFraction = (scroll: HTMLElement) => {
      const range = scroll.scrollHeight - scroll.clientHeight
      return range > 0 ? scroll.scrollTop / range : 1
    }

    it('paints no frame away from the end when a width change rebuilds the view', async () => {
      const { component, scroll } = mountFixture({
        initialLines: mixedCorpus(4000),
        initialWidth: 800
      })
      component.stick()
      await frames(30)
      expect(thumbFraction(scroll)).toBeGreaterThan(0.99)

      component.setWidth(785) // the width a classic scrollbar takes
      const painted: number[] = []
      for (let frame = 0; frame < 120; frame++) {
        await nextFrame()
        painted.push(thumbFraction(scroll))
      }

      // Guards the test itself: if nothing was ever painted the assertion below is vacuous.
      expect(painted.length).toBeGreaterThan(20)
      expect(Math.min(...painted)).toBeGreaterThan(0.9)
      expect(thumbFraction(scroll)).toBeGreaterThan(0.99)
    })

    it('reserves the scrollbar gutter so the scrollbar cannot trigger a rebuild', () => {
      const { scroll } = mountFixture({ initialLines: mixedCorpus(200) })
      // Behavioural coverage is impossible under overlay scrollbars, so pin the property that
      // removes the trigger. Without it, a classic scrollbar changes `clientWidth` on overflow.
      expect(getComputedStyle(scroll).scrollbarGutter).toBe('stable')
    })
  })

  /**
   * Shrinking the container moves the bottom away without moving the content, and the browser
   * reports that as a scroll. Read as user intent it unsticks a view that never moved, leaving the
   * log short of the end and no longer following. Anchoring has to mean staying at the end when
   * the panel is resized.
   */
  it('stays anchored when the container is made shorter', async () => {
    const { component, scroll } = mountFixture({
      initialLines: mixedCorpus(3000),
      initialHeight: 400
    })
    component.stick()
    await frames(30)
    expect(scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight).toBeLessThanOrEqual(3)

    component.setHeight(160)
    await frames(40)
    // `isStuck` reads the debounced transition callback, so give it past the 50ms window.
    await new Promise((resolve) => setTimeout(resolve, 120))

    expect(scroll.clientHeight).toBeLessThan(200)
    expect(scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight).toBeLessThanOrEqual(3)
    expect(component.isStuck()).toBe(true)
  })
})
