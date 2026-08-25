/**
 * Search tests for `LogView`.
 *
 * The behaviour under test is the pin: the chunk holding the current match is held in the DOM
 * through `keepMounted` for as long as that match is current. Two things follow from it, and both
 * are asserted here — the highlight can be painted against a node that is certain to exist, and
 * the match survives being scrolled away from.
 *
 * The renderer this replaced virtualised by line and could not pin one, so it re-attempted the
 * paint every frame for up to sixty frames, hoping the scroll would mount the row before it gave
 * up. The first test below is what makes that loop unnecessary: it asserts the row is present as
 * soon as Svelte has flushed, before any scrolling has had a chance to happen.
 */

import { flushSync } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import LogViewFixture from './fixtures/LogViewFixture.svelte'

const HIGHLIGHT = 'feldera-log-view-search'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))

/** Wait until the container's scroll position stops moving, so an in-flight jump cannot undo
 *  whatever the caller does next. Budget exceeds LogView's own alignment window. */
const settleScroll = async (scroll: HTMLElement) => {
  let previous = Number.NaN
  let still = 0
  for (let frame = 0; frame < 90 && still < 5; frame++) {
    await nextFrame()
    still = scroll.scrollTop === previous ? still + 1 : 0
    previous = scroll.scrollTop
  }
}

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mount = (props: Record<string, unknown>) => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(LogViewFixture, { target: mountTarget, props } as any)
  mounted = result
  return {
    component: result.component as any,
    scroll: mountTarget.querySelector<HTMLElement>('.log-view-scroll')!
  }
}

const rowFor = (line: number) => document.querySelector<HTMLElement>(`[data-line="${line}"]`)

const copyText = (scroll: HTMLElement) => {
  const clipboardData = new DataTransfer()
  scroll.dispatchEvent(
    new ClipboardEvent('copy', { clipboardData, bubbles: true, cancelable: true })
  )
  return clipboardData.getData('text/plain')
}

// 5 000 lines, each its own 1-based number. "needle" sits alone on line 4000 — far outside the
// window the virtualiser mounts at the top of a 400px-tall viewport.
const NEEDLE_LINE = 3999
const bigCorpus = () =>
  Array.from({ length: 5000 }, (_, i) => (i === NEEDLE_LINE ? 'needle here' : `line ${i + 1}`))

describe('LogView search', () => {
  afterEach(async () => {
    CSS.highlights.delete(HIGHLIGHT)
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('mounts a match outside the rendered window as soon as the search is submitted', async () => {
    const { component } = mount({ initialLines: bigCorpus() })
    await nextFrame()

    // Precondition: the match is nowhere near the mounted window, so nothing has it in the DOM.
    expect(rowFor(NEEDLE_LINE)).toBeNull()

    component.setSearch({ kind: 'substring', query: 'needle' })
    flushSync()

    // No frames waited, no scrolling completed: the pin alone put the row in the document. This
    // is the property that lets the highlight be painted once instead of retried.
    expect(rowFor(NEEDLE_LINE)).toBeTruthy()
  })

  it('paints the highlight on a match outside the rendered window', async () => {
    const { component } = mount({ initialLines: bigCorpus() })
    await nextFrame()

    component.setSearch({ kind: 'substring', query: 'needle' })
    flushSync()

    expect(CSS.highlights.has(HIGHLIGHT)).toBe(true)
  })

  /**
   * Submitting a search has to move the view to the match, not merely mount it.
   *
   * The jump is arithmetic: the chunk's offset comes from the virtualiser and the line's offset
   * inside it from the predicted layout, so the first assignment already lands close. What this
   * pins is the result, which is the part a reader notices — the match visible, and near the
   * middle rather than clipped against an edge.
   */
  it('brings a match thousands of lines down into view, near the middle', async () => {
    const { component, scroll } = mount({ initialLines: bigCorpus() })
    await nextFrame()
    expect(scroll.scrollTop).toBe(0)

    component.setSearch({ kind: 'substring', query: 'needle' })
    flushSync()
    await settleScroll(scroll)

    const row = rowFor(NEEDLE_LINE)
    expect(row, 'the match never mounted').toBeTruthy()
    const rowRect = row!.getBoundingClientRect()
    const viewRect = scroll.getBoundingClientRect()
    expect(rowRect.top).toBeGreaterThanOrEqual(viewRect.top - 1)
    expect(rowRect.bottom).toBeLessThanOrEqual(viewRect.bottom + 1)

    const offCentre = Math.abs(
      rowRect.top + rowRect.height / 2 - (viewRect.top + viewRect.height / 2)
    )
    expect(offCentre, 'the match landed in view but nowhere near the middle').toBeLessThan(
      viewRect.height * 0.1
    )
  })

  /**
   * The same jump where the arithmetic cannot reach all the way.
   *
   * A chunk holding a line outside the monospace grid is left unpredicted, so the layout can place
   * the chunk but not the line inside it — the jump lands on the top of the chunk, up to fifty
   * lines short. Correcting against the row once it has rendered is what closes that, and this is
   * the case that needs it.
   */
  it('brings a match into view even when its chunk could not be predicted', async () => {
    const lines = bigCorpus()
    lines[NEEDLE_LINE - 20] = '\u6f22\u5b57 unpredictable width'
    const { component, scroll } = mount({ initialLines: lines })
    await nextFrame()

    component.setSearch({ kind: 'substring', query: 'needle' })
    flushSync()
    await settleScroll(scroll)

    const row = rowFor(NEEDLE_LINE)
    expect(row, 'the match never mounted').toBeTruthy()
    const rowRect = row!.getBoundingClientRect()
    const viewRect = scroll.getBoundingClientRect()
    expect(rowRect.top).toBeGreaterThanOrEqual(viewRect.top - 1)
    expect(rowRect.bottom).toBeLessThanOrEqual(viewRect.bottom + 1)
  })

  it('keeps the match mounted after scrolling away from it', async () => {
    const { component, scroll } = mount({ initialLines: bigCorpus() })
    await nextFrame()

    component.setSearch({ kind: 'substring', query: 'needle' })
    flushSync()

    // The jump corrects the row into place over a couple of frames, and it will happily undo a
    // scroll made underneath it. Wait for it to stop moving before scrolling away, or this test
    // passes because the jump put the row back rather than because the pin held it.
    await settleScroll(scroll)

    // Back to the very top — the match is now thousands of lines below the viewport.
    scroll.scrollTop = 0
    for (let i = 0; i < 10; i++) {
      await nextFrame()
    }
    expect(scroll.scrollTop).toBe(0)

    expect(rowFor(NEEDLE_LINE)).toBeTruthy()
  })

  it('reports the match count and clears it when the search is dropped', async () => {
    const lines = ['alpha', 'beta match', 'gamma', 'delta match', 'match']
    const { component } = mount({ initialLines: lines })
    await nextFrame()

    component.setSearch({ kind: 'substring', query: 'match' })
    flushSync()
    expect(component.getMatchCount()).toBe(3)

    component.setSearch(null)
    flushSync()
    expect(component.getMatchCount()).toBe(0)
    expect(CSS.highlights.has(HIGHLIGHT)).toBe(false)
  })

  it('steps to the next occurrence', async () => {
    const lines = Array.from({ length: 300 }, (_, i) => (i % 100 === 50 ? 'match' : `line ${i}`))
    const { component } = mount({ initialLines: lines })
    await nextFrame()

    component.setSearch({ kind: 'substring', query: 'match' }, 0)
    flushSync()
    expect(rowFor(50)).toBeTruthy()

    component.setSearch({ kind: 'substring', query: 'match' }, 1)
    flushSync()
    expect(rowFor(150)).toBeTruthy()

    component.setSearch({ kind: 'substring', query: 'match' }, 2)
    flushSync()
    expect(rowFor(250)).toBeTruthy()
  })
})

describe('LogView line numbers', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('renders a 1-based gutter', async () => {
    mount({ initialLines: ['alpha', 'bravo', 'charlie'], showLineNumbers: true })
    await nextFrame()

    const row = rowFor(0)!
    expect(row.classList.contains('log-view-numbered')).toBe(true)
    // The number is generated content driven by `counter-set`, not a text node.
    expect(row.style.counterSet).toBe('line 1')
    expect(getComputedStyle(row, '::before').content).toBe('counter(line)')
    expect(rowFor(2)!.style.counterSet).toBe('line 3')
  })

  it('keeps the gutter out of copied text', async () => {
    const lines = ['alpha', 'bravo', 'charlie']
    const { scroll } = mount({ initialLines: lines, showLineNumbers: true })
    await nextFrame()

    const textIn = (row: HTMLElement) =>
      document.createTreeWalker(row, NodeFilter.SHOW_TEXT).nextNode()!
    document
      .getSelection()!
      .setBaseAndExtent(textIn(rowFor(0)!), 0, textIn(rowFor(2)!), 'charlie'.length)

    // Were the number a text node it would be prefixed onto each line, and the column offsets the
    // copy resolves would be shifted by its width.
    expect(copyText(scroll)).toBe('alpha\nbravo\ncharlie')
  })
})
