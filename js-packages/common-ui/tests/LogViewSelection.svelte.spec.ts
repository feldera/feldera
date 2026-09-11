/**
 * Selection and copy tests for `LogView`.
 *
 * The two mechanisms under test are independent and both are needed. Chunks the selection touches
 * are pinned into the DOM, so a drag that scrolls does not destroy what it already covered; and
 * the copy handler rebuilds the text from the source lines, so a selection whose middle was never
 * mounted — which every select-all is — still copies in full.
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

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mountFixture = (initialLines: string[]) => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(LogViewFixture, { target: mountTarget, props: { initialLines } } as any)
  mounted = result
  return { scroll: mountTarget.querySelector<HTMLDivElement>('.log-view-scroll')! }
}

const rowFor = (scroll: HTMLElement, line: number) =>
  scroll.querySelector<HTMLElement>(`[data-line="${line}"]`)

/** First text node inside a row — ANSI styling wraps runs in spans, plain lines do not. */
const textIn = (row: HTMLElement) =>
  document.createTreeWalker(row, NodeFilter.SHOW_TEXT).nextNode() as Text

/** Select from `startLine`:`startCol` to `endLine`:`endCol`. Both rows must be mounted. */
const selectRange = (
  scroll: HTMLElement,
  startLine: number,
  startCol: number,
  endLine: number,
  endCol: number
) => {
  const start = rowFor(scroll, startLine)
  const end = rowFor(scroll, endLine)
  expect(start, `row ${startLine} is not mounted`).toBeTruthy()
  expect(end, `row ${endLine} is not mounted`).toBeTruthy()
  const selection = document.getSelection()!
  selection.setBaseAndExtent(textIn(start!), startCol, textIn(end!), endCol)
}

const copyText = (scroll: HTMLElement) => {
  const clipboardData = new DataTransfer()
  scroll.dispatchEvent(
    new ClipboardEvent('copy', { clipboardData, bubbles: true, cancelable: true })
  )
  return clipboardData.getData('text/plain')
}

const numbered = (count: number) => Array.from({ length: count }, (_, i) => `line ${i}`)

describe('LogView selection', () => {
  afterEach(async () => {
    document.getSelection()?.removeAllRanges()
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('keeps rows in the DOM while scrolling far away from them', async () => {
    const { scroll } = mountFixture(numbered(50_000))
    await frames(3)

    selectRange(scroll, 0, 0, 5, 6)
    await frames(2)

    scroll.scrollTop = 400_000
    await frames(4)

    // Without pinning, virtua unmounts these the moment they leave the buffer and the selection
    // collapses with them.
    expect(rowFor(scroll, 0), 'row 0 was unmounted').toBeTruthy()
    expect(document.getSelection()!.isCollapsed).toBe(false)
  })

  it('copies a 500-line selection after scrolling 20k lines away', async () => {
    const total = 50_000
    const { scroll } = mountFixture(numbered(total))
    await frames(3)

    // Anchor on row 0 and let the pin register before scrolling, so the node survives being
    // scrolled past. Holding the reference is the point: re-querying it later would only find it
    // if it were still mounted, which is the very thing under test.
    selectRange(scroll, 0, 0, 1, 0)
    await frames(2)
    const anchor = textIn(rowFor(scroll, 0)!)

    // Extend downwards the way a drag with auto-scroll does, giving `selectionchange` a frame to
    // pin each newly touched chunk.
    for (let line = 20; line <= 500; line += 20) {
      scroll.scrollTop = (line / total) * scroll.scrollHeight
      await nextFrame()
      const row = rowFor(scroll, line)
      if (!row) {
        continue
      }
      document.getSelection()!.setBaseAndExtent(anchor, 0, textIn(row), 0)
      await nextFrame()
    }

    const lastRow = rowFor(scroll, 500)
    expect(lastRow, 'row 500 never mounted during the drag').toBeTruthy()
    document
      .getSelection()!
      .setBaseAndExtent(anchor, 0, textIn(lastRow!), lastRow!.textContent!.length)
    await frames(2)

    scroll.scrollTop = 400_000
    await frames(4)

    const copied = copyText(scroll).split('\n')
    expect(copied).toHaveLength(501)
    expect(copied[0]).toBe('line 0')
    expect(copied[250]).toBe('line 250')
    expect(copied[500]).toBe('line 500')
  })

  it('snaps a selection back when the drag runs out of the log', async () => {
    // `user-select: contain` expresses this in CSS, but only Firefox implements it, so the scroll
    // container carries the `selectScope` action instead. Without either, a drag that leaves the
    // log keeps going and takes the surrounding page with it.
    const { scroll } = mountFixture(numbered(200))
    await frames(4)

    const outside = document.createElement('p')
    outside.textContent = 'chrome around the log'
    document.body.appendChild(outside)
    try {
      // The action only acts for the scope that owns the focus, which is what stops a selection
      // made elsewhere on the page from being dragged into this log.
      scroll.focus()
      const row = rowFor(scroll, 3)!
      const selection = document.getSelection()!
      selection.setBaseAndExtent(textIn(row), 0, outside.firstChild!, 5)
      expect(scroll.contains(selection.getRangeAt(0).commonAncestorContainer)).toBe(false)

      // `selectionchange` is delivered as a task, so the snap lands a tick later.
      await frames(2)

      const snapped = document.getSelection()!.getRangeAt(0)
      expect(scroll.contains(snapped.commonAncestorContainer)).toBe(true)
      expect(snapped.toString()).not.toContain('chrome around the log')
      // A snapped selection spans the rows rather than sitting inside one, which the copy path
      // reads as the whole log — the same degradation a select-all gets.
      expect(copyText(scroll)).toBe(numbered(200).join('\n'))
    } finally {
      outside.remove()
    }
  })

  it('copies every line of a select-all, including rows never mounted', async () => {
    const lines = numbered(5000)
    const { scroll } = mountFixture(lines)
    await frames(3)

    // What Ctrl+A produces inside `user-select: contain`: endpoints on the container, not on rows.
    document.getSelection()!.selectAllChildren(scroll)
    await frames(2)

    expect(copyText(scroll)).toBe(lines.join('\n'))
  })

  it('respects column boundaries on a partial selection', async () => {
    const { scroll } = mountFixture(['alpha bravo', 'charlie delta', 'echo foxtrot'])
    await frames(3)

    selectRange(scroll, 0, 6, 2, 4)
    await frames(2)

    expect(copyText(scroll)).toBe('bravo\ncharlie delta\necho')
  })

  it('respects column boundaries within a single row', async () => {
    const { scroll } = mountFixture(['alpha bravo charlie'])
    await frames(3)

    selectRange(scroll, 0, 6, 0, 11)
    await frames(2)

    expect(copyText(scroll)).toBe('bravo')
  })

  it('copies styled lines without escape sequences', async () => {
    // Built from an explicit escape rather than a literal control character, which does not
    // reliably survive a round trip through a source file.
    const esc = '\u001b'
    const { scroll } = mountFixture([`${esc}[31mred line${esc}[0m`, `${esc}[1mbold line${esc}[0m`])
    await frames(3)

    // The rendered DOM carries no escapes, so a copy rebuilt from the source must strip them too.
    expect(scroll.textContent).not.toContain(esc)

    document.getSelection()!.selectAllChildren(scroll)
    await frames(2)

    const copied = copyText(scroll)
    expect(copied).toBe('red line\nbold line')
    expect(copied).not.toContain(esc)
  })

  it('leaves rows pointer-interactive throughout a sustained scroll', async () => {
    const { scroll } = mountFixture(numbered(50_000))
    await frames(3)

    const inner = scroll.firstElementChild as HTMLElement

    // virtua sets `pointer-events: none` inline on this container while a scroll is in flight, and
    // holds it for 150ms after the last scroll event. That is what stops a drag-selection from
    // tracking rows once it reaches the viewport edge and starts auto-scrolling. A single sample
    // proves nothing — the flag has to be caught while it is actually raised, so sample across a
    // continuous scroll and require every reading to stay interactive.
    const computed = new Set<string>()
    const inline = new Set<string>()
    for (let i = 0; i < 30; i++) {
      scroll.scrollTop += 300
      computed.add(getComputedStyle(inner).pointerEvents)
      inline.add(inner.style.pointerEvents)
      await nextFrame()
    }

    // Guards the test itself: if virtua stopped setting the inline flag, the assertion below would
    // pass for the wrong reason and quietly stop covering anything.
    expect([...inline], 'virtua never disabled pointer events; the test is not exercising it')
      .toContain('none')
    expect([...computed]).toEqual(['auto'])
  })

  describe('select-all', () => {
    /** Ctrl+A as the browser delivers it: keydown first, then the selection it produces. */
    const pressSelectAll = (scroll: HTMLElement) => {
      scroll.focus()
      document.dispatchEvent(
        new KeyboardEvent('keydown', { key: 'a', ctrlKey: true, bubbles: true })
      )
      document.getSelection()!.selectAllChildren(scroll)
    }

    it('pins no chunk, so scrolling away still unmounts', async () => {
      const lines = numbered(5000)
      const { scroll } = mountFixture(lines)
      await frames(4)

      // Chunk 0 is mounted and visible at the moment of the Ctrl+A, so a selection that pinned
      // what it touched would hold it in the DOM forever. That makes its absence after scrolling
      // the discriminator, and it does not depend on whether the browser fires selectionchange
      // during a scroll — which it need not.
      expect(scroll.querySelector('[data-chunk="0"]')).toBeTruthy()

      pressSelectAll(scroll)
      await frames(4)

      scroll.scrollTop = scroll.scrollHeight / 2
      await frames(10)

      expect(
        scroll.querySelector('[data-chunk="0"]'),
        'chunk 0 was pinned by the select-all; it should not have been'
      ).toBeNull()
      // The whole log is 100 chunks. Only the viewport's worth should be resident.
      expect(scroll.querySelectorAll('[data-chunk]').length).toBeLessThan(20)
    })

    it('copies every line even though nothing was pinned', async () => {
      const lines = numbered(5000)
      const { scroll } = mountFixture(lines)
      await frames(4)

      pressSelectAll(scroll)
      await frames(4)

      expect(copyText(scroll)).toBe(lines.join('\n'))
    })

    it('still copies every line after scrolling the selection out of the DOM', async () => {
      const lines = numbered(5000)
      const { scroll } = mountFixture(lines)
      await frames(4)

      pressSelectAll(scroll)
      await frames(4)

      // Scrolling unmounts the rows the endpoints sat on, which in some browsers collapses the
      // range. The copy must survive that, because the source array never went anywhere.
      scroll.scrollTop = scroll.scrollHeight
      await frames(10)

      expect(copyText(scroll)).toBe(lines.join('\n'))
    })

    it('copies the whole log when a select-all is anchored on rows rather than the container', async () => {
      const lines = numbered(5000)
      const { scroll } = mountFixture(lines)
      await frames(4)

      scroll.focus()
      document.dispatchEvent(
        new KeyboardEvent('keydown', { key: 'a', ctrlKey: true, bubbles: true })
      )

      // Chromium anchors a select-all on the container, which the copy path already reads as the
      // whole log. Not every engine does: normalising the range to the outermost text nodes is
      // equally valid, and with nothing pinned those are the mounted rows only — so resolving the
      // endpoints would silently truncate. Reproduce that shape directly, since the test browser
      // will not produce it.
      const rows = scroll.querySelectorAll<HTMLElement>('[data-line]')
      const last = textIn(rows[rows.length - 1])
      document.getSelection()!.setBaseAndExtent(textIn(rows[0]), 0, last, last.length)
      await frames(4)

      expect(copyText(scroll)).toBe(lines.join('\n'))
    })

    it('resumes normal pinning after a pointer press ends the select-all', async () => {
      const lines = numbered(5000)
      const { scroll } = mountFixture(lines)
      await frames(4)

      pressSelectAll(scroll)
      await frames(4)

      // A pointer press begins a fresh selection; a drag after it must pin again, or the
      // select-all case would have permanently disabled selection survival.
      document.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true }))
      selectRange(scroll, 2, 0, 12, 4)
      await frames(4)

      const pinnedRow = rowFor(scroll, 2)
      expect(pinnedRow).toBeTruthy()

      scroll.scrollTop = scroll.scrollHeight / 2
      await frames(10)

      expect(rowFor(scroll, 2), 'the drag selection was not pinned').toBeTruthy()
      expect(copyText(scroll).split('\n').length).toBe(11)
    })
  })
})
