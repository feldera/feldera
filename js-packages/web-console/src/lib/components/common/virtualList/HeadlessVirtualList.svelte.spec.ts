/**
 * Coverage of the rendered window in `HeadlessVirtualList`.
 *
 * The list renders a fixed count of rows around the scroll offset, so the only thing that can go
 * wrong is arithmetic: too few rows and the viewport has a blank strip along one edge, which reads
 * as the list failing to keep up rather than as an off-by-one.
 */

import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import HeadlessVirtualListFixture from './HeadlessVirtualListFixture.svelte'

const ITEM_SIZE = 28
const VIEWPORT_HEIGHT = 400

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))

describe('HeadlessVirtualList', () => {
  it('renders rows over the whole viewport at every scroll offset', async () => {
    const target = document.createElement('div')
    document.body.appendChild(target)
    const { unmount } = render(HeadlessVirtualListFixture, {
      target,
      props: { itemCount: 500, itemSize: ITEM_SIZE, viewportHeight: VIEWPORT_HEIGHT }
    } as any)
    try {
      const viewport = target.querySelector<HTMLDivElement>('[data-testid=virtual-list-viewport]')!
      await nextFrame()

      // Two full row heights of offsets, so the sweep covers the rounding period twice over. A
      // window one row short only leaves a gap at some offsets within it.
      const gaps: string[] = []
      for (let scrollTop = 0; scrollTop <= 2 * ITEM_SIZE; scrollTop += 2) {
        viewport.scrollTop = scrollTop
        await nextFrame()
        await nextFrame()

        const rows = [...viewport.querySelectorAll<HTMLElement>('[data-index]')]
        const rects = rows.map((row) => row.getBoundingClientRect())
        const view = viewport.getBoundingClientRect()
        const top = Math.min(...rects.map((r) => r.top))
        const bottom = Math.max(...rects.map((r) => r.bottom))
        if (top > view.top + 0.5 || bottom < view.bottom - 0.5) {
          gaps.push(
            `at ${scrollTop}px: rows span ${top.toFixed(1)}..${bottom.toFixed(1)}, ` +
              `viewport ${view.top.toFixed(1)}..${view.bottom.toFixed(1)}`
          )
        }
      }
      // Guards the sweep itself: with nothing rendered every comparison above is vacuous.
      expect(viewport.querySelectorAll('[data-index]').length).toBeGreaterThan(
        VIEWPORT_HEIGHT / ITEM_SIZE
      )
      expect(gaps).toEqual([])
    } finally {
      await unmount()
      target.remove()
    }
  })
})
