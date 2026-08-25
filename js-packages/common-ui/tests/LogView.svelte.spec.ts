/**
 * Rendering and scroll-anchoring tests for `LogView`.
 *
 * Covers the four features that do not involve selection: chunked virtual rendering, word wrap on
 * unbroken runs, ANSI colour and style, and a stick-to-bottom that lands in one pass. Selection
 * and copy live in `LogViewSelection.svelte.spec.ts`.
 */

import { flushSync } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import LogViewFixture from './fixtures/LogViewFixture.svelte'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
const frames = async (count: number) => {
  for (let i = 0; i < count; i++) {
    await nextFrame()
  }
}

/**
 * Wait for the debounced stick-to-bottom report to reach `expected`.
 *
 * `onStickToBottomChange` deliberately lags the raw value by 50ms so hosts are not asked to
 * pause and resume a feed on every flicker, which is longer than a couple of frames.
 */
const stuckSettles = async (read: () => boolean, expected: boolean) => {
  await expect.poll(read, { timeout: 1000, interval: 20 }).toBe(expected)
}

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mountFixture = (props: { initialLines?: string[]; streaming?: boolean } = {}) => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(LogViewFixture, { target: mountTarget, props } as any)
  mounted = result
  const scroll = mountTarget.querySelector<HTMLDivElement>('.log-view-scroll')!
  return { component: result.component, scroll }
}

const distanceFromBottom = (el: HTMLElement) => el.scrollHeight - el.scrollTop - el.clientHeight
const rows = (scroll: HTMLElement) => scroll.querySelectorAll('[data-line]')

const numbered = (count: number, prefix = 'line') =>
  Array.from({ length: count }, (_, i) => `${prefix} ${i}`)

describe('LogView rendering', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('renders only a fraction of a large log', async () => {
    const { scroll } = mountFixture({ initialLines: numbered(100_000) })
    await frames(3)

    expect(rows(scroll).length).toBeGreaterThan(0)
    // A 400px viewport over 100k lines: anything near the full set means virtualisation is off.
    expect(rows(scroll).length).toBeLessThan(2000)
    expect(scroll.textContent).toContain('line 0')
  })

  it('sweeps to the true bottom of a large log', async () => {
    const { scroll } = mountFixture({ initialLines: numbered(100_000) })
    await frames(3)

    // Walk down in viewport-sized steps, the way a real scroll does, so every chunk gets measured
    // on the way rather than jumping straight to an estimated offset.
    for (let i = 0; i < 400; i++) {
      scroll.scrollTop += scroll.clientHeight * 4
      await nextFrame()
      if (distanceFromBottom(scroll) <= 3) {
        break
      }
    }
    scroll.scrollTop = scroll.scrollHeight
    await frames(4)

    expect(distanceFromBottom(scroll)).toBeLessThanOrEqual(3)
    expect(scroll.textContent).toContain('line 99999')
    // The viewport is never left blank at rest.
    expect(rows(scroll).length).toBeGreaterThan(0)
  })

  it('wraps an unbroken run instead of overflowing', async () => {
    const long = 'x'.repeat(2000)
    const { scroll } = mountFixture({ initialLines: ['short', long, 'short'] })
    await frames(3)

    const row = [...rows(scroll)].find((el) => el.textContent === long) as HTMLElement
    expect(row).toBeDefined()

    const lineHeight = Number.parseFloat(getComputedStyle(row).lineHeight)
    // 2000 characters cannot fit one 800px line, so a wrapping row is several line-heights tall.
    expect(row.offsetHeight).toBeGreaterThan(lineHeight * 5)
    // And the page itself must not gain a horizontal scroll.
    expect(scroll.scrollWidth).toBeLessThanOrEqual(scroll.clientWidth + 1)
  })

  it('renders ANSI colour and style', async () => {
    const { scroll } = mountFixture({
      initialLines: [
        '[31mred16[0m',
        '[38;5;208m256colour[0m',
        '[38;2;12;34;56mtruecolour[0m',
        '[1mbold[0m',
        '[3mitalic[0m',
        '[4munderline[0m'
      ]
    })
    await frames(3)

    // No escape sequences survive into the rendered text.
    expect(scroll.textContent).not.toContain('')
    expect(scroll.textContent).toContain('truecolour')

    const styled = (text: string) =>
      [...scroll.querySelectorAll<HTMLElement>('[data-line] span')].find(
        (el) => el.textContent === text
      )

    const colourOf = (text: string) => {
      const el = styled(text)
      expect(el, `no span for ${text}`).toBeDefined()
      return getComputedStyle(el!)
    }

    // Each colour mode has to resolve to an actual paint, not the inherited default.
    const inherited = getComputedStyle(scroll).color
    expect(colourOf('red16').color).not.toBe(inherited)
    expect(colourOf('256colour').color).not.toBe(inherited)
    expect(colourOf('truecolour').color).toBe('rgb(12, 34, 56)')

    expect(Number(colourOf('bold').fontWeight)).toBeGreaterThan(400)
    expect(colourOf('italic').fontStyle).toBe('italic')
    expect(colourOf('underline').textDecorationLine).toContain('underline')
  })
})

describe('LogView stick-to-bottom', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('stays at the bottom across a stream of appends', async () => {
    const { component, scroll } = mountFixture({
      initialLines: numbered(200),
      streaming: true
    })
    await frames(4)
    expect(distanceFromBottom(scroll)).toBeLessThanOrEqual(3)

    // The loop is the test. A single append converges by luck; twenty in a row do not.
    for (let batch = 0; batch < 20; batch++) {
      component.append(numbered(50, `batch ${batch}`))
      flushSync()
      await frames(3)
      expect(distanceFromBottom(scroll), `after batch ${batch}`).toBeLessThanOrEqual(3)
    }
    expect(scroll.textContent).toContain('batch 19 49')
  })

  it('leaves the view alone once the user has scrolled up', async () => {
    const { component, scroll } = mountFixture({
      initialLines: numbered(2000),
      streaming: true
    })
    await frames(4)

    // A wheel event up is what tells the container the user has taken over.
    scroll.dispatchEvent(new WheelEvent('wheel', { deltaY: -200, bubbles: true }))
    scroll.scrollTop -= 200
    await frames(2)
    const offset = scroll.scrollTop

    component.append(numbered(1000, 'appended'))
    flushSync()
    await frames(4)

    expect(scroll.scrollTop).toBe(offset)
    await stuckSettles(() => component.isStuck(), false)
  })

  it('re-arms when the user returns to the bottom', async () => {
    const { component, scroll } = mountFixture({
      initialLines: numbered(2000),
      streaming: true
    })
    await frames(4)

    scroll.dispatchEvent(new WheelEvent('wheel', { deltaY: -200, bubbles: true }))
    scroll.scrollTop -= 200
    await stuckSettles(() => component.isStuck(), false)

    scroll.scrollTop = scroll.scrollHeight
    await frames(4)

    expect(distanceFromBottom(scroll)).toBeLessThanOrEqual(3)
    await stuckSettles(() => component.isStuck(), true)
  })

  /**
   * Hosts pause and resume their feed on the reported value, so the report has to arrive while the
   * user is still scrolling, not once they stop.
   *
   * The report is debounced to keep a converging anchor from reporting its intermediate offsets.
   * Restarting that debounce on every scroll event — and every scroll event asks — postpones a
   * genuine transition for as long as the scrolling lasts, which leaves the feed running long
   * after the reader has left the end.
   */
  it('reports the user scrolling away without waiting for them to stop', async () => {
    const { component, scroll } = mountFixture({
      initialLines: numbered(5000),
      streaming: true
    })
    await frames(4)
    await stuckSettles(() => component.isStuck(), true)

    scroll.dispatchEvent(new WheelEvent('wheel', { deltaY: -200, bubbles: true }))
    // Twenty frames of continuous scrolling, well past the 50ms the report is debounced by.
    for (let i = 0; i < 20; i++) {
      scroll.scrollTop -= 20
      scroll.dispatchEvent(new Event('scroll'))
      await nextFrame()
    }

    expect(component.isStuck()).toBe(false)
  })

  it('settles to the bottom from the top of a large log in one press', async () => {
    // Rows of wildly different heights, so the virtualiser's estimate for everything it has not
    // measured is badly wrong and the anchor has a moving target to converge on.
    const lines = Array.from({ length: 20_000 }, (_, i) =>
      i % 7 === 0 ? `line ${i} ${'x'.repeat(900)}` : `line ${i}`
    )
    const { component, scroll } = mountFixture({ initialLines: lines, streaming: true })
    await frames(6)

    scroll.dispatchEvent(new WheelEvent('wheel', { deltaY: -300, bubbles: true }))
    scroll.scrollTop = 0
    await frames(3)
    expect(scroll.scrollTop).toBe(0)

    // One call, the way one press of the button is one call. Reaching the true bottom must not
    // require a second.
    component.stick()
    await frames(4)

    expect(distanceFromBottom(scroll)).toBeLessThanOrEqual(3)
    expect(scroll.textContent).toContain('line 19999')
  })

  /**
   * A streaming log must never be seen anywhere but at its end.
   *
   * The lines have to arrive *after* mount for this to mean anything. Passing them as a mount prop
   * gives the virtualiser its content and its anchor in the same pass, which is not how a live
   * feed behaves and hides the problem completely.
   *
   * The renderer this replaced showed line 1 of a hundred-thousand-line log for a frame and then
   * snapped to the end, because a virtualiser with nothing measured reports a scroll range no
   * taller than its viewport and there is no end to anchor to yet. Seeding it with predicted sizes
   * is what removes that: the range is right in the first frame, so the anchor has somewhere to go
   * before anything is painted. Every frame is checked, not just the ones after some settling
   * period — a single bad one is what a reader notices.
   */
  it('never paints a frame away from the end while a streaming log loads', async () => {
    const { component, scroll } = mountFixture({ initialLines: [], streaming: true })
    await frames(3)

    component.append(numbered(20000))

    const painted: number[] = []
    for (let frame = 0; frame < 120; frame++) {
      await nextFrame()
      const range = scroll.scrollHeight - scroll.clientHeight
      // A range of zero means there is nothing to be wrong about yet.
      if (range > 0) {
        painted.push(scroll.scrollTop / range)
      }
    }

    // Guards the test itself: with nothing painted the assertion below would be vacuous.
    expect(painted.length).toBeGreaterThan(20)
    expect(Math.min(...painted)).toBeGreaterThan(0.9)
  })

  /**
   * The counterpart: a log too short to scroll has no end to anchor to, so anything that waits for
   * one has to recognise that there is nothing to wait for.
   */
  it('renders a log too short to scroll straight away', async () => {
    const { component, scroll } = mountFixture({ initialLines: [], streaming: true })
    await frames(3)

    component.append(numbered(3))
    await frames(3)

    expect(scroll.scrollHeight).toBeLessThanOrEqual(scroll.clientHeight)
    expect(rows(scroll).length).toBe(3)
    expect(scroll.textContent).toContain('line 2')
  })
})
