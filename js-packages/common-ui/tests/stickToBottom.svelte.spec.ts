/**
 * Regression tests for the stick-to-bottom scroll container behind the Logs tab, the change stream
 * and the ad-hoc query results (`useStickToBottom`).
 *
 * The first two cases carry over from the helper this replaced: content that grows before the
 * previous scroll event was delivered still has to re-anchor, while a deliberate jump away from
 * the bottom (LogView scrolling to a search match) has to survive.
 *
 * The rest pin the four defects that made the scroll-to-bottom button need several presses:
 * a settle that stopped after a fixed number of passes, geometry that unstuck the view when the
 * virtualiser corrected its own scroll position, a debounced reading behind the button, and a
 * bottom tolerance too tight for fractional device pixel ratios.
 */

import { flushSync } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import StickToBottomFixture from './fixtures/StickToBottomFixture.svelte'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
const frames = async (count: number) => {
  for (let i = 0; i < count; i++) {
    await nextFrame()
  }
}

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mountFixture = (props: { observe?: boolean; deferContent?: boolean } = {}) => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(StickToBottomFixture, { target: mountTarget, props } as any)
  mounted = result
  return {
    component: result.component,
    container: mountTarget.querySelector<HTMLDivElement>('[data-testid=stick-to-bottom-container]')!
  }
}

const distanceFromBottom = (el: HTMLElement) => el.scrollHeight - el.scrollTop - el.clientHeight

/** One-finger touch at `clientY`, which is all the anchor reads. */
const dispatchTouch = (el: HTMLElement, type: 'touchstart' | 'touchmove', clientY: number) => {
  const touch = new Touch({ identifier: 0, target: el, clientY })
  el.dispatchEvent(new TouchEvent(type, { touches: [touch], bubbles: true }))
}

describe('useStickToBottom', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
  })

  it('still scrolls down when the content grew before the last scroll was reported', async () => {
    const { component, container } = mountFixture()
    // Mounting sticks to the bottom of 1000px of content in a 200px box, synchronously.
    expect(container.scrollTop).toBe(800)

    // No wait here on purpose: the scroll event for the mount scroll is still undelivered, so a
    // geometry read sees 800 against the grown content and reports "not the bottom". A live feed
    // outruns its own scroll events the same way, and the view still has to follow it down.
    component.setContentHeight(4000)
    flushSync()
    await expect.poll(() => container.scrollTop).toBe(3800)
  })

  it('does not scroll down after the caller released the view and scrolled elsewhere', async () => {
    const { component, container } = mountFixture()
    // Settle first, so the resize below is racing a stick that is already established.
    await frames(2)
    expect(container.scrollTop).toBe(800)

    component.setContentHeight(4000)
    flushSync()
    await frames(2)

    // What LogView does when a search hits a match: give up the bottom, then scroll to the row.
    component.stickToBottom.release()
    container.scrollTop = 500

    await frames(3)
    expect(container.scrollTop).toBe(500)
  })

  it('reaches the bottom in one press when the content height was under-estimated', async () => {
    const { component, container } = mountFixture()
    await frames(2)

    // 400px of apparent content that is really 6000px, revealed 700px at a time. A settle that
    // stops after a fixed number of passes lands thousands of pixels short of the end.
    component.useEstimatedHeight(400, 6000, 700)
    component.stickToBottom.release()
    container.scrollTop = 0
    await frames(2)

    component.stickToBottom.stick()

    // Polling the distance alone would pass instantly: at the estimated bottom the view really is
    // at the end of the 400px it currently believes in. Convergence is the content reaching its
    // true height with the view still pinned to it.
    await expect.poll(() => component.getReportedHeight(), { timeout: 2000 }).toBe(6000)
    await expect.poll(() => distanceFromBottom(container)).toBeLessThanOrEqual(3)
  })

  it('converges on a moving bottom with no resize observer to re-kick it', async () => {
    // Same under-estimated content, but the resize observer is off. With it on, every growth
    // re-enters `settleToBottom` from the outside, so the loop converges however low its own
    // pass cap is. Off, the cap is the only thing standing between the view and the true bottom.
    const { component, container } = mountFixture({ observe: false })
    component.useEstimatedHeight(400, 6000, 700)
    component.stickToBottom.release()
    container.scrollTop = 0
    await frames(2)

    component.stickToBottom.stick()

    await expect.poll(() => component.getReportedHeight(), { timeout: 2000 }).toBe(6000)
    await expect.poll(() => distanceFromBottom(container)).toBeLessThanOrEqual(3)
  })

  it('stays stuck when the virtualiser corrects scrollTop mid-settle', async () => {
    const { component, container } = mountFixture()
    await frames(2)
    expect(component.stickToBottom.stuck).toBe(true)

    component.setContentHeight(4000)
    flushSync()
    // `stick()` starts the settle synchronously, so the writes below land inside it. Without a
    // settle in flight a scroll event is indistinguishable from the user's, and unsticking is
    // then the correct response rather than the bug.
    component.stickToBottom.stick()

    // What virtua's `$fixScrollJump` does after measuring a freshly mounted row: write scrollTop
    // from outside the user's control. Landing it away from the bottom mid-settle is exactly the
    // state that used to cancel the stick.
    container.scrollTop = 1200
    container.dispatchEvent(new Event('scroll'))

    expect(component.stickToBottom.stuck).toBe(true)
    await expect.poll(() => distanceFromBottom(container)).toBeLessThanOrEqual(3)
    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('releases on a wheel event that scrolls up', async () => {
    const { component, container } = mountFixture()
    await frames(2)
    expect(component.stickToBottom.stuck).toBe(true)

    container.dispatchEvent(new WheelEvent('wheel', { deltaY: -120, bubbles: true }))

    expect(component.stickToBottom.stuck).toBe(false)
  })

  it('stays stuck on a wheel event that scrolls down', async () => {
    const { component, container } = mountFixture()
    await frames(2)
    expect(component.stickToBottom.stuck).toBe(true)

    container.dispatchEvent(new WheelEvent('wheel', { deltaY: 120, bubbles: true }))

    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('anchors to content that only appears after the action mounted', async () => {
    // The only thing that can notice this growth is the content observer: the container never
    // changes size, no row count is being watched, and the fixture opts out of the browser's own
    // scroll anchoring. Resolving `observeElement` once at mount left nothing observed at all.
    const { component, container } = mountFixture({ deferContent: true })
    await frames(2)
    expect(container.scrollTop).toBe(0)

    component.showContent()

    await expect.poll(() => container.scrollTop).toBe(800)
    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('stays stuck when the finger is put down without dragging', async () => {
    const { component, container } = mountFixture()
    await frames(2)
    expect(component.stickToBottom.stuck).toBe(true)

    // A tap on a row is not a scroll. Releasing on `touchstart` alone detached a streaming view
    // every time the user touched it.
    dispatchTouch(container, 'touchstart', 300)
    dispatchTouch(container, 'touchmove', 304)

    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('releases when the finger drags down, which scrolls the view up', async () => {
    const { component, container } = mountFixture()
    await frames(2)

    dispatchTouch(container, 'touchstart', 300)
    dispatchTouch(container, 'touchmove', 340)

    expect(component.stickToBottom.stuck).toBe(false)
  })

  it('stays stuck when the finger drags up, which follows the feed', async () => {
    const { component, container } = mountFixture()
    await frames(2)

    dispatchTouch(container, 'touchstart', 300)
    dispatchTouch(container, 'touchmove', 260)
    dispatchTouch(container, 'touchmove', 220)

    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('releases on a drag that reverses, measured from where it turned', async () => {
    const { component, container } = mountFixture()
    await frames(2)

    // Up 80px then back down 40px. Judged from the touch's origin the finger is still 40px above
    // where it started and nothing would release; judged from the turning point it has dragged
    // down far enough, and the second half of that gesture is a scroll up like any other.
    dispatchTouch(container, 'touchstart', 300)
    dispatchTouch(container, 'touchmove', 220)
    dispatchTouch(container, 'touchmove', 260)

    expect(component.stickToBottom.stuck).toBe(false)
  })

  it('settles again after the content was rebuilt underneath it', async () => {
    // No content observer: LogView watches the line count, not the element, and a rebuild changes
    // neither. With the observer on it re-settles on the replaced element and covers for a
    // `refresh` that does nothing.
    const { component, container } = mountFixture({ observe: false })
    // Long enough for the visibility observer's own settle to have run: it holds a few frames
    // after the container appears, and a rebuild inside that window is put right by it rather than
    // by the call under test.
    await frames(8)
    expect(container.scrollTop).toBe(800)

    // What LogView does when a fresh size cache arrives: the virtualiser is remounted, so the
    // scroll position is gone while the user's intent to follow the end is not.
    component.rebuildContent()
    flushSync()
    expect(container.scrollTop).toBe(0)

    component.stickToBottom.refresh()

    // The offset, not the distance from the bottom: a container whose content has momentarily gone
    // reports a distance of zero at the top, and the assertion would pass on nothing.
    await expect.poll(() => container.scrollTop).toBe(800)
    expect(component.stickToBottom.stuck).toBe(true)
  })

  it('leaves a rebuilt view at the top when the user had scrolled away', async () => {
    const { component, container } = mountFixture({ observe: false })
    await frames(8)

    // `refresh` re-settles, it never re-arms: a reader who scrolled up keeps their position
    // through a rebuild, which is why LogView can call it unconditionally.
    component.stickToBottom.release()
    container.scrollTop = 300
    await frames(2)

    component.rebuildContent()
    flushSync()
    component.stickToBottom.refresh()

    await frames(4)
    expect(container.scrollTop).toBe(0)
    expect(component.stickToBottom.stuck).toBe(false)
  })

  it('keeps `stuck` true at a bottom left a fraction of a pixel short', async () => {
    const { component, container } = mountFixture()
    await frames(2)

    // A fractional device pixel ratio leaves this residual at a visually pinned bottom. Reading
    // it as "not the bottom" strands the FAB on screen with nowhere left to scroll.
    container.scrollTop = container.scrollHeight - container.clientHeight - 1.6
    container.dispatchEvent(new Event('scroll'))

    expect(component.stickToBottom.stuck).toBe(true)
  })
})
