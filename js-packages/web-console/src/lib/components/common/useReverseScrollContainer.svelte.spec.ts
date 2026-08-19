/**
 * Regression tests for the stick-to-bottom scroll container behind the Logs tab, the change stream
 * and the ad-hoc query results (common-ui's `useReverseScrollContainer`).
 *
 * A scroll-to-bottom is queued a frame ahead by the resize observer and runs a second pass a frame
 * after that, so growing content is re-anchored once the virtualiser has measured what it mounted.
 * Both deferred passes run after whatever else happened in between, which is what these two tests
 * pull apart: content that grows before the previous scroll event was delivered still has to
 * re-anchor, while a deliberate jump away from the bottom (LogList scrolling to a search match) has
 * to survive.
 */

import { flushSync } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import ReverseScrollFixture from './ReverseScrollFixture.svelte'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mountFixture = () => {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(ReverseScrollFixture, { target: mountTarget } as any)
  mounted = result
  return {
    component: result.component,
    container: mountTarget.querySelector<HTMLDivElement>('[data-testid=reverse-scroll-container]')!
  }
}

describe('useReverseScrollContainer', () => {
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

    // No wait here on purpose: the scroll event for the mount scroll is still undelivered, so
    // `onscroll` reads 800 against the grown content and reports "not the bottom". A live feed
    // outruns its own scroll events the same way, and the view still has to follow it down.
    component.setContentHeight(4000)
    flushSync()
    await expect.poll(() => container.scrollTop).toBe(3800)
  })

  it('does not scroll down after the caller unstuck the view and scrolled elsewhere', async () => {
    const { component, container } = mountFixture()
    // Settle first: the scroll event for the mount scroll has to be delivered while the content is
    // still 1000px tall, or `onscroll` reports "not the bottom" and the resize below queues no
    // scroll pass at all - leaving nothing for this test to race.
    await nextFrame()
    await nextFrame()
    expect(container.scrollTop).toBe(800)

    component.setContentHeight(4000)
    flushSync()
    // The resize observer is delivered at the end of a frame, after that frame's animation-frame
    // callbacks, so its scroll pass is queued for the next frame. Two frames land us in that next
    // frame with the pass still pending: this callback was registered a frame earlier than the
    // observer's, and a microtask checkpoint runs between the two.
    await nextFrame()
    await nextFrame()

    // What LogList does when a search hits a match: give up the bottom, then scroll to the row.
    component.scroll.stickToBottom = false
    container.scrollTop = 500

    await nextFrame()
    await nextFrame()
    await nextFrame()
    expect(container.scrollTop).toBe(500)
  })
})
