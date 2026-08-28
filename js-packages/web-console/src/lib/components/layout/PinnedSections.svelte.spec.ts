/**
 * Sections pinned over the bottom of a scrolling page: the group shows its peek
 * until the content above it has been scrolled through, and a click on a section
 * that is still parked there brings that section into view instead of reaching
 * what the section holds.
 *
 * The fixture is a scroll container with a tall filler above the group, which is
 * what the pipelines table is on the home page.
 */

import { createRawSnippet } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import PinnedSections, { pinnedSectionsPeekPixels } from './PinnedSections.svelte'

const SCREEN = 400
const FILLER = 1000
const TALL_SECTION = 300
const SHORT_SECTION = 100

/** One section, tall enough to overflow what is left below the middle of the screen. */
const tallSection = createRawSnippet(() => ({
  render: () =>
    `<div data-testid="section-tall" style="height: ${TALL_SECTION}px">` +
    `<button data-testid="btn-in-tall">In the tall section</button></div>`
}))

let mounted: { unmount: () => Promise<void> } | undefined
let scroller: HTMLElement | undefined

/**
 * A scroll container holding a filler, the group with one tall section, and a
 * second, short section appended to the group. Everything below the group is what
 * gives the short section room to reach the middle of the screen.
 */
const renderPinned = async () => {
  scroller = document.createElement('div')
  scroller.style.cssText = `height: ${SCREEN}px; overflow-y: auto`
  document.body.appendChild(scroller)
  mounted = render(PinnedSections, {
    target: scroller,
    props: { children: tallSection }
  }) as any

  const filler = document.createElement('div')
  filler.style.height = `${FILLER}px`
  scroller.prepend(filler)

  const group = scroller.querySelector<HTMLElement>('[data-testid=box-pinned-sections]')!
  const short = document.createElement('div')
  short.dataset.testid = 'section-short'
  short.style.height = `${SHORT_SECTION}px`
  short.append(Object.assign(document.createElement('button'), { textContent: 'In the short one' }))
  group.append(short)

  const below = document.createElement('div')
  below.style.height = `${SCREEN}px`
  group.after(below)

  // The pin offset is derived from the measured group, so it settles a frame after
  // the sections are in place.
  await expect.poll(() => group.style.bottom).not.toBe('')
  await new Promise((settled) => requestAnimationFrame(() => requestAnimationFrame(settled)))
  return {
    group,
    tall: group.querySelector<HTMLElement>('[data-testid=section-tall]')!,
    short,
    button: group.querySelector<HTMLElement>('[data-testid=btn-in-tall]')!
  }
}

/** How far below the top of the screen an element sits. */
const topOnScreen = (element: HTMLElement) =>
  element.getBoundingClientRect().top - scroller!.getBoundingClientRect().top

describe('PinnedSections.svelte', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    scroller?.remove()
    scroller = undefined
  })

  it('shows no more of the group than the peek', async () => {
    const { group } = await renderPinned()

    expect(scroller!.scrollTop).toBe(0)
    expect(topOnScreen(group)).toBeCloseTo(SCREEN - pinnedSectionsPeekPixels, 0)
    expect(group.getBoundingClientRect().bottom).toBeGreaterThan(
      scroller!.getBoundingClientRect().bottom
    )
  })

  it('brings the first section up when clicked, and the click goes no further', async () => {
    const { tall, button } = await renderPinned()
    const clicked = vi.fn()
    button.addEventListener('click', clicked)

    button.click()

    // The first section is taller than half the screen, so it comes up far enough to
    // show all of it. The sections behind it are no part of that measurement.
    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN - TALL_SECTION)
    expect(clicked).not.toHaveBeenCalled()
  })

  it('stops the top of a short first section at the middle of the screen', async () => {
    const { group, tall } = await renderPinned()
    const stickyBefore = group.style.bottom
    tall.style.height = `${SHORT_SECTION}px`
    await expect.poll(() => group.style.bottom).not.toBe(stickyBefore)

    group.click()

    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN / 2)
  })

  it('lets a click through once the section is in view', async () => {
    const { tall, button } = await renderPinned()
    const clicked = vi.fn((e: MouseEvent) => e.defaultPrevented)
    button.addEventListener('click', clicked)

    button.click()
    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN - TALL_SECTION)

    button.click()

    // The section is up, so nothing stands between the pointer and its contents.
    expect(clicked).toHaveBeenCalledOnce()
    expect(clicked).toHaveReturnedWith(false)
  })
})
