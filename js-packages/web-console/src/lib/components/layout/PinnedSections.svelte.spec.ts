/**
 * `PinnedSections`: the wrapper shows no more than its peek until the content above it
 * has scrolled past, and a pointer click on a pinned section scrolls that section into
 * view rather than activating a control inside it.
 *
 * Needs the browser project, because every assertion is a layout measurement and the
 * behaviour under test is `position: sticky`.
 *
 * The fixture is a scroll container with a tall filler above the wrapper, standing in
 * for the pipelines table on the home page.
 */

import { createRawSnippet } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import PinnedSections, { pinnedPeekHeightPixels } from './PinnedSections.svelte'

const SCREEN = 400
const FILLER = 1000
const TALL_SECTION = 300
const SHORT_SECTION = 100

/**
 * A pointer click.
 *
 * `HTMLElement.click()` reports `detail` 0, which the component treats as keyboard
 * activation. Playwright's click would report 1 but scrolls the target into view
 * first, which undoes the state under test.
 */
const pointerClick = (element: HTMLElement) =>
  element.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail: 1 }))

/** Keyboard activation, as Enter on a focused control produces it. */
const keyboardClick = (element: HTMLElement) =>
  element.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail: 0 }))

/** A section too tall to fit in the lower half of the screen. */
const tallSection = createRawSnippet(() => ({
  render: () =>
    `<div data-testid="section-tall" style="height: ${TALL_SECTION}px">` +
    `<button data-testid="btn-in-tall">In the tall section</button></div>`
}))

let mounted: { unmount: () => Promise<void> } | undefined
let scroller: HTMLElement | undefined

/**
 * A scroll container holding a filler, the wrapper with one tall section, and a short
 * second section appended to the wrapper. The spacer below the wrapper leaves a short
 * section room to reach the middle of the screen.
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

  const wrapper = scroller.querySelector<HTMLElement>('[data-testid=box-pinned-sections]')!
  const short = document.createElement('div')
  short.dataset.testid = 'section-short'
  short.style.height = `${SHORT_SECTION}px`
  short.append(Object.assign(document.createElement('button'), { textContent: 'In the short one' }))
  wrapper.append(short)

  const below = document.createElement('div')
  below.style.height = `${SCREEN}px`
  wrapper.after(below)

  // `stickyBottom` comes from the measured height, so it is set a frame after the
  // sections are in place.
  await expect.poll(() => wrapper.style.bottom).not.toBe('')
  await new Promise((settled) => requestAnimationFrame(() => requestAnimationFrame(settled)))
  return {
    wrapper,
    tall: wrapper.querySelector<HTMLElement>('[data-testid=section-tall]')!,
    short,
    button: wrapper.querySelector<HTMLElement>('[data-testid=btn-in-tall]')!
  }
}

/** How far in px the top of `element` is below the top of the screen. */
const topOnScreen = (element: HTMLElement) =>
  element.getBoundingClientRect().top - scroller!.getBoundingClientRect().top

describe('PinnedSections.svelte', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    scroller?.remove()
    scroller = undefined
  })

  it('shows no more of the wrapper than the peek', async () => {
    const { wrapper } = await renderPinned()

    expect(scroller!.scrollTop).toBe(0)
    expect(topOnScreen(wrapper)).toBeCloseTo(SCREEN - pinnedPeekHeightPixels, 0)
    expect(wrapper.getBoundingClientRect().bottom).toBeGreaterThan(
      scroller!.getBoundingClientRect().bottom
    )
  })

  it('brings the first section up when clicked, and the click goes no further', async () => {
    const { tall, button } = await renderPinned()
    const clicked = vi.fn()
    button.addEventListener('click', clicked)

    pointerClick(button)

    // The section is taller than half the screen, so it stops where all of it shows
    // rather than at the middle.
    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN - TALL_SECTION)
    expect(clicked).not.toHaveBeenCalled()
  })

  it('stops the top of a short first section at the middle of the screen', async () => {
    const { wrapper, tall } = await renderPinned()
    const stickyBefore = wrapper.style.bottom
    tall.style.height = `${SHORT_SECTION}px`
    await expect.poll(() => wrapper.style.bottom).not.toBe(stickyBefore)

    pointerClick(wrapper)

    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN / 2)
  })

  it('lets a click through once the section is in view', async () => {
    const { tall, button } = await renderPinned()
    const clicked = vi.fn((e: MouseEvent) => e.defaultPrevented)
    button.addEventListener('click', clicked)

    pointerClick(button)
    await expect.poll(() => Math.round(topOnScreen(tall))).toBe(SCREEN - TALL_SECTION)

    pointerClick(button)

    // All of the section shows now, so the click reaches the button.
    expect(clicked).toHaveBeenCalledOnce()
    expect(clicked).toHaveReturnedWith(false)
  })

  it('leaves keyboard activation alone while pinned', async () => {
    const { button } = await renderPinned()
    const clicked = vi.fn((e: MouseEvent) => e.defaultPrevented)
    button.addEventListener('click', clicked)
    const scrollTo = vi.spyOn(scroller!, 'scrollTo')

    keyboardClick(button)

    // Nothing scrolls, and the click reaches the button.
    expect(scrollTo).not.toHaveBeenCalled()
    expect(clicked).toHaveBeenCalledOnce()
    expect(clicked).toHaveReturnedWith(false)
  })
})
