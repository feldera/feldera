/**
 * Tests for `Popup`'s bindable `open` prop.
 *
 * The trigger is no longer the only thing that can open the popup: the support bundle
 * dropdown reopens itself once the user has chosen a file, because clicking the hidden
 * `<input type=file>` closed it. These tests pin both directions of that binding, and
 * that a click outside still closes the popup whichever of the two opened it.
 */

import { flushSync } from 'svelte'
import { afterEach, describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import PopupFixture from './PopupFixture.svelte'

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const mountFixture = () => {
  // A real element in the document, because the popup closes on a window click and
  // "outside the popup" has to mean something.
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const result = render(PopupFixture, { target: mountTarget } as any)
  mounted = result
  return result.component as unknown as {
    openFromOutside: () => void
    isOpen: () => boolean
  }
}

const find = (testid: string) => mountTarget!.querySelector<HTMLElement>(`[data-testid=${testid}]`)

afterEach(async () => {
  await mounted?.unmount()
  mounted = undefined
  mountTarget?.remove()
  mountTarget = undefined
})

describe('Popup.svelte', () => {
  it('starts closed and opens on the trigger', async () => {
    mountFixture()
    expect(find('box-popup-content')).toBe(null)

    find('btn-popup-trigger')!.click()

    await expect.poll(() => find('box-popup-content')).toBeTruthy()
    // The trigger is told whether the popup is open, so it can label itself.
    expect(find('btn-popup-trigger')!.textContent!.trim()).toBe('Close')
  })

  it('opens from the binding, without a click on the trigger', async () => {
    const fixture = mountFixture()

    fixture.openFromOutside()
    flushSync()

    await expect.poll(() => find('box-popup-content')).toBeTruthy()
  })

  it('reports back through the binding when the content closes itself', async () => {
    const fixture = mountFixture()
    fixture.openFromOutside()
    flushSync()
    await expect.poll(() => find('box-popup-content')).toBeTruthy()

    find('btn-popup-close')!.click()

    await expect.poll(() => fixture.isOpen()).toBe(false)
    expect(find('box-popup-content')).toBe(null)
  })

  it('reports back through the binding when a click lands outside', async () => {
    const fixture = mountFixture()
    fixture.openFromOutside()
    flushSync()
    await expect.poll(() => find('box-popup-content')).toBeTruthy()

    find('btn-outside')!.click()

    await expect.poll(() => fixture.isOpen()).toBe(false)
    expect(find('box-popup-content')).toBe(null)
  })

  it('stays open for a click on its own content', async () => {
    const fixture = mountFixture()
    fixture.openFromOutside()
    flushSync()
    await expect.poll(() => find('box-popup-content')).toBeTruthy()

    find('box-popup-content')!.click()

    // The close runs on a timer, so give it the turn of the event loop it would use.
    await new Promise((resolve) => setTimeout(resolve))
    expect(fixture.isOpen()).toBe(true)
    expect(find('box-popup-content')).toBeTruthy()
  })
})
