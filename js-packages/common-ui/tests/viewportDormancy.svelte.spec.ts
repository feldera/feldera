/**
 * Dormancy tracking behind the shared scroll anchor.
 *
 * A container behind an inactive tab reports a frozen or zero viewport, and showing it again
 * flushes the accumulated drift as a scroll event. Read as user intent, that one event detaches a
 * streaming log for good — every recovery path is gated on the view still being anchored. These
 * cases pin the window in which the geometry is refused, and the two ways it closes.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { observeViewportDormancy, type ViewportDormancy } from '$lib/viewportDormancy'

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()))
const frames = async (count: number) => {
  for (let i = 0; i < count; i++) {
    await nextFrame()
  }
}

/**
 * Frames the geometry stays refused after the container is back.
 *
 * Duplicated from the module, which keeps it private: the number is the behaviour under test, so
 * a test that read it from there could not fail when it changed.
 */
const RETURN_HOLD_FRAMES = 4

/** Past the observer's own delivery plus the return hold. */
const settled = () => frames(12)

let host: HTMLDivElement | undefined
let node: HTMLDivElement | undefined
let dormancy: ViewportDormancy | undefined
let returns = 0

const mount = () => {
  returns = 0
  host = document.createElement('div')
  node = document.createElement('div')
  node.style.height = '100px'
  host.appendChild(node)
  document.body.appendChild(host)
  dormancy = observeViewportDormancy(node, () => {
    returns++
  })
  return dormancy
}

/** Hide or show the container the way switching tabs does: by taking it out of layout. */
const setHidden = (hidden: boolean) => {
  host!.style.display = hidden ? 'none' : 'block'
}

describe('observeViewportDormancy', () => {
  afterEach(() => {
    dormancy?.disconnect()
    dormancy = undefined
    host?.remove()
    host = undefined
    node = undefined
  })

  it('trusts the geometry of a container that has never been away', async () => {
    const tracker = mount()
    await settled()

    expect(tracker.dormant).toBe(false)
    // The first intersection report is not a return. Treating it as one would settle every view
    // to the bottom the moment it mounted, whatever the caller asked for.
    expect(returns).toBe(0)
  })

  it('refuses the geometry the moment the container goes away', async () => {
    const tracker = mount()
    await settled()

    setHidden(true)
    // Raised on the way out, not on the way in: the catch-up scroll event beats the observer's
    // callback, so waiting until the view is back is already too late.
    await expect.poll(() => tracker.dormant).toBe(true)
  })

  it('holds the refusal past the frame the container came back on', async () => {
    const tracker = mount()
    await settled()
    setHidden(true)
    await expect.poll(() => tracker.dormant).toBe(true)

    // Count the frames the refusal survives. The observer's own report costs at least one of them
    // and the hold is four more, so anything short of four means the hold was dropped and the
    // catch-up scroll events of the restored layout are being read as the user scrolling up.
    setHidden(false)
    let held = 0
    while (tracker.dormant && held < 60) {
      await nextFrame()
      held++
    }

    expect(held).toBeGreaterThanOrEqual(RETURN_HOLD_FRAMES)
    expect(tracker.dormant).toBe(false)
    expect(returns).toBe(1)
  })

  it('trusts the geometry again at once when the caller says the user is driving', async () => {
    const tracker = mount()
    await settled()
    setHidden(true)
    await expect.poll(() => tracker.dormant).toBe(true)

    setHidden(false)
    await frames(1)
    expect(tracker.dormant).toBe(true)

    // A user gesture proves the container is on screen and being scrolled deliberately, which is
    // what the hold was waiting to find out.
    tracker.wake()
    expect(tracker.dormant).toBe(false)

    // And the hold it pre-empted must not fire afterwards: re-settling a view the user has just
    // taken over is the bug this cancels.
    await settled()
    expect(returns).toBe(0)
  })

  it('reports nothing more once disconnected', async () => {
    const tracker = mount()
    await settled()
    setHidden(true)
    await expect.poll(() => tracker.dormant).toBe(true)

    tracker.disconnect()
    setHidden(false)
    await settled()

    expect(returns).toBe(0)
  })
})
