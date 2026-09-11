/**
 * Tracks whether a scroll container's geometry can be believed.
 *
 * A container behind an inactive tab keeps receiving content, but its viewport is frozen or zero,
 * so the distance to the bottom drifts while nothing is watching. Showing it again flushes that
 * drift as a scroll event which reads as "the user scrolled up" — and for a view whose every
 * recovery path is gated on being anchored, one such reading detaches it for good.
 *
 * None of those readings describe user intent, so they are marked instead of interpreted: geometry
 * is dormant from the moment the container leaves the screen until a few frames after it is back.
 */

/**
 * Frames to ignore scroll geometry for after the container comes back into view.
 *
 * Long enough for the browser to restore the layout and flush the catch-up scroll events that go
 * with it, short enough that a user scrolling immediately afterwards is not swallowed.
 */
const RETURN_HOLD_FRAMES = 4

export type ViewportDormancy = {
  /** True while the container's scroll geometry is not to be read as user intent. */
  readonly dormant: boolean
  /**
   * Declare the geometry trustworthy again, ahead of the hold expiring.
   *
   * For a caller that has just handled an unambiguous user gesture: a gesture can only land on a
   * container that is on screen and is being scrolled deliberately, which is the thing the hold was
   * waiting to find out. That also settles the return itself, so a report still in the pipe is
   * dropped rather than delivered behind the user's back.
   */
  wake(): void
  disconnect(): void
}

/**
 * Watch `node`'s visibility, calling `onReturn` once it is back and the hold has expired.
 *
 * The hold is raised on the way out rather than on the way in: the catch-up scroll event beats the
 * observer's callback, so by the time we learn the view is visible the damage would already be
 * done.
 */
export const observeViewportDormancy = (
  node: HTMLElement,
  onReturn: () => void
): ViewportDormancy => {
  let dormant = false
  let wasHidden = false
  let hold = 0

  const observer = new IntersectionObserver((entries) => {
    const visible = entries[0]?.isIntersecting ?? false
    if (!visible) {
      dormant = true
      wasHidden = true
      return
    }
    if (!wasHidden) {
      return
    }
    wasHidden = false
    const generation = ++hold
    let frames = 0
    const waitForLayout = () => {
      // A later transition, or a `wake`, has taken over: this hold no longer speaks for the view.
      if (generation !== hold) {
        return
      }
      if (++frames < RETURN_HOLD_FRAMES) {
        requestAnimationFrame(waitForLayout)
        return
      }
      dormant = false
      onReturn()
    }
    requestAnimationFrame(waitForLayout)
  })
  observer.observe(node)

  return {
    get dormant() {
      return dormant
    },
    wake: () => {
      hold++
      wasHidden = false
      dormant = false
    },
    disconnect: () => {
      hold++
      observer.disconnect()
    }
  }
}
