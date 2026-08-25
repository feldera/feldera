/**
 * Decides when a user gesture means "I am taking over the scroll".
 *
 * Kept apart from the anchor that consumes it because the two answer different questions. The
 * anchor asks where the bottom is; this asks whether the person at the keyboard wants to be there.
 * Geometry cannot answer the second: a scroll event carries no author, and a virtualiser writes
 * `scrollTop` itself whenever a freshly measured row changes the layout, so reading intent off the
 * offset cancels the very scroll that was correcting it. Input events carry the author by
 * construction, which is why release is driven from here and from nowhere else.
 */

/** Keys that move the view up, and so mean the user is taking over. */
const RELEASE_KEYS = new Set(['ArrowUp', 'PageUp', 'Home'])

/**
 * Downward finger travel that counts as scrolling up rather than as a tap.
 *
 * A tap is not always perfectly still, and releasing on the first pixel of slop would detach a
 * streaming view every time the user touched a row.
 */
const TOUCH_RELEASE_PX = 8

/**
 * Call `onRelease` when a gesture on `node` scrolls the view up. Returns the detach function.
 *
 * Every listener is passive bar the keyboard one: none of them prevent the scroll, they only
 * report it, and saying so up front keeps the wheel and touch handlers off the main thread's
 * critical path.
 */
export const observeReleaseGestures = (node: HTMLElement, onRelease: () => void): (() => void) => {
  const onWheel = (event: WheelEvent) => {
    if (event.deltaY < 0) {
      onRelease()
    }
  }

  const onKeyDown = (event: KeyboardEvent) => {
    if (RELEASE_KEYS.has(event.key)) {
      onRelease()
    }
  }

  /**
   * Topmost point the finger has reached in the current touch, in client coordinates.
   *
   * Downward travel is measured from here rather than from where the touch began, so a drag that
   * reverses is judged from where it turned.
   */
  let touchAnchorY: number | undefined

  const onTouchStart = (event: TouchEvent) => {
    touchAnchorY = event.touches[0]?.clientY
  }

  const onTouchMove = (event: TouchEvent) => {
    const y = event.touches[0]?.clientY
    if (y === undefined) {
      return
    }
    if (touchAnchorY === undefined) {
      touchAnchorY = y
      return
    }
    // A finger dragged down pulls the content down and the view up, which is the same intent as a
    // wheel with a negative delta. Dragging up follows the feed and leaves the anchor alone.
    if (y - touchAnchorY > TOUCH_RELEASE_PX) {
      onRelease()
      return
    }
    touchAnchorY = Math.min(touchAnchorY, y)
  }

  node.addEventListener('wheel', onWheel, { passive: true })
  node.addEventListener('keydown', onKeyDown)
  node.addEventListener('touchstart', onTouchStart, { passive: true })
  node.addEventListener('touchmove', onTouchMove, { passive: true })

  return () => {
    node.removeEventListener('wheel', onWheel)
    node.removeEventListener('keydown', onKeyDown)
    node.removeEventListener('touchstart', onTouchStart)
    node.removeEventListener('touchmove', onTouchMove)
  }
}
