<script lang="ts" module>
  /** How many pixels of the wrapper stay on screen while it is pinned. */
  export const pinnedPeekHeightPixels = 44
</script>

<script lang="ts">
  /**
   * Wraps the trailing sections of a scrolling page and holds them at the bottom of
   * the screen while the content above them scrolls. "Pinned" below means held there
   * by `position: sticky`.
   *
   * While the wrapper is pinned, `pinnedPeekHeightPixels` of it shows and the rest
   * sits below the bottom edge of the scroller, so the content above keeps all but
   * that strip of the screen. Once that content has scrolled past, the wrapper scrolls
   * up with it.
   *
   * Each direct child is one section. The wrapper measures its own height, so callers
   * add and remove sections freely.
   *
   * Clicking the pinned wrapper scrolls its first section into view instead of
   * activating a control inside it; see `onclickcapture` below.
   */
  import type { Snippet } from '$lib/types/svelte'

  const {
    children,
    class: className
  }: {
    children: Snippet
    /** Applied to the wrapper: its width, and the gap between sections. */
    class?: string
  } = $props()

  let wrapperHeight = $state(0)
  let wrapperNode = $state<HTMLElement>()

  // Return the value of CSS property `bottom` for the wrapper, in px. Negative, so the wrapper
  // hangs below the scroller's bottom edge with only the peek of its content on screen.
  // `undefined` until the height has been measured.
  const stickyBottom = $derived.by(() => {
    if (!wrapperHeight) {
      return undefined
    }
    // A wrapper shorter than the peek needs no offset and shows whole.
    const peek = Math.min(wrapperHeight, pinnedPeekHeightPixels)
    return `${peek - wrapperHeight}px`
  })

  /** Nearest ancestor of `node` - the wrapper - that scrolls vertically, or the document's scroller. */
  const scrollParentOf = (node: HTMLElement): HTMLElement | null => {
    for (let parent = node.parentElement; parent; parent = parent.parentElement) {
      const overflowY = getComputedStyle(parent).overflowY
      // `hidden` makes a scroll container too: the user cannot drag it, but `scrollTo`
      // still moves it. `clip` does not, and is left out.
      if (overflowY === 'auto' || overflowY === 'scroll' || overflowY === 'hidden') {
        return parent
      }
    }
    // The page may hand scrolling back to the document, and a click on the peek still
    // has to scroll something.
    return document.scrollingElement as HTMLElement | null
  }

  /**
   * Distance in px from the top of `scroller`'s content to the top of `section`, as if
   * the wrapper carried no `stickyBottom` offset.
   */
  const contentTopOf = (section: HTMLElement, scroller: HTMLElement) => {
    // `getBoundingClientRect` reports where the sticky offset parked the wrapper, not
    // where `scroller` has to go. Clearing the offset and restoring it within the same
    // tick gives the unparked position with no visible jump, because the browser
    // paints once, after the tick.
    const parked = wrapperNode?.style.bottom
    if (wrapperNode) {
      wrapperNode.style.bottom = 'auto'
    }
    const top =
      section.getBoundingClientRect().top -
      scroller.getBoundingClientRect().top +
      scroller.scrollTop
    if (wrapperNode && parked !== undefined) {
      wrapperNode.style.bottom = parked
    }
    return top
  }

  /** Whether no more than the peek height of `section` shows inside `scroller`. */
  const isPinnedAtBottom = (section: HTMLElement, scroller: HTMLElement) =>
    scroller.getBoundingClientRect().bottom - section.getBoundingClientRect().top <=
    // The extra pixel absorbs sub-pixel rounding in the two rectangles.
    pinnedPeekHeightPixels + 1

  /**
   * Scrolls the top of `section` to the middle of `scroller`, or higher when `section`
   * is too tall to fit in the lower half.
   */
  const scrollSectionIntoView = (section: HTMLElement, scroller: HTMLElement) => {
    const scrollerHeight = scroller.clientHeight
    const topOnScreen = Math.min(
      scrollerHeight / 2,
      Math.max(0, scrollerHeight - section.offsetHeight)
    )
    scroller.scrollTo({
      top: contentTopOf(section, scroller) - topOnScreen,
      behavior: 'smooth'
    })
  }
</script>

<!-- `bg-white-dark` is opaque, so the content the wrapper is pinned over does not show
     through it. -->
<div
  class="bg-white-dark sticky left-0 z-10 flex flex-col {className}"
  style:bottom={stickyBottom}
  bind:offsetHeight={wrapperHeight}
  bind:this={wrapperNode}
  onclickcapture={(e) => {
    // While the wrapper is pinned, an interactive element in the first pinned section
    // may show up on screen. Capturing the click over the section based on its height
    // (`isPinnedAtBottom`) scrolls the section into view instead,
    // and the user can click again on what they can now see.
    //
    // A keyboard click reports `detail` 0. No pointer is involved, so let it reach the
    // focused control.
    if (e.detail === 0) {
      return
    }
    const wrapper = e.currentTarget as HTMLElement
    const section = wrapper.firstElementChild as HTMLElement | null
    const scroller = scrollParentOf(wrapper)
    if (!section || !scroller || !isPinnedAtBottom(section, scroller)) {
      return
    }
    e.stopPropagation()
    e.preventDefault()
    scrollSectionIntoView(section, scroller)
  }}
  role="presentation"
  data-testid="box-pinned-sections"
>
  {@render children()}
</div>
