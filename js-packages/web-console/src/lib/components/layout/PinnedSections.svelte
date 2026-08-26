<script lang="ts" module>
  // This is a wrapper for the home page sections that are pinned to the bottom of the viewport
  // when scrolling through the pipelines table.

  /**
   * How much of the section shows, in px, while it is pinned. Layout tests assert against this.
   */
  export const pinnedPeekHeightPixels = 44
</script>

<script lang="ts">
  /**
   * Trailing sections of a scrolling page, pinned as one group over the bottom of
   * the content above them.
   *
   * The group sticks to the bottom of the scroll container showing
   * `pinnedPeekHeightPixels` of itself, which leaves the content above it a window
   * to scroll through. Once that content is scrolled through, the group scrolls up
   * with it. The page is the only scroll container.
   *
   * A click on the pinned group scrolls its first section into view instead of
   * reaching the controls inside that section, which takes a second click.
   *
   * Each direct child is one section; the group measures its own height, so sections can be
   * added and removed freely.
   */
  import type { Snippet } from '$lib/types/svelte'

  const {
    children,
    class: className
  }: {
    children: Snippet
    /** Width and spacing between sections. */
    class?: string
  } = $props()

  let groupHeight = $state(0)
  let groupNode = $state<HTMLElement>()

  // The group's bottom edge sits this far below the screen edge, which leaves the
  // peek showing.
  const stickyBottom = $derived.by(() => {
    if (!groupHeight) {
      return undefined
    }
    // A group shorter than the peek shows whole.
    const peek = Math.min(groupHeight, pinnedPeekHeightPixels)
    return `${peek - groupHeight}px`
  })

  /** Nearest scrolling ancestor: the container the group is pinned in. */
  const scrollParentOf = (node: HTMLElement) => {
    for (let parent = node.parentElement; parent; parent = parent.parentElement) {
      const overflowY = getComputedStyle(parent).overflowY
      if (overflowY === 'auto' || overflowY === 'scroll') {
        return parent
      }
    }
    return null
  }

  /**
   * Where `section` sits in the scrolled content, ignoring the sticky offset.
   *
   * `getBoundingClientRect` and `offsetTop` both report where the sticky style parked the
   * group, not where the page has to scroll to. So the measurement lifts the offset
   * and restores it within the same task: the browser paints once, at the end of the
   * task, so the group never moves on screen.
   */
  const contentTopOf = (section: HTMLElement, scroller: HTMLElement) => {
    const parked = groupNode?.style.bottom
    if (groupNode) {
      groupNode.style.bottom = 'auto'
    }
    const top =
      section.getBoundingClientRect().top -
      scroller.getBoundingClientRect().top +
      scroller.scrollTop
    if (groupNode && parked !== undefined) {
      groupNode.style.bottom = parked
    }
    return top
  }

  /**
   * Whether the section shows no more than the peek height, meaning the group is still
   * pinned. A click there targets the group; higher up it targets the controls
   * inside the section.
   */
  const isPinnedAtBottom = (section: HTMLElement, scroller: HTMLElement) =>
    scroller.getBoundingClientRect().bottom - section.getBoundingClientRect().top <=
    pinnedPeekHeightPixels + 1

  /**
   * Scrolls a section's top to the middle of the screen, or higher if the section is
   * too tall to fit below the middle.
   */
  const scrollSectionIntoView = (section: HTMLElement, scroller: HTMLElement) => {
    const screenHeight = scroller.clientHeight
    const topOnScreen = Math.min(screenHeight / 2, Math.max(0, screenHeight - section.offsetHeight))
    scroller.scrollTo({
      top: contentTopOf(section, scroller) - topOnScreen,
      behavior: 'smooth'
    })
  }
</script>

<!-- The background hides the content the group is pinned over. The click event capture-phase
     handler keeps a click on the pinned group from reaching the controls inside the
     first section, and scrolls to that section instead. -->
<div
  class="bg-white-dark sticky left-0 z-10 flex flex-col {className}"
  style:bottom={stickyBottom}
  bind:offsetHeight={groupHeight}
  bind:this={groupNode}
  onclickcapture={(e) => {
    const group = e.currentTarget as HTMLElement
    const section = group.firstElementChild as HTMLElement | null
    const scroller = scrollParentOf(group)
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
