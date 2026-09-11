<script lang="ts" module>
  /**
   * How much of the group shows, in px, while it is pinned over the content above
   * it. Exported so a layout test asserts against the same number.
   */
  export const pinnedSectionsPeekPixels = 44
</script>

<script lang="ts">
  /**
   * Trailing sections of a scrolling page, pinned as one group over the bottom of
   * what precedes them.
   *
   * The group sticks to the bottom of the scroll container showing
   * `pinnedSectionsPeekPixels` of itself, which leaves the content above it a window
   * to scroll through and puts the top of the group on screen from the start. Sticky
   * releases the group once that content has been scrolled through, and the rest of
   * the group scrolls into view. The page stays the only thing that scrolls.
   *
   * A click on the group while it is still parked at the bottom brings its first
   * section into view and goes no further, so reaching what the group holds takes a
   * second click, once there is something to aim at.
   *
   * Each direct child is one section. The group is measured, so sections can be
   * added and removed freely.
   */
  import type { Snippet } from '$lib/types/svelte'

  const {
    children,
    class: className
  }: {
    children: Snippet
    /** Width and the spacing between sections; the group brings its own position. */
    class?: string
  } = $props()

  let groupHeight = $state(0)
  let groupNode = $state<HTMLElement>()

  // Sticking the group's bottom edge that far below the screen edge leaves exactly
  // the peek showing.
  const stickyBottom = $derived.by(() => {
    if (!groupHeight) {
      return undefined
    }
    // A group shorter than the peek shows whole, without hanging below the screen
    // edge.
    const peek = Math.min(groupHeight, pinnedSectionsPeekPixels)
    return `${peek - groupHeight}px`
  })

  /** Nearest ancestor that scrolls, which is what the group is pinned in. */
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
   * Where `section` sits in the scrolled content, ignoring the sticky offset that
   * parks the group at the bottom of the screen.
   *
   * Both `getBoundingClientRect` and `offsetTop` report where sticky has parked the
   * group, which is not where the page has to scroll to. So the measurement is taken
   * with the constraint lifted, and the style is put back in the same task: the
   * browser paints once, at the end of it, so the group never moves on screen.
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
   * Whether no more of the section shows than the peek, which is where it sits while
   * the group is pinned. A click there is aimed at the group itself; higher up, the
   * group is in view and a click is aimed at what it holds.
   */
  const isPinnedAtBottom = (section: HTMLElement, scroller: HTMLElement) =>
    scroller.getBoundingClientRect().bottom - section.getBoundingClientRect().top <=
    pinnedSectionsPeekPixels + 1

  /**
   * Scrolls a section up into view: its top lands in the middle of the screen, or
   * higher where the section is too tall to fit below the middle.
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

<!-- The background makes the group hide the content it is pinned over. Capture
     phase, so a click on the parked group never reaches what it holds. The first
     section is the one on show while the group is parked, and the one a click
     brings up. -->
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
