<script lang="ts" generics="K extends string">
  import { fly } from 'svelte/transition'
  import type { Snippet } from '$lib/types/svelte'

  /**
   * Shows one "page" at a time and slides between them, like the panels of a phone
   * settings screen. `pages` is a fixed, ordered list of named panels and `current`
   * names the one on screen; changing `current` is the "navigation" that animates
   * the swap.
   *
   * The caller never requests "forward" or "back": navigation is assumed to be linear,
   * and the slide direction is derived from each page's index in `pages`. Moving to
   * a larger index the new page enters from the right, moving to a smaller index
   * slides the page from the left. Comparing indices is a lightweight stand-in
   * for an actual navigation-history stack, which has not been needed in web-console.
   * `pages` must be ordered from the outermost page (root, index 0) to the deepest.
   *
   */
  let {
    current,
    pages,
    width = 220,
    duration = 200,
    class: className = ''
  }: {
    /** Key of the page currently shown. Must match one of `pages[].key`. */
    current: K
    /** The panels, ordered outermost-first: index 0 is the root, later entries are deeper. */
    pages: { key: K; content: Snippet }[]
    /** How far a page slides as it enters or leaves, in pixels. */
    width?: number
    /** Slide duration, in milliseconds. */
    duration?: number
    class?: string
  } = $props()

  const indexOf = (key: K) => pages.findIndex((page) => page.key === key)

  // Which way the latest navigation slides: +1 toward a nested page (later in
  // `pages`), -1 toward a "previous" one. We compare the new page's position with
  // the previous page's (`lastKey`) — position alone decides the direction, so no
  // navigation history is kept. `$effect.pre` runs before the DOM is updated, so
  // `direction` is already current when the entering and leaving pages mount and
  // read it for their slide transitions.
  let direction = $state(1)
  let lastKey = current
  $effect.pre(() => {
    const delta = indexOf(current) - indexOf(lastKey)
    if (delta !== 0) {
      direction = Math.sign(delta)
    }
    lastKey = current
  })
</script>

<!-- Every page occupies the same single grid cell (row 1, column 1), so the
     leaving and entering pages overlap and slide across each other rather than
     pushing the layout around. Both move along the same `direction`: the entering
     page flies in from one edge while the leaving page flies out the opposite
     edge, reading as one continuous sweep. -->
<div class="grid {className}">
  {#each pages as page (page.key)}
    {#if page.key === current}
      <div
        in:fly={{ x: direction * width, duration }}
        out:fly={{ x: -direction * width, duration }}
        class="col-start-1 row-start-1 flex flex-col">
        {@render page.content()}
      </div>
    {/if}
  {/each}
</div>
