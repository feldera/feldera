<!--
  Harness for `useStickToBottom`.

  Two modes, because the two failure families need different content:

  - Exact mode: the test sets the content height directly. A real virtualiser resizes its own
    content as it measures rows, which is what makes the timing hard to pin down; a single div
    resizes only when the test says so.

  - Estimated mode: the spacer reports a height well short of its real one until it is scrolled
    into view, then grows to full size. This is what a virtualiser does with unmeasured rows, and
    it is the only way to catch a scroll-to-bottom that gives up after a fixed number of passes.
    Every exact-height fixture converges on the first pass, so it cannot fail that test.
-->
<script lang="ts">
  import { useStickToBottom } from '$lib/stickToBottom.svelte'

  let {
    /**
     * Watch the content element for resizes. Turning this off leaves the settle loop's own
     * iteration as the only thing that can converge on a moving bottom, which is how a test
     * isolates it from the resize observer that would otherwise re-kick it.
     */
    observe = true,
    /**
     * Mount with no content at all, so {@link showContent} is what puts it in the DOM. Models a
     * host that renders its content conditionally: nothing is there to observe on the first pass.
     */
    deferContent = false
  }: { observe?: boolean; deferContent?: boolean } = $props()

  let contentHeight = $state(1000)

  // Estimated mode. The spacer starts at `estimatedHeight` and grows by `growthStep` each time it
  // is scrolled near, capped at `trueHeight` — the same one-directional error a virtualiser makes
  // when its per-row estimate is smaller than the wrapped row actually is.
  let estimated = $state(false)
  let reportedHeight = $state(0)
  let trueHeight = 0
  let growthStep = 0

  let content: HTMLDivElement | undefined = $state()
  // svelte-ignore state_referenced_locally
  let showing = $state(!deferContent)

  export function showContent() {
    showing = true
  }

  export const stickToBottom = useStickToBottom(
    observe ? { observeElement: () => content } : {}
  )

  let container: HTMLDivElement | undefined = $state()

  // Grow whenever the view arrives at what it currently believes is the bottom. Reading geometry
  // in a scroll handler is what a virtualiser's measure pass does, so the growth lands in the same
  // frame relationship to the settle loop as a real measurement would.
  const onscroll = () => {
    if (!estimated || !container || reportedHeight >= trueHeight) {
      return
    }
    const distance = container.scrollHeight - container.scrollTop - container.clientHeight
    if (distance <= 8) {
      reportedHeight = Math.min(reportedHeight + growthStep, trueHeight)
    }
  }

  export function setContentHeight(px: number) {
    contentHeight = px
  }
  /**
   * Switch to estimated mode. The container starts believing it is `from` tall and converges on
   * `to`, one `step` per arrival at the apparent bottom.
   */
  export function useEstimatedHeight(from: number, to: number, step: number) {
    estimated = true
    reportedHeight = from
    trueHeight = to
    growthStep = step
  }
  export function getReportedHeight() {
    return reportedHeight
  }

  // A rebuild, the way LogView remounts its virtualiser around a fresh size cache: the content
  // element is replaced and the scroll position it held is gone. The container the action sits on
  // survives, so the anchor is never re-created — only the thing it was anchoring.
  let rebuildEpoch = $state(0)
  export function rebuildContent() {
    rebuildEpoch++
    if (container) {
      container.scrollTop = 0
    }
  }
</script>

<!-- `overflow-anchor: none` is what virtua sets on its own scroll container, so a rebuild here
     loses its scroll position the way a remounted virtualiser does. Left to the browser, scroll
     anchoring restores the offset on its own and the anchor is never the thing under test. -->
<div
  bind:this={container}
  data-testid="stick-to-bottom-container"
  style="height: 200px; overflow-y: auto; overflow-anchor: none;"
  use:stickToBottom.action
  {onscroll}
>
  {#if showing}
    {#key rebuildEpoch}
      <div
        bind:this={content}
        style="height: {estimated ? reportedHeight : contentHeight}px;"
      ></div>
    {/key}
  {/if}
</div>
