<!--
  Harness for `LogView`.

  Gives the tests a sized ancestor (LogView is `h-full`, so without one the scroll container
  collapses to clientHeight 0 and the virtualiser never mounts anything), plus imperative control
  over the line buffer so a spec can append the way a live feed does.
-->
<script lang="ts">
  import LogView from '$lib/LogView.svelte'
  import { emptySearchState, type SearchPattern, type SearchState } from '$lib/logSearch'

  let {
    initialLines = [],
    streaming = false,
    initialWidth = 800,
    initialHeight = 400,
    showLineNumbers = false
  }: {
    initialLines?: string[]
    streaming?: boolean
    initialWidth?: number
    initialHeight?: number
    showLineNumbers?: boolean
  } = $props()

  // Both are seeds, not live inputs: the spec drives the buffer through `append` afterwards, and
  // `streaming` is a mount-time mode in LogView itself.
  // svelte-ignore state_referenced_locally
  let lines = $state<string[]>([...initialLines])
  // svelte-ignore state_referenced_locally
  let stuck = $state(streaming)
  // svelte-ignore state_referenced_locally
  let width = $state(initialWidth)
  // svelte-ignore state_referenced_locally
  let height = $state(initialHeight)
  let view: ReturnType<typeof LogView> | undefined = $state()

  /** Resize the container, as a pane drag or a window resize would. */
  export function setWidth(next: number) {
    width = next
  }

  /** Shorten or lengthen the container, as dragging a horizontal splitter would. */
  export function setHeight(next: number) {
    height = next
  }

  export function append(newLines: string[]) {
    lines.push(...newLines)
  }
  export function getLines() {
    return lines
  }
  export function isStuck() {
    return stuck
  }
  /** Re-arm the bottom anchor, as the scroll-to-bottom button does. */
  export function stick() {
    view?.stick()
  }

  let search = $state<SearchState>(emptySearchState)
  let matchCount = $state(0)

  /** Submit a search, as the host's search bar does. Bumping `occurrenceIndex` steps matches. */
  export function setSearch(pattern: SearchPattern | null, occurrenceIndex = 0) {
    search = pattern ? { pattern, occurrenceIndex } : emptySearchState
  }
  export function getMatchCount() {
    return matchCount
  }
</script>

<div style="height: {height}px; width: {width}px; display: flex; flex-direction: column;">
  <LogView
    bind:this={view}
    {lines}
    {streaming}
    {search}
    {showLineNumbers}
    onStickToBottomChange={(value) => (stuck = value)}
    onMatchCountChange={(count) => (matchCount = count)}
  />
</div>
