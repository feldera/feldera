<script lang="ts">
  import { untrack, type Snippet } from 'svelte'
  import { Virtualizer, type VirtualizerHandle } from 'virtua/svelte'
  import stripAnsi from 'strip-ansi'
  import ANSIDecoratedText from './ANSIDecoratedText.svelte'
  import ScrollDownFab from './ScrollDownFab.svelte'
  import { useReverseScrollContainer } from './useReverseScrollContainer.svelte'
  import { virtualSelect } from './userSelect'
  import {
    applySearchHighlight,
    emptySearchState,
    findMatchOffsets,
    findOccurrence,
    type SearchState
  } from './logSearch'

  /** Shape of the slice passed by {@link virtualSelect}'s copy interceptor — row/column
   *  endpoints over the virtualised list. */
  type CopySlice =
    | 'all'
    | { start: { row: number; col: number }; end: { row: number; col: number } }

  interface Props {
    /** Lines to render, one per row. Hosts split their source (a string, a stream, ...) into
     *  lines before passing them in. */
    lines: string[]
    /** Offset added to the row index when computing the Virtualizer's stable key — needed
     *  when the buffer evicts from the front (a streaming log shifts indices downward). */
    firstLineIndex?: number
    /** Externally-controlled search state. The owner advances it on submit via
     *  {@link advanceSearch} — kept here as a prop so callers can wire the search input
     *  wherever fits their layout. */
    search?: SearchState
    /** Show a left gutter with 1-based row numbers. Implemented via per-row `counter-set` +
     *  `::before` generated content so the line number is NOT a DOM text node, keeping the
     *  search-offset TreeWalker pointed at the actual log content. */
    showLineNumbers?: boolean
    /** When true, the container starts at the bottom and re-sticks as content grows — the
     *  streaming-log behaviour. When false (default), the container starts at the top and
     *  never auto-scrolls on mount — appropriate for static log dumps. */
    streaming?: boolean
    /** Extra classes for the scroll container — wrappers add background, padding, etc.
     *  The monospace font is already baked in. */
    class?: string
    /** Inline style for the scroll container — for values that don't fit Tailwind utilities. */
    style?: string
    /** Renders above the scroll container — for status banners (e.g. "logs were skipped"). */
    header?: Snippet
    /** Override the text produced by copy / Ctrl-C across the virtualised list. Default
     *  joins `lines` (ANSI-stripped) with `\n`. Hosts whose rows already carry trailing
     *  newlines pass an override that joins with `''`. */
    getCopyContent?: (slice: CopySlice) => string
    /** Invoked when the user presses Ctrl-F / Cmd-F while focus is inside the log list.
     *  Hosts typically focus their search input here. When omitted, the browser's native
     *  find-in-page is left to handle the shortcut. */
    onSearchShortcut?: () => void
  }

  let {
    lines,
    firstLineIndex = 0,
    search = emptySearchState,
    showLineNumbers = false,
    streaming = false,
    class: className = '',
    style,
    header,
    getCopyContent,
    onSearchShortcut
  }: Props = $props()

  // Reverse-scroll lives here (not in wrappers) so search behaviour and auto-scroll behaviour
  // stay co-located. `initialStickToBottom` flips with `streaming`: streams start at the
  // bottom and re-stick on growth; static bundles start at the top. The capture-by-initial
  // value is intentional — `streaming` is a structural switch chosen at mount time, not a
  // runtime-toggleable prop.
  // svelte-ignore state_referenced_locally
  const reverseScroll = useReverseScrollContainer({
    observeContentElement: (e) => e.firstElementChild!,
    initialStickToBottom: streaming
  })

  let scrollContainer: HTMLDivElement | undefined = $state()
  let virtualizer: VirtualizerHandle = $state()!

  // `CSS.highlights` is keyed by a static name (the `::highlight(<name>)` selector below can
  // not take a dynamic suffix). One LogList visible at a time per host, so a fixed name
  // shared across embedders is fine.
  const highlightName = 'feldera-log-list-search'

  const matchedIndex = $derived(
    search.pattern ? findOccurrence(lines, search.pattern, search.occurrenceIndex) : -1
  )

  function paintHighlight() {
    if (!scrollContainer || matchedIndex < 0 || !search.pattern) {
      applySearchHighlight(highlightName, null, [])
      return
    }
    const el = scrollContainer.querySelector<HTMLElement>(`[data-rowindex="${matchedIndex}"]`)
    applySearchHighlight(
      highlightName,
      el,
      el ? findMatchOffsets(stripAnsi(lines[matchedIndex]), search.pattern) : []
    )
  }

  // Re-paint when the matched row index shifts (streaming evicts rows) or the pattern changes.
  // Pure paint — never moves the viewport.
  $effect(() => {
    void matchedIndex
    void search.pattern
    paintHighlight()
  })

  // Bring the match into view ONCE per submission. Tracked only on the SearchState fields,
  // and the lookup runs `untrack`ed so streaming updates can't re-trigger the scroll — the
  // user retains scroll control after the initial jump.
  $effect(() => {
    const pattern = search.pattern
    const occ = search.occurrenceIndex
    if (!pattern) return
    untrack(() => {
      const idx = findOccurrence(lines, pattern, occ)
      if (idx < 0) return
      virtualizer?.scrollToIndex(idx, { align: 'center' })
      // virtua mounts the freshly-visible row on the next frame; paint then so the Range is
      // created against the new DOM node rather than the previously-cleared one.
      requestAnimationFrame(paintHighlight)
    })
  })

  // Drop the registered Highlight on unmount so it doesn't leak across hosts.
  $effect(() => () => applySearchHighlight(highlightName, null, []))

  function defaultGetCopyContent(slice: CopySlice): string {
    if (slice === 'all') return lines.map(stripAnsi).join('\n')
    const result = lines.slice(slice.start.row, slice.end.row + 1).map(stripAnsi)
    result[0] = result[0].slice(slice.start.col)
    result[result.length - 1] = result[result.length - 1].slice(
      0,
      slice.end.col - (slice.start.row === slice.end.row ? slice.start.col : 0)
    )
    return result.join('\n')
  }
</script>

<!-- The outer wrapper is positioned so ScrollDownFab (absolute-positioned) can anchor inside
     LogList itself — wrappers don't have to add their own `relative` parent. -->
<div class="relative flex h-full w-full flex-col">
  {@render header?.()}
  <!-- onscroll re-paints the highlight: when the matched row scrolls into view, virtua mounts
       a fresh DOM node, so the previously-painted Range is gone and must be re-created
       against the new node. The monospace font + user-select: contain are baked in here so
       wrappers don't have to repeat them. -->
  <div
    bind:this={scrollContainer}
    role="textbox"
    tabindex={-1}
    class="log-list-scroll scrollbar w-full flex-1 overflow-y-auto whitespace-pre-wrap {className}"
    {style}
    use:reverseScroll.action
    use:virtualSelect={{
      getRoot: (node) => node.firstElementChild!,
      getCopyContent: getCopyContent ?? defaultGetCopyContent
    }}
    onkeydown={(e) => {
      // Ctrl-F (Win/Linux) or Cmd-F (Mac) — redirect to host's search input. Only intercept
      // when the host supplied a handler; otherwise the browser's find-in-page is left alone.
      if (
        onSearchShortcut &&
        (e.key === 'f' || e.key === 'F') &&
        (e.ctrlKey || e.metaKey) &&
        !e.altKey &&
        !e.shiftKey
      ) {
        e.preventDefault()
        onSearchShortcut()
      }
    }}
  >
    <Virtualizer
      data={lines}
      getKey={(_, i) => i + firstLineIndex}
      bind:this={virtualizer}
      onscroll={paintHighlight}
    >
      {#snippet children(value, index)}
        <div
          data-rowindex={index}
          class:logline={showLineNumbers}
          style:counter-set={showLineNumbers ? `line ${index + 1}` : undefined}
        >
          <ANSIDecoratedText {value} />
        </div>
      {/snippet}
    </Virtualizer>
  </div>
  <ScrollDownFab {reverseScroll}></ScrollDownFab>
</div>

<style>
  /* Monospace font baked in. Both embedding apps load DM Mono via @fontsource/dm-mono, so it
     resolves; consumers without that font fall back to the system monospace stack. */
  .log-list-scroll {
    font-family:
      'DM Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono',
      'Courier New', monospace;
    user-select: contain;
  }
  /* `::highlight()` is registered globally on `CSS.highlights`, so the matching CSS rule
     also has to live at the document root. Class-scoped variants (Tailwind's
     `[&::highlight(...)]:bg-…`) work in Chromium but Firefox only honors `::highlight()`
     rules whose selector matches at the document level. */
  :global(::highlight(feldera-log-list-search)) {
    background-color: var(--color-secondary-200);
  }
  :global(.dark ::highlight(feldera-log-list-search)) {
    background-color: var(--color-secondary-800);
  }
  /* Line-number gutter. Generated content (`::before`) is invisible to DOM text-node
     walkers — keeps the search-offset mapping in {@link applySearchHighlight} aligned to
     the log content, ignoring the displayed line number. */
  .logline {
    position: relative;
    padding-left: 4rem;
    word-break: break-all;
  }
  .logline::before {
    content: counter(line);
    position: absolute;
    left: 0;
    width: 3rem;
    padding-right: 0.5rem;
    text-align: right;
    user-select: none;
    color: var(--color-surface-400);
    border-right: 1px solid var(--color-surface-200);
  }
  :global(.dark) .logline::before {
    color: var(--color-surface-600);
    border-right-color: var(--color-surface-800);
  }
</style>
