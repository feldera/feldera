<!--
  LogView: a virtualised, selectable monospace renderer for line-oriented logs.

  Renders lines in chunks rather than one at a time, which keeps the node count and the number of
  resize observations down, and keeps the set of chunks pinned for a selection small. Text is
  static: no collapsing, no folding, colour and style only.

  Four behaviours that a plain virtual list does not give you:

  - Selection survives scrolling. Chunks the selection touches are held in the DOM until it
    collapses, so dragging past the edge of the viewport does not destroy what came before.
  - Copying is reconstructed from the source lines, not from the DOM, so a selection whose middle
    was never mounted still copies in full.
  - The view anchors to the bottom as content arrives, and settles there in one press of the FAB.
  - A search match is held in the DOM while it is current, so the highlight paints against a node
    that is certain to exist rather than one the scroll might still be on its way to.
-->
<script lang="ts">
  import stripAnsi from 'strip-ansi'
  import type { Snippet } from 'svelte'
  import { untrack } from 'svelte'
  import { Virtualizer, type VirtualizerHandle } from 'virtua/svelte'
  import ANSIDecoratedText from './ANSIDecoratedText.svelte'
  import { chunkIndexOfLine, chunkLines } from './logChunks'
  import { resolveCopySlice, sliceLinesForCopy } from './logCopy'
  import { useLogLayout } from './logLayout.svelte'
  import { createLineAligner } from './logLineAlign'
  import {
    applySearchHighlight,
    countOccurrences,
    emptySearchState,
    findMatchOffsets,
    findOccurrence,
    type SearchState
  } from './logSearch'
  import ScrollDownFab from './ScrollDownFab.svelte'
  import { selectScope } from './selectScope'
  import { useSelectionPin } from './selectionPin.svelte'
  import { useStickToBottom } from './stickToBottom.svelte'

  interface Props {
    /** Lines to render, one per row. Hosts pre-split their source into lines. */
    lines: string[]
    /** Absolute line number of `lines[0]`. Chunk boundaries are cut against it, so a buffer that
     *  evicts from the front does not reshuffle the chunks that remain. */
    firstLineIndex?: number
    /** Externally-owned search state, advanced by the host through {@link advanceSearch}. Kept a
     *  prop so hosts can put the search input wherever their layout wants it. */
    search?: SearchState
    /** Show a left gutter of 1-based line numbers. Drawn as generated content rather than text,
     *  so it stays out of both the copied text and the search-offset mapping. */
    showLineNumbers?: boolean
    /** When true the view starts at the bottom and re-anchors as content grows — the streaming
     *  behaviour. When false (default) it starts at the top and never auto-scrolls, which suits a
     *  static dump. */
    streaming?: boolean
    /** Extra classes for the scroll container (background, padding, ...). */
    class?: string
    /** Inline style for the scroll container — for values that don't fit Tailwind utilities. */
    style?: string
    /** Renders above the scroll container — for status banners (e.g. "logs were skipped"). */
    header?: Snippet
    /** Fired whenever stick-to-bottom toggles: `true` when the view re-anchors to the bottom,
     *  `false` when the user scrolls up off it. */
    onStickToBottomChange?: (stickToBottom: boolean) => void
    /** Fired with the number of lines matching the current pattern (0 when the search is
     *  cleared). Hosts use it to enable and disable their match-navigation buttons. */
    onMatchCountChange?: (count: number) => void
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
    onStickToBottomChange,
    onMatchCountChange
  }: Props = $props()

  let scrollContainer: HTMLDivElement | undefined = $state()
  let virtualizer = $state<VirtualizerHandle | undefined>()

  const chunks = $derived(chunkLines(lines, firstLineIndex))

  // ---------------------------------------------------------------------------------------------
  // Stick to bottom.
  //
  // The same anchor the change stream and the ad-hoc query results use, with nothing added. It
  // settles by writing `scrollTop` and iterating until the scroll height stops moving, which is
  // what makes it right for a virtualiser as well: the virtualiser writes `scrollTop` itself
  // whenever a freshly measured row changes the layout, and each pass reads the corrected geometry
  // back and aims again.
  //
  // It converges in a frame or two only because the size cache has already told the virtualiser
  // how tall everything is. Left to discover the sizes it re-measures for about a second, and the
  // anchor spends that second chasing a moving bottom — the two halves hold each other up.

  // `streaming` picks the initial anchor and is deliberately read once: it is a mount-time mode,
  // not a live toggle, so changing it later must not yank a list the user is already reading.
  // svelte-ignore state_referenced_locally
  const stickToBottom = useStickToBottom({
    initial: streaming,
    // Tracked on the line count rather than the chunk list, because an append usually lengthens
    // the trailing chunk rather than adding one.
    observeSize: () => lines.length,
    onChange: (stuck) => onStickToBottomChange?.(stuck)
  })

  /** Re-arm the bottom anchor. The scroll-to-bottom button calls this; hosts may too. */
  export function stick() {
    stickToBottom.stick()
  }

  const selectionPin = useSelectionPin(() => scrollContainer)

  // The virtualiser's size cache. A fresh prediction arrives as a new `layout.current`, which the
  // `{#key}` block below turns into a fresh virtualiser — see `logLayout`.
  const layout = useLogLayout({
    container: () => scrollContainer,
    chunks: () => chunks,
    showLineNumbers: () => showLineNumbers,
    virtualizer: () => virtualizer,
    anchor: stickToBottom,
    selection: selectionPin
  })

  // Line the search cursor is currently on, or -1 when there is no pattern or nothing matches.
  const matchedLine = $derived(
    search.pattern ? findOccurrence(lines, search.pattern, search.occurrenceIndex) : -1
  )

  // Two things pin chunks into the DOM, and they pin for the same reason: something outside the
  // viewport still needs a live node.
  //
  // `keepMounted` takes positions in `chunks`, while a selection pin records the chunk's stable
  // key. Keys run contiguously from the first chunk, so the position is just the offset from it.
  //
  // Holding the *tail* chunk as well was tried, on the theory that an unmeasured last chunk leaves
  // `scrollHeight` an estimate and makes the first scroll land short. It changed nothing
  // measurable: virtua measures the tail in the same frame the scroll reaches it, and the settle
  // loop already absorbs whatever gap remains. It is the settle loop that makes the anchor
  // reliable, not a pin.
  const keepMounted = $derived.by(() => {
    const firstKey = chunks[0]?.key
    if (firstKey === undefined) {
      return []
    }
    const indices = new Set<number>()
    for (const key of selectionPin.pinned) {
      const index = key - firstKey
      if (index >= 0 && index < chunks.length) {
        indices.add(index)
      }
    }
    if (matchedLine >= 0) {
      const index = chunkIndexOfLine(chunks, matchedLine)
      if (index >= 0) {
        indices.add(index)
      }
    }
    return [...indices]
  })

  // ---------------------------------------------------------------------------------------------
  // Search.
  //
  // The match is painted with the CSS Custom Highlight API rather than by wrapping the matched
  // characters in markup, so a matched row renders exactly like every other row and the ANSI
  // output is left alone.
  //
  // `CSS.highlights` is keyed by a name the `::highlight()` selector cannot interpolate, so the
  // name is fixed. One log view is visible per host at a time, which makes that safe.
  const highlightName = 'feldera-log-view-search'

  // Report the match count so the host can enable and disable its navigation buttons. Recomputes
  // as the pattern changes and as streaming appends lines that match.
  const matchCount = $derived(countOccurrences(lines, search.pattern))
  $effect(() => {
    onMatchCountChange?.(matchCount)
  })

  function paintHighlight() {
    if (!scrollContainer || matchedLine < 0 || !search.pattern) {
      applySearchHighlight(highlightName, null, [])
      return
    }
    const row = scrollContainer.querySelector<HTMLElement>(`[data-line="${matchedLine}"]`)
    applySearchHighlight(
      highlightName,
      row,
      // Offsets index the visible characters, so they are computed against the stripped line:
      // escape sequences are markup by the time they reach the DOM, and the line-number gutter is
      // generated content, so neither shows up in the text nodes the highlight walks.
      row ? findMatchOffsets(stripAnsi(lines[matchedLine]), search.pattern) : []
    )
  }

  // Paint whenever the match moves, the pattern changes, the pin lands, or the virtualiser is
  // rebuilt around a width change (which replaces the row's nodes and invalidates the Ranges).
  //
  // The matched chunk is in `keepMounted`, so by the time this effect runs the row is in the
  // document whether or not the scroll has reached it. That is what removes the retry loop the
  // previous renderer needed: it virtualised by line, could not pin one, and had to re-attempt the
  // paint every frame until the scroll happened to mount the row.
  $effect(() => {
    void matchedLine
    void search.pattern
    void keepMounted
    void layout.current
    paintHighlight()
  })

  // Drop the registered Highlight on unmount so it does not leak into the next host.
  $effect(() => () => applySearchHighlight(highlightName, null, []))

  const aligner = createLineAligner()

  /**
   * Put `line` in the middle of the viewport.
   *
   * The chunk's offset comes from the virtualiser, which has been measuring since the cache seeded
   * it and so is the better source by now; the line's offset inside its chunk comes from the
   * prediction, since the chunk need not be mounted yet.
   */
  const centreOnLine = (line: number) => {
    const predicted = layout.current
    const index = chunkIndexOfLine(chunks, line)
    if (!scrollContainer || !predicted || !virtualizer || index < 0) {
      return
    }
    void aligner.centre(
      scrollContainer,
      line,
      virtualizer.getItemOffset(index) + predicted.offsetWithinChunk(line),
      predicted.heightOfLine(line)
    )
  }

  // Bring the match into view once per submission. Tracked on the SearchState fields only, with
  // the lookup untracked, so streaming appends cannot re-trigger the jump — the user keeps control
  // of the scroll after the initial move.
  $effect(() => {
    const pattern = search.pattern
    const occurrence = search.occurrenceIndex
    if (!pattern) {
      return
    }
    untrack(() => {
      const line = findOccurrence(lines, pattern, occurrence)
      if (line < 0) {
        return
      }
      // Give up the bottom first, so a streaming log does not immediately drag the view off the
      // match. `release` also calls off an anchor already in flight, which would otherwise keep
      // re-applying the end offset and override the jump.
      stickToBottom.release()
      centreOnLine(line)
    })
  })

  // Rebuild the clipboard text from the source rather than letting the browser serialise the DOM.
  // A selection can span chunks that were never mounted — a select-all always does — and those
  // contribute no text to a native copy.
  const oncopy = (event: ClipboardEvent) => {
    if (!scrollContainer) {
      return
    }
    // A select-all pins nothing, so its DOM endpoints only ever span the mounted rows. Take the
    // whole log from the source instead of resolving them, which would silently truncate.
    const slice = selectionPin.selectAll ? 'all' : resolveCopySlice(scrollContainer)
    if (!slice) {
      return
    }
    event.clipboardData?.setData('text/plain', sliceLinesForCopy(lines, slice))
    event.preventDefault()
  }
</script>

<!-- Positioned so ScrollDownFab can anchor inside LogView itself, rather than every wrapper
     having to provide a `relative` parent. -->
<div class="relative flex h-full w-full flex-col">
  {@render header?.()}
  <div
    bind:this={scrollContainer}
    role="textbox"
    tabindex={-1}
    use:stickToBottom.action
    use:selectScope
    class="log-view-scroll scrollbar w-full flex-1 overflow-y-auto {className}"
    {style}
    {oncopy}
  >
    <!-- Keyed on the layout: a `CacheSnapshot` is read when the virtualiser mounts, so a fresh
         prediction has to arrive with a fresh virtualiser. See the comment on RESEED_SETTLE_MS.
         Gated on it as well, so the first virtualiser to mount is already the one holding the
         prediction: mounting an uncached one first and replacing it costs a frame before anything
         appears, which is long enough to notice on a big log. -->
    {#if layout.current}
      {#key layout.current}
        <Virtualizer
          bind:this={virtualizer}
          data={chunks}
          getKey={(chunk) => chunk.key}
          {keepMounted}
          cache={[layout.current.sizes, layout.current.defaultSize]}
        >
          {#snippet children(chunk)}
            <div data-chunk={chunk.key}>
              {#each chunk.lines as line, i (i)}
                <div
                  class="log-view-row"
                  class:log-view-numbered={showLineNumbers}
                  style:counter-set={showLineNumbers
                    ? `line ${chunk.startLine + i + 1}`
                    : undefined}
                  data-line={chunk.startLine + i}
                >
                  <ANSIDecoratedText value={line} />
                </div>
              {/each}
            </div>
          {/snippet}
        </Virtualizer>
      {/key}
    {/if}
  </div>
  <ScrollDownFab {stickToBottom}></ScrollDownFab>
</div>

<style>
  .log-view-scroll {
    font-family:
      'DM Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono',
      'Courier New', monospace;
    /*
      Reserve the scrollbar gutter up front. Without this, the scrollbar appearing the moment the
      log first overflows takes ~15px out of `clientWidth`, which is indistinguishable from the
      user resizing the panel — it rebuilds the virtualiser and paints a frame at the wrong
      offset. Reserving the space means the width never changes, so the rebuild never fires.

      Browsers with overlay scrollbars (headless Chromium, macOS by default) have a zero-width
      gutter and never had the problem; this costs them nothing.
    */
    scrollbar-gutter: stable;
  }
  .log-view-row {
    white-space: pre-wrap;
    /* `break-word`, not `break-all`: it moves an overlong token to its own line before splitting
       it, which keeps a wrapped URL or hash readable instead of starting it mid-token. */
    overflow-wrap: break-word;
    tab-size: 8;
  }
  /*
    virtua disables pointer events on its inner container while a scroll is in flight, and holds
    that for 150ms after the last scroll event. A drag-selection that reaches the edge of the
    viewport auto-scrolls, so the rows go pointer-transparent mid-drag and the selection stops
    tracking them — the "could not select and scroll at the same time" bug. Chunking keeps the
    node count low enough that the optimisation buys nothing here.
  */
  .log-view-scroll > :global(div) {
    pointer-events: auto !important;
  }
  /*
    `CSS.highlights` is registered on the document, so the matching rule has to resolve at the
    document root too. Chromium honours a class-scoped `::highlight()`; Firefox only honours one
    whose selector matches at document level, which is why this is `:global` rather than scoped.
  */
  :global(::highlight(feldera-log-view-search)) {
    background-color: var(--color-secondary-200);
  }
  :global(.dark ::highlight(feldera-log-view-search)) {
    background-color: var(--color-secondary-800);
  }
  /*
    Line-number gutter. Drawn with `counter-set` + `::before` so the number is generated content
    rather than a text node: it stays out of the copy reconstruction (which reads `data-line`) and
    out of the search-offset walk (which counts rendered text), both of which would otherwise be
    shifted by the width of the number.
  */
  .log-view-numbered {
    position: relative;
    padding-left: 4rem;
  }
  .log-view-numbered::before {
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
  :global(.dark) .log-view-numbered::before {
    color: var(--color-surface-600);
    border-right-color: var(--color-surface-800);
  }
</style>
