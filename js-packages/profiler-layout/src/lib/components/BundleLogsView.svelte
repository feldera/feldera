<script lang="ts">
  import { ANSIDecoratedText } from 'common-ui'
  import type { LookupCoordinator } from '../functions/lookup'

  interface Props {
    logText: string | undefined
    /** Lookup coordinator; the view registers an imperative handler so each Enter on the
     *  panel's search input cycles to the next match even when the query is unchanged. */
    lookup?: LookupCoordinator
  }

  let { logText, lookup }: Props = $props()

  let lineElements: (HTMLElement | null)[] = $state([])
  let lastMatchIndex = $state(-1)

  const lines = $derived(logText ? logText.split('\n') : [])

  function runSearch(query: string) {
    if (!query) {
      lastMatchIndex = -1
      return
    }
    const q = query.toLowerCase()
    const startSearch = lastMatchIndex + 1
    const total = lines.length

    for (let i = 0; i < total; i++) {
      const idx = (startSearch + i) % total
      if (lines[idx].toLowerCase().includes(q)) {
        lastMatchIndex = idx
        lineElements[idx]?.scrollIntoView({ block: 'center' })
        return
      }
    }
    lastMatchIndex = -1
  }

  $effect(() => {
    if (!lookup) {
      return
    }
    return lookup.register('Logs', runSearch)
  })
</script>

<div class="h-full rounded overflow-auto scrollbar bg-white-dark font-mono">
  {#if !logText}
    <div class="flex h-full items-center justify-center">
      No logs available in this bundle
    </div>
  {:else}
    <!-- Block-per-line layout: line numbers live in each row's ::before, positioned
         absolutely so wrapped text aligns with itself (not under the gutter). A CSS
         counter on the container drives the numbering so we don't pass indices to CSS. -->
    <div class="[counter-reset:line]">
      {#each lines as line, i (i)}
        <div
          bind:this={lineElements[i]}
          class="logline relative whitespace-pre-wrap break-all pl-16 [counter-increment:line]"
          class:bg-primary-100={i === lastMatchIndex}
          class:dark:bg-primary-900={i === lastMatchIndex}
        >
          <ANSIDecoratedText value={line} />
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
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
