<script lang="ts">
  import { SvelteSet } from 'svelte/reactivity'
  import { slide } from 'svelte/transition'
  import type { Severity, TriageResults } from 'triage-types'
  import type { LookupCoordinator } from '../functions/lookup'
  import { getCategory, severityLabel, severityOrder } from '../functions/triage'

  interface Props {
    results: TriageResults
    /** Lookup coordinator; the view registers an imperative handler so each Enter on the
     *  panel's search input cycles to the next match even when the query is unchanged. */
    lookup?: LookupCoordinator
    /** 'all' or a Severity. */
    severityFilter?: 'all' | Severity
    /** 'all' or a category label as produced by `getCategory`. */
    categoryFilter?: string
  }

  let { results, lookup, severityFilter = 'all', categoryFilter = 'all' }: Props = $props()

  let rowElements: (HTMLElement | null)[] = $state([])
  let lastMatchIndex = $state(-1)

  const sortedResults = $derived(
    [...results.results].sort(
      (a, b) => (severityOrder[a.severity] ?? 3) - (severityOrder[b.severity] ?? 3)
    )
  )

  const visibleResults = $derived(
    sortedResults.filter(
      (r) =>
        (severityFilter === 'all' || r.severity === severityFilter) &&
        (categoryFilter === 'all' || getCategory(r) === categoryFilter)
    )
  )

  function runSearch(query: string) {
    if (!query) {
      lastMatchIndex = -1
      return
    }
    const q = query.toLowerCase()
    const startSearch = lastMatchIndex + 1
    const total = visibleResults.length

    for (let i = 0; i < total; i++) {
      const idx = (startSearch + i) % total
      const r = visibleResults[idx]
      if (
        r.rule.toLowerCase().includes(q) ||
        r.message.toLowerCase().includes(q) ||
        JSON.stringify(r.details).toLowerCase().includes(q)
      ) {
        lastMatchIndex = idx
        rowElements[idx]?.scrollIntoView({ block: 'center' })
        return
      }
    }
    lastMatchIndex = -1
  }

  $effect(() => {
    if (!lookup) {
      return
    }
    return lookup.register('Issues', runSearch)
  })

  // Reset search cursor when the filtered list changes.
  $effect(() => {
    void visibleResults
    lastMatchIndex = -1
  })

  const severityChipClass: Record<Severity, string> = {
    error: 'bg-error-50-950/50 text-error-700 dark:text-error-300',
    warning: 'bg-warning-100-900/50 text-warning-800-200',
    info: 'bg-tertiary-100-900/50 text-tertiary-800-200'
  }

  function formatDetails(details: Record<string, unknown>): string {
    return JSON.stringify(details, null, 2)
  }

  // Keyed by the row's stable id ("rule:index") rather than positional index, so the
  // expanded state survives filter changes that re-order the visible list.
  const expandedRows = new SvelteSet<string>()
  function toggleRow(id: string) {
    if (expandedRows.has(id)) {
      expandedRows.delete(id)
    } else {
      expandedRows.add(id)
    }
  }
</script>

<div class="h-full overflow-auto scrollbar">
  {#if visibleResults.length === 0}
    <div class="flex h-full flex-col items-center justify-center gap-2">
      <span class="fd fd-check-circle text-[32px] text-success-500"></span>
      <span>
        {sortedResults.length === 0 ? 'No issues found' : 'No issues match the current filters'}
      </span>
    </div>
  {:else}
    <div class="flex flex-col divide-y divide-surface-100-900 bg-white-dark">
      {#each visibleResults as result, i (result.rule + ':' + i)}
        {@const rowId = result.rule + ':' + i}
        {@const hasDetails = Object.keys(result.details).length > 0}
        {@const category = getCategory(result)}
        {@const isExpanded = expandedRows.has(rowId)}
        <div
          bind:this={rowElements[i]}
          class="p-2"
          class:bg-primary-50={i === lastMatchIndex}
          class:dark:bg-primary-950={i === lastMatchIndex}
        >
          <div class="flex flex-wrap items-center gap-2">
            <span
              class="inline-flex h-6 items-center rounded px-2 text-sm font-medium {severityChipClass[
                result.severity
              ] ?? ''}"
            >
              {severityLabel[result.severity] ?? result.severity}
            </span>
            <span
              class="inline-flex h-6 items-center rounded bg-surface-100-900/50 px-2 text-sm font-medium text-surface-950-50"
            >
              {category}
            </span>
            <span class="ml-auto font-mono text-sm text-surface-700-300">{result.rule}</span>
          </div>
          <p class="mt-2 font-semibold">{result.message}</p>
          {#if hasDetails}
            <button
              class="mt-1 text-primary-600-400 hover:underline"
              onclick={() => toggleRow(rowId)}
            >
              {isExpanded ? 'Hide details' : 'Show details'}
            </button>
            {#if isExpanded}
              <div transition:slide={{duration: 100}}>
                <pre
                  class="mt-2 overflow-x-auto rounded bg-surface-50-950 p-2 font-mono">{formatDetails(
                    result.details
                  )}</pre>
              </div>
            {/if}
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</div>
