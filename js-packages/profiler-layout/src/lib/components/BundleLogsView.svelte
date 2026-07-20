<script lang="ts">
  import {
    advanceSearch,
    countOccurrences,
    emptySearchState,
    LogList,
    type SearchState
  } from 'common-ui'
  import type { LookupCoordinator } from '../functions/lookup'

  interface Props {
    logText: string | undefined
    /** Optional cross-tab search router. When provided, this view registers itself under
     *  `lookupTabId` (default `'Logs'`) and folds incoming queries into its own
     *  {@link SearchState} via {@link advanceSearch} — same pattern → next match, new
     *  pattern → first match, empty query → clear. */
    lookup?: LookupCoordinator
    lookupTabId?: string
  }

  let { logText, lookup, lookupTabId = 'Logs' }: Props = $props()

  // SearchState lives here so any host that uses BundleLogsView
  // gets the search-on-tab behaviour through LookupCoordinator.
  // LogList is purely the renderer; it accepts the state as a prop.
  let search: SearchState = $state(emptySearchState)

  // LogList expects pre-split lines — the bundle view's source is a single string blob.
  const lines = $derived(logText ? logText.split('\n') : [])

  $effect(() => {
    if (!lookup) {
      return
    }
    return lookup.register(lookupTabId, (query, direction) => {
      const pattern = query ? ({ kind: 'substring', query } as const) : null
      search = advanceSearch(search, pattern, direction)
      const total = countOccurrences(lines, pattern)
      const current = total > 0 ? (((search.occurrenceIndex % total) + total) % total) + 1 : 0
      return { current, total }
    })
  })
</script>

{#if !logText}
  <div class="bg-white-dark flex h-full items-center justify-center rounded font-mono">
    No logs available in this bundle
  </div>
{:else}
  <LogList {lines} {search} showLineNumbers class="bg-white-dark rounded" />
{/if}
