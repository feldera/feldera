<script lang="ts">
  import { advanceSearch, emptySearchState, LogList, type SearchState } from 'common-ui'
  import type { LookupCoordinator } from '../functions/lookup'

  interface Props {
    logText: string | undefined
    /** Optional cross-tab search router. When provided, this view registers itself under
     *  `lookupTabId` (default `'Logs'`) and folds incoming queries into its own
     *  {@link SearchState} via {@link advanceSearch} — same pattern → next match, new
     *  pattern → first match, empty query → clear. */
    lookup?: LookupCoordinator
    lookupTabId?: string
    /** Forwarded to {@link LogList}; invoked on Ctrl-F / Cmd-F inside the list so the host
     *  can focus its search input. */
    onSearchShortcut?: () => void
  }

  let { logText, lookup, lookupTabId = 'Logs', onSearchShortcut }: Props = $props()

  // SearchState lives here so any host that uses BundleLogsView
  // gets the search-on-tab behaviour through LookupCoordinator.
  // LogList is purely the renderer; it accepts the state as a prop.
  let search: SearchState = $state(emptySearchState)

  $effect(() => {
    if (!lookup) {
      return
    }
    return lookup.register(lookupTabId, (query) => {
      search = advanceSearch(search, query ? { kind: 'substring', query } : null)
    })
  })

  // LogList expects pre-split lines — the bundle view's source is a single string blob.
  const lines = $derived(logText ? logText.split('\n') : [])
</script>

{#if !logText}
  <div class="bg-white-dark flex h-full items-center justify-center rounded font-mono">
    No logs available in this bundle
  </div>
{:else}
  <LogList {lines} {search} {onSearchShortcut} showLineNumbers class="bg-white-dark rounded" />
{/if}
