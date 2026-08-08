import type { SearchDirection, SearchProgress } from 'common-ui'

export type { SearchProgress }

/**
 * Coordinates the single "search within the active tab" input shared by the
 * analysis panel (the tabbed Metrics / Logs / Issues panel of the profile
 * viewer; see SupportBundleViewerLayout.svelte).
 *
 * Only one tab is visible at a time, but they don't know about each other. Each
 * tab calls `register(tabId, fn)` on mount to say "when the user searches while
 * I'm active, call this." The layout calls `execute(activeTabId, query, direction)`
 * when the user submits the input; the coordinator forwards to just the active
 * tab's handler and returns the handler's match count, which the layout uses to
 * enable/disable the search nav buttons. This keeps the tabs decoupled — the input
 * lives in the layout, the search behavior lives in each tab.
 */

/** No match — the empty result returned for an unknown tab. */
export const noMatches: SearchProgress = { current: 0, total: 0 }

/** A tab's search handler: move the cursor in `direction` for `query` and report the resulting
 *  position. */
export type LookupHandler = (query: string, direction: SearchDirection) => SearchProgress

export function createLookupCoordinator() {
  const handlers = new Map<string, LookupHandler>()

  return {
    register(tabId: string, fn: LookupHandler): () => void {
      handlers.set(tabId, fn)
      return () => handlers.delete(tabId)
    },
    /** Forward the query to the active tab's handler; returns its match position, or
     *  {@link noMatches} when no tab is registered under `activeTabId`. */
    execute(
      activeTabId: string,
      query: string,
      direction: SearchDirection = 'next'
    ): SearchProgress {
      return handlers.get(activeTabId)?.(query, direction) ?? noMatches
    }
  }
}

export type LookupCoordinator = ReturnType<typeof createLookupCoordinator>
