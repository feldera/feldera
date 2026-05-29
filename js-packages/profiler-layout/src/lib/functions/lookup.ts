/**
 * Coordinates the single "search within the active tab" input shared by the
 * analysis panel (the tabbed Metrics / Logs / Issues panel of the profile
 * viewer; see SupportBundleViewerLayout.svelte).
 *
 * Only one tab is visible at a time, but they don't know about each other. Each
 * tab calls `register(tabId, fn)` on mount to say "when the user searches while
 * I'm active, call this." The layout calls `execute(activeTabId, query)` when
 * the user submits the input; the coordinator forwards the query to just the
 * active tab's handler. This keeps the tabs decoupled — the input lives in the
 * layout, the search behavior lives in each tab.
 */
export function createLookupCoordinator() {
  const handlers = new Map<string, (query: string) => void>()

  return {
    register(tabId: string, fn: (query: string) => void): () => void {
      handlers.set(tabId, fn)
      return () => handlers.delete(tabId)
    },
    execute(activeTabId: string, query: string) {
      handlers.get(activeTabId)?.(query)
    }
  }
}

export type LookupCoordinator = ReturnType<typeof createLookupCoordinator>
