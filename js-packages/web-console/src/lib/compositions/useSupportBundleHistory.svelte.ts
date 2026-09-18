import { rememberSupportBundleFile } from '$lib/services/supportBundleCache'
import {
  clearSupportBundles,
  listSupportBundles,
  queryBundleReadPermission,
  rememberSupportBundle,
  requestBundleReadPermission,
  type StoredSupportBundle,
  touchSupportBundle
} from '$lib/services/supportBundleHistory'

/**
 * A remembered bundle, plus whether reading it needs the user's permission.
 *
 * The permission is looked up when the list is read rather than when a bundle is
 * opened. Asking the user for it has to happen inside a click handler, so a caller has
 * to know the answer before it draws the button that would do the asking.
 */
export type SupportBundleEntry = StoredSupportBundle & { needsPermission: boolean }

/**
 * The support bundles remembered in IndexedDB, as reactive state.
 *
 * The state lives at module level, as it does in `useDemos`: every caller reads the
 * same list, so a bundle opened in one component shows up in the others without
 * another trip to the database. The first caller starts the initial read.
 *
 * A failure to read IndexedDB, in a private window or with a database that cannot be
 * opened, leaves the list empty rather than being passed on to the caller. The history
 * is a convenience, and losing it must not stop the user opening a bundle from disk.
 */
let bundles = $state<SupportBundleEntry[]>([])
let loaded = false

const refresh = async () => {
  try {
    const stored = await listSupportBundles()
    bundles = await Promise.all(
      stored.map(async (bundle) => ({
        ...bundle,
        needsPermission: (await queryBundleReadPermission(bundle)) !== 'granted'
      }))
    )
  } catch (e) {
    console.warn('Failed to read the support bundle history:', e)
    bundles = []
  }
}

/** Re-reads the history from IndexedDB. Exported for the tests and for the first read. */
export const loadSupportBundleHistory = () => refresh()

export const useSupportBundleHistory = () => {
  if (!loaded) {
    loaded = true
    refresh()
  }
  return {
    get current() {
      return bundles
    },
    /**
     * Remembers a bundle the user chose in the file picker and returns its history
     * entry, or null when the entry could not be written.
     */
    async remember(handle: FileSystemFileHandle) {
      try {
        const bundle = await rememberSupportBundle(handle)
        await refresh()
        return bundle
      } catch (e) {
        console.warn('Failed to remember the support bundle:', e)
        return null
      }
    },
    /**
     * Remembers a bundle that came from an `<input type=file>`, by keeping a copy of
     * the archive. Returns null when the archive is too large to copy, or when the
     * browser refused to store it. The user can open the bundle either way; without a
     * copy it simply gets no history entry.
     */
    async rememberFile(file: File) {
      try {
        const bundle = await rememberSupportBundleFile(file)
        await refresh()
        return bundle
      } catch (e) {
        console.warn('Failed to remember the support bundle:', e)
        return null
      }
    },
    /**
     * Asks the user for permission to read this bundle again, which browsers forget
     * from one visit to the next. MUST be called while handling a click: a browser
     * turns down a permission request that no user action can be attributed to.
     */
    async grantAccess(bundle: StoredSupportBundle) {
      const granted = await requestBundleReadPermission(bundle)
      if (granted) {
        await refresh()
      }
      return granted
    },
    /** Moves a bundle to the front of the history, as the most recently opened. */
    async touch(id: number) {
      try {
        await touchSupportBundle(id)
        await refresh()
      } catch (e) {
        console.warn('Failed to update the support bundle history:', e)
      }
    },
    async clear() {
      try {
        await clearSupportBundles()
      } catch (e) {
        console.warn('Failed to clear the support bundle history:', e)
      }
      await refresh()
    }
  }
}
