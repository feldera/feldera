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
 * A remembered bundle plus whether reading it needs the user's permission.
 *
 * The permission is queried when the list is read, not when a bundle is opened:
 * requesting it takes a click, so a caller has to know the answer before it can
 * decide whether to ask for one.
 */
export type SupportBundleEntry = StoredSupportBundle & { needsPermission: boolean }

/**
 * Reactive view of the support bundles remembered in IndexedDB.
 *
 * The state is module-level, like `useDemos`: every caller reads the same list,
 * so a bundle opened in one component shows up in the others without its own
 * trip to the database. The first caller triggers the initial read.
 *
 * IndexedDB failures (private-mode restrictions, a corrupt database) leave the
 * list empty rather than propagating: the history is a convenience, and losing
 * it must not stop the user from opening a bundle from disk.
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

/** Re-reads the history from IndexedDB. Exposed for tests and for the first read. */
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
    /** Records a picked bundle and returns its entry, or null if it cannot be stored. */
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
     * Records a bundle that came from a file input, by keeping a copy of it.
     * Returns null when there is no room for the copy, or when the storage quota
     * refuses it. The bundle still opens; it just leaves no history entry.
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
     * Asks for read access to this bundle again; browsers drop file grants between
     * sessions. MUST be called from a user-gesture handler.
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
