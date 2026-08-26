import {
  isBundlePickerSupported,
  pickSupportBundle,
  readSupportBundle
} from '$lib/services/supportBundleHistory'
import { useSupportBundleHistory } from './useSupportBundleHistory.svelte'

/**
 * A support bundle the user chose.
 *
 * `bundleId` names the history entry, which lets the profile viewer read the archive
 * again later: after a reload, or in a tab opened days afterwards. Without a
 * `bundleId` (an archive too big to copy, or a history write that failed) the bundle
 * exists only as the bytes `read` returns, once.
 */
export type PickedBundle = {
  name: string
  bundleId?: number
  read: () => Promise<Uint8Array>
}

/**
 * Choosing a support bundle from disk, in one place. The File System Access picker
 * is preferred, since its handle costs the history almost nothing; where that API is
 * missing, callers fall back to a plain file input. Either way the bundle lands in
 * the history.
 */
export const useBundlePicker = () => {
  const history = useSupportBundleHistory()

  return {
    /** Whether `pick` is available; if not, callers open a file input. */
    get isSupported() {
      return isBundlePickerSupported()
    },

    /**
     * Shows the file picker and remembers what came back. Resolves to null when the
     * browser has no picker or the user dismissed it.
     *
     * The promise waits for the history write, so the caller holds `bundleId` before
     * it opens the viewer tab: `window.open` works only inside a click handler, and a
     * click handler cannot await the write itself.
     */
    async pick(): Promise<PickedBundle | null> {
      if (!isBundlePickerSupported()) {
        return null
      }
      const handle = await pickSupportBundle()
      if (!handle) {
        return null
      }
      const remembered = await history.remember(handle)
      return {
        name: handle.name,
        bundleId: remembered?.id,
        read: () => readSupportBundle(handle)
      }
    },

    /**
     * Wraps a file from an `<input type=file>`, which yields no handle, and remembers
     * it by keeping a copy. The copy gives browsers without a picker a history.
     *
     * The promise waits for the history write, as in `pick`.
     */
    async fromFile(file: File): Promise<PickedBundle> {
      const remembered = await history.rememberFile(file)
      return {
        name: file.name,
        bundleId: remembered?.id,
        read: async () => new Uint8Array(await file.arrayBuffer())
      }
    }
  }
}
