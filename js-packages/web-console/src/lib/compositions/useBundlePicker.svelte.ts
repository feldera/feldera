import {
  isBundlePickerSupported,
  pickSupportBundle,
  readSupportBundle
} from '$lib/services/supportBundleHistory'
import { useSupportBundleHistory } from './useSupportBundleHistory.svelte'

/**
 * A support bundle the user chose, however they chose it.
 *
 * `bundleId` names the history entry, which is what lets the profile viewer read
 * the archive again later: on a reload, or in a tab opened days afterwards.
 * Without it (an archive too big to copy, or a history that could not be written)
 * the bundle exists only as the bytes `read` returns, once.
 */
export type PickedBundle = {
  name: string
  bundleId?: number
  read: () => Promise<Uint8Array>
}

/**
 * Choosing a support bundle from disk, in one place. Every entry point prefers
 * the File System Access picker, whose handle costs the history almost nothing,
 * and falls back to a plain file input where that API is missing. Either way the
 * bundle lands in the history.
 */
export const useBundlePicker = () => {
  const history = useSupportBundleHistory()

  return {
    /** Whether `pick` is available; if not, callers open a file input. */
    get isSupported() {
      return isBundlePickerSupported()
    },

    /**
     * Shows the file picker and remembers what came back. Resolves to null when
     * the browser has no picker or the user dismissed it.
     *
     * The returned promise waits for the history write, so the caller holds
     * `bundleId` before it opens the viewer tab: that tab is opened with
     * `window.open`, which works only inside a click handler and cannot await the
     * write itself.
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
     * Wraps a file from an `<input type=file>`, which yields no handle, and
     * remembers it by keeping a copy. The copy is what gives browsers without the
     * picker a history.
     *
     * The returned promise waits for the history write, so the caller holds
     * `bundleId` before it opens the viewer tab (see `pick`).
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
