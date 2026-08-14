import {
  isBundlePickerSupported,
  pickSupportBundle,
  readSupportBundle
} from '$lib/services/supportBundleHistory'
import { useSupportBundleHistory } from './useSupportBundleHistory.svelte'

/**
 * A support bundle the user just chose, however they chose it.
 *
 * `bundleId` is the history entry, and it is what lets the profile viewer read
 * the archive again later - on a reload, or in a tab opened days afterwards.
 * Without it (an archive too big to copy, or a history that could not be written)
 * the bundle exists only as the bytes `read` returns, once.
 */
export type PickedBundle = {
  name: string
  bundleId?: number
  read: () => Promise<Uint8Array>
}

/**
 * Choosing a support bundle from disk, in one place: every entry point in the
 * app prefers the File System Access picker, whose handle the history keeps for
 * nothing, and falls back to a plain file input where that API is missing. Either
 * way the bundle lands in the history.
 */
export const useBundlePicker = () => {
  const history = useSupportBundleHistory()

  return {
    /** Whether `pick` can be used at all; if not, callers open a file input. */
    get isSupported() {
      return isBundlePickerSupported()
    },

    /**
     * Shows the file picker and remembers what came back. Resolves to null when
     * the browser has no picker or the user dismissed it.
     *
     * The history write is awaited: `bundleId` is what the viewer is opened
     * with, and a caller cannot wait for it later - `window.open` has to run
     * inside a click.
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
     * Wraps a file that came from an `<input type=file>`, which yields no handle,
     * and remembers it by keeping a copy. The copy is what gives browsers without
     * the picker a history at all, and it is awaited for the same reason `pick`
     * awaits its write: `bundleId` has to be known before the opening click.
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
