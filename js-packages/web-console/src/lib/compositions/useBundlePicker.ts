import {
  addToBundleHistory,
  isBundlePickerSupported,
  pickSupportBundle,
  type PickedDiskFile
} from '$lib/services/supportBundleHistory'

/**
 * A support bundle the user chose.
 *
 * `bundleId` names its entry in the history, which is what lets the profile viewer
 * read the archive again later, after a reload or in a tab opened days afterwards.
 * Without a `bundleId` the bundle exists only as the bytes `read` returns, and only
 * once: that is what becomes of an archive too large to copy, and of one the history
 * failed to write.
 */
export type PickedBundle = {
  name: string
  bundleId?: number
  read: () => Promise<Uint8Array>
}

/**
 * Reads what the picker handed back. This is the file the user just chose, so the
 * browser grants the read without asking, whichever of the two forms it took.
 */
const readPicked = async (picked: PickedDiskFile): Promise<Uint8Array> => {
  const file = picked instanceof File ? picked : await picked.getFile()
  return new Uint8Array(await file.arrayBuffer())
}

/**
 * Adds the chosen file to the history and describes it for the caller. A history that
 * cannot be written leaves `bundleId` unset, and the bundle still opens: the caller
 * holds the bytes either way, it just cannot reopen them in a later tab.
 */
const toPickedBundle = async (picked: PickedDiskFile): Promise<PickedBundle> => {
  let bundleId: number | undefined
  try {
    bundleId = (await addToBundleHistory(picked))?.id
  } catch (e) {
    console.warn('Failed to add the support bundle to the history:', e)
  }
  return { name: picked.name, bundleId, read: () => readPicked(picked) }
}

/**
 * Choosing a support bundle from disk, in one place. `showOpenFilePicker` is used
 * wherever the browser has it, because the handle it gives back costs the history a
 * few hundred bytes however large the archive is. Where it is missing, the caller
 * falls back to an `<input type=file>` and hands the file to `fromFile` below. Either
 * way the bundle is recorded in the history.
 */
export const useBundlePicker = () => ({
  /**
   * Whether `pick` can be used at all. Where it cannot, the caller clicks a hidden
   * `<input type=file>` instead and passes the chosen file to `fromFile`.
   */
  get isSupported() {
    return isBundlePickerSupported()
  },

  /**
   * Shows the file picker and remembers the file that comes back. Resolves to null
   * when the browser has no `showOpenFilePicker`, and when the user dismisses the
   * picker without choosing anything.
   *
   * The promise resolves only once the history has been written, so that the caller
   * already holds `bundleId` when the user clicks to open the viewer tab. A browser
   * allows `window.open` only while it is handling that click, and waiting for the
   * write there would outlast it.
   */
  async pick(): Promise<PickedBundle | null> {
    if (!isBundlePickerSupported()) {
      return null
    }
    const handle = await pickSupportBundle()
    return handle && toPickedBundle(handle)
  },

  /**
   * Takes a file from an `<input type=file>`, which comes with no handle, and
   * remembers it by keeping a copy of the archive. That copy is what gives a browser
   * without `showOpenFilePicker` a history at all.
   *
   * As in `pick`, the promise resolves only once the history has been written.
   */
  fromFile(file: File): Promise<PickedBundle> {
    return toPickedBundle(file)
  }
})
