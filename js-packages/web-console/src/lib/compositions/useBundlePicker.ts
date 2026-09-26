import {
  addToBundleHistory,
  isBundlePickerSupported,
  type PickedDiskFile,
  pickSupportBundle
} from '$lib/services/supportBundleHistory'

/** A support bundle the user chose. */
export type PickedBundle = {
  /** The file name of the archive. */
  name: string
  /**
   * The bundle's entry in the history, which lets the profile viewer reopen it later.
   * Unset when the history could not store the bundle.
   */
  bundleId?: number
  /** Reads the archive's bytes from the chosen file. */
  read: () => Promise<Uint8Array>
}

/**
 * Reads the file the user just chose: either a `File` from an `<input type=file>` or a
 * `FileSystemFileHandle` from `showOpenFilePicker`. The browser grants either read
 * without prompting, because the user chose the file moments ago.
 */
const readPicked = async (picked: PickedDiskFile): Promise<Uint8Array> => {
  const file = picked instanceof File ? picked : await picked.getFile()
  return new Uint8Array(await file.arrayBuffer())
}

/**
 * Adds the chosen file to the history and returns it as a `PickedBundle`. If the
 * history write fails, `bundleId` stays unset: the bundle still opens now, but a later
 * tab cannot reopen it.
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
 * Lets the user choose a support bundle from disk and records it in the history.
 *
 * Chromium-based browsers provide `showOpenFilePicker`, which returns a
 * `FileSystemFileHandle`. The history stores that handle in a few hundred bytes,
 * regardless of the archive's size. Firefox and Safari lack `showOpenFilePicker`, so
 * there the caller uses an `<input type=file>` and passes the file to `fromFile`, and
 * the history stores a copy of the archive instead.
 */
export const useBundlePicker = () => ({
  /**
   * Whether the browser can return a `FileSystemFileHandle` for a picked file.
   */
  get isSupported() {
    return isBundlePickerSupported()
  },

  /**
   * Shows the file picker and remembers the file that comes back. Resolves to null
   * when the browser has no `showOpenFilePicker`, or when the user dismisses the
   * picker without choosing anything.
   *
   * The caller needs `bundleId` before the user clicks to open the viewer tab, so the
   * promise waits for the history write to finish before it resolves. The click
   * handler must call `window.open` synchronously: if it first awaited the write, the
   * browser would block the new tab as a popup.
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
