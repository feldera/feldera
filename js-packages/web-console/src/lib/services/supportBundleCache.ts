/**
 * Copies of support bundle archives, kept in the same IndexedDB store as the history
 * (see `supportBundleHistory.ts`).
 *
 * Browsers with no File System Access API give an `<input type=file>` a `File`, and a
 * `File` cannot be re-opened from its path once the page is gone. Remembering such a
 * bundle therefore means keeping the archive itself. The two limits below cap what
 * that costs in storage. A bundle over either limit still opens; it gets no history
 * entry.
 *
 * Nothing in the file-picker path reads this module, and `supportBundleHistory` does
 * not import it.
 */

import {
  type CachedSupportBundle,
  deleteSupportBundles,
  isBundlePickerSupported,
  isHistorySupported,
  listSupportBundles,
  maxRememberedBundles,
  pruneToCountLimit,
  putSupportBundle,
  type StoredSupportBundle
} from './supportBundleHistory'

/**
 * Whether remembering a bundle means keeping a copy of the archive, because this
 * browser has no file picker and so yields no handles.
 *
 * False where IndexedDB is unusable, since there is nowhere to keep the copy.
 */
export const isBundleCacheRequired = () => isHistorySupported() && !isBundlePickerSupported()

/**
 * Biggest archive this module will copy, in bytes. A larger bundle gets no history
 * entry, because one archive filling the origin's storage quota costs the user more
 * than the history is worth.
 */
export const maxCachedBundleBytes = 256 * 1024 * 1024

/**
 * Bytes all the copies together may occupy. Over the budget, the least recently
 * opened copies are deleted, so a browser that copies archives remembers fewer
 * bundles than one that stores handles. The budget exceeds `maxCachedBundleBytes`, so
 * the pruning pass after a write never deletes the bundle that write just added.
 */
export const cachedBundlesByteBudget = 512 * 1024 * 1024

export const isCachedBundle = (bundle: StoredSupportBundle): bundle is CachedSupportBundle =>
  'file' in bundle

/**
 * The copies that do not fit in the byte budget, in the order `mostRecentFirst` gives
 * them. Entries holding a handle occupy no budget and are skipped.
 *
 * @param mostRecentFirst the history in the order `listSupportBundles` returns.
 *   Exported so tests can exercise the budget without storing half a gigabyte.
 */
export const cachedBundlesOverBudget = (
  mostRecentFirst: StoredSupportBundle[]
): StoredSupportBundle[] => {
  const excess: StoredSupportBundle[] = []
  let cachedBytes = 0
  for (const bundle of mostRecentFirst.slice(0, maxRememberedBundles)) {
    if (!isCachedBundle(bundle)) {
      continue
    }
    cachedBytes += bundle.file.size
    if (cachedBytes > cachedBundlesByteBudget) {
      excess.push(bundle)
    }
  }
  return excess
}

/**
 * The existing copy of `file`, if there is one.
 *
 * A `File` has no identity test, so name, size and last-modified date stand in for
 * one. Two different files agreeing on all three are treated as one, which at worst
 * leaves the user a stale entry.
 */
const findCachedFile = async (file: File): Promise<StoredSupportBundle | undefined> =>
  (await listSupportBundles()).find(
    (bundle) =>
      isCachedBundle(bundle) &&
      bundle.file.name === file.name &&
      bundle.file.size === file.size &&
      bundle.file.lastModified === file.lastModified
  )

/**
 * Writes a copy of `file` to IndexedDB, or returns null if the web origin's storage quota refused it.
 *
 * The quota is a third limit on top of the two above, and it depends on how much disk
 * the browser has left, so no check up front can rule it out. A refusal here means the
 * same to the caller as an archive that is too big to copy.
 */
const storeCopy = async (id: number | undefined, file: File) => {
  try {
    return await putSupportBundle(id, { name: file.name, openedAt: Date.now(), file })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'QuotaExceededError') {
      return null
    }
    throw error
  }
}

/**
 * Records a bundle that came from an `<input type=file>` by keeping a copy of the
 * archive. Returns null when the copy does not fit, which leaves the bundle out of
 * the history.
 *
 * Adding a copy is the only thing that can put the store over its byte budget, so the
 * budget is applied here.
 */
export const rememberSupportBundleFile = async (
  file: File
): Promise<StoredSupportBundle | null> => {
  if (file.size > maxCachedBundleBytes) {
    return null
  }
  const existing = await findCachedFile(file)
  const stored = await storeCopy(existing?.id, file)
  if (!stored) {
    return null
  }
  await pruneToCountLimit()
  await deleteSupportBundles(
    cachedBundlesOverBudget(await listSupportBundles()).map((bundle) => bundle.id)
  )
  return stored
}
