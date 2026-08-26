/**
 * Copies of support bundle archives, kept in the same IndexedDB store the history
 * uses (see `supportBundleHistory.ts`).
 *
 * The cache serves browsers with no File System Access API: an `<input type=file>`
 * yields a `File`, which cannot be re-opened from its path once the page is gone, so
 * remembering such a bundle means keeping the archive itself. Copies cost storage,
 * hence the two limits below; a bundle past them still opens, it just leaves no
 * history entry.
 *
 * The picker path needs nothing from here, and the history module depends on nothing
 * here.
 */

import {
  type CachedSupportBundle,
  deleteSupportBundles,
  isBundlePickerSupported,
  listSupportBundles,
  maxRememberedBundles,
  pruneToCountLimit,
  putSupportBundle,
  type StoredSupportBundle
} from './supportBundleHistory'

/**
 * Whether remembering a bundle means caching the archive, because this browser hands
 * out no file handles.
 */
export const isBundleCacheRequired = () => !isBundlePickerSupported()

/**
 * Biggest archive the cache takes. A bigger bundle is not remembered: filling the
 * origin's storage quota with one archive costs the user more than the history is
 * worth.
 */
export const maxCachedBundleBytes = 256 * 1024 * 1024

/**
 * What all the copies together may take. Past the budget the oldest copies are
 * dropped, so a browser that caches archives keeps fewer bundles than one storing
 * handles. The budget is above `maxCachedBundleBytes`, so the bundle just opened
 * survives its own pruning pass.
 */
export const cachedBundlesByteBudget = 512 * 1024 * 1024

export const isCachedBundle = (bundle: StoredSupportBundle): bundle is CachedSupportBundle =>
  'file' in bundle

/**
 * The cached copies that exceed the byte budget, oldest first. Entries holding a
 * handle weigh nothing, so they are passed over.
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
 * Same file as `file`, if the cache already holds a copy of it.
 *
 * A `File` supports no identity test, so name, size and modification time stand in
 * for one. Two files that agree on all three count as one file, which costs the user
 * a stale entry at worst.
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
 * Records a bundle that came from an `<input type=file>` by caching the archive.
 * Returns null for an archive too big to cache, which leaves it unremembered.
 *
 * Adding a copy is the only thing that can put the cache over its byte budget, so
 * the budget is enforced here.
 */
export const rememberSupportBundleFile = async (
  file: File
): Promise<StoredSupportBundle | null> => {
  if (file.size > maxCachedBundleBytes) {
    return null
  }
  const existing = await findCachedFile(file)
  const stored = await putSupportBundle(existing?.id, {
    name: file.name,
    openedAt: Date.now(),
    file
  })
  await pruneToCountLimit()
  await deleteSupportBundles(
    cachedBundlesOverBudget(await listSupportBundles()).map((bundle) => bundle.id)
  )
  return stored
}
