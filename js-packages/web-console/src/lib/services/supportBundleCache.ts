/**
 * Copies of support bundle archives, kept in the same IndexedDB store the history
 * uses (see `supportBundleHistory.ts`).
 *
 * The cache exists for browsers with no File System Access API: an
 * `<input type=file>` yields a `File`, which cannot be re-opened from its path
 * once the page is gone, so the only way to remember such a bundle is to keep the
 * archive itself. That costs storage, hence the two limits below; a bundle past
 * them is opened all the same, it just leaves no history entry.
 *
 * Nothing here is needed where the picker exists, and nothing in the history
 * module depends on this one.
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
 * Whether remembering a bundle means caching the archive, because this browser
 * hands out no file handles. A feature check behind a name, so the interface can
 * say so without repeating the detection.
 */
export const isBundleCacheRequired = () => !isBundlePickerSupported()

/**
 * Biggest archive the cache takes. Past it a bundle is not remembered at all:
 * filling the origin's storage quota with one archive would cost the user more
 * than the history is worth.
 */
export const maxCachedBundleBytes = 256 * 1024 * 1024

/**
 * What all the copies together may take. The oldest are dropped past it, so a
 * browser that has to cache archives keeps fewer bundles than one storing
 * handles. Kept above `maxCachedBundleBytes` so the bundle just opened always
 * survives its own pruning pass.
 */
export const cachedBundlesByteBudget = 512 * 1024 * 1024

export const isCachedBundle = (bundle: StoredSupportBundle): bundle is CachedSupportBundle =>
  'file' in bundle

/**
 * The cached copies that no longer fit the byte budget, oldest first. Entries
 * holding a handle weigh nothing, so they are passed over.
 *
 * @param mostRecentFirst the history in the order `listSupportBundles` returns.
 *   Exported to let the budget be exercised without storing half a gigabyte to
 *   reach it.
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
 * A `File` supports no identity test at all, so name, size and modification time
 * stand in for one. Two different files that agree on all three are treated as
 * one, which costs the user a stale tile at worst.
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
 * this is where the budget is enforced.
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
