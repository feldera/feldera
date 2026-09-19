/**
 * Copies of support bundle archives, stored in IndexedDB.
 *
 * `supportBundleHistory.ts` remembers the support bundles the user opened from disk.
 * This file implements a feature where, when storing history through FileSystemFileHandle
 * is not possible, the opened files are cached instead.
 *
 * Browsers with the File System Access API let the history store a handle: an object
 * that points at the file on disk and can open it again later. Firefox and Safari do
 * not have that API. There the user picks the file with an `<input type=file>`, and
 * the page is handed a `File` and nothing else. A `File` says nothing about where on
 * disk it came from, and it stops working once the page is closed or reloaded, so the
 * only way to open that bundle a second time is to keep a copy of the archive bytes. The
 * functions below write those copies, into the same IndexedDB store the history uses.
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
 * Whether remembering a bundle in this browser means keeping a copy of the archive,
 * because the browser has no File System Access API and so hands the page a `File`
 * rather than a handle that can open the archive again.
 *
 * False when IndexedDB is unusable, since then there is nowhere to keep the copy.
 */
export const isBundleCacheRequired = () => isHistorySupported() && !isBundlePickerSupported()

/**
 * The largest archive, in bytes, that is copied into IndexedDB. A bundle larger than
 * this still opens, it just gets no history entry: one archive filling up the storage
 * the browser allows this site costs the user more than remembering it is worth.
 */
export const maxCachedBundleBytes = 256 * 1024 * 1024

/**
 * How many bytes all the copies together may occupy. Over that, the copies of the
 * bundles opened longest ago are deleted, so a browser that keeps copies remembers
 * fewer bundles than one that stores handles.
 *
 * The budget is larger than `maxCachedBundleBytes`, so the deletion pass that runs
 * after a copy is written never deletes the copy that write just added.
 */
export const cachedBundlesByteBudget = 512 * 1024 * 1024

/** Whether this history entry holds a copy of the archive rather than a handle. */
export const isCachedBundle = (bundle: StoredSupportBundle): bundle is CachedSupportBundle =>
  'file' in bundle

/**
 * The entries whose copies do not fit in `cachedBundlesByteBudget`, for the caller to
 * delete. Entries holding a handle take up no space worth counting and are skipped.
 *
 * @param mostRecentFirst the history, ordered as `listSupportBundles` returns it. The
 *   result keeps that order. Taking the list as an argument rather than reading it
 *   here lets the tests check the arithmetic without writing half a gigabyte.
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
 * The entry already holding a copy of `file`, if there is one.
 *
 * Two `File` objects cannot be asked whether they came from the same file on disk, so
 * the name, the size and the last-modified date are compared instead. Two different
 * files agreeing on all three are taken for one file, and the worst that does is show
 * the user a history entry that opens the older contents.
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
 * Writes a copy of `file` into IndexedDB, or returns null when the browser refused
 * the write because this site has used up the storage it is allowed.
 *
 * How much that is depends on the free space on the machine, so nothing checked
 * beforehand can tell whether a given archive will fit. A refusal means the same to
 * the caller as an archive too large to copy: no history entry, and the bundle the
 * user just picked still opens.
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
 * Remembers a bundle the user picked with an `<input type=file>`, by keeping a copy
 * of the archive. Returns null when the copy does not fit, and the bundle is then
 * left out of the history.
 *
 * Writing a copy is the only thing that can put the store over
 * `cachedBundlesByteBudget`, so the budget is applied here.
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
