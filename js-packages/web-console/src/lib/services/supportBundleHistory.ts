/**
 * The history of support bundles the user opened from disk, and the API the rest of
 * the application calls.
 *
 * Every entry records how to read its archive again. `supportBundleStorage.ts` holds
 * the two ways of doing that, and `supportBundleStore.ts` one level down keeps the records
 * in IndexedDB. This module provides access to history entries.
 *
 * Each record is matched against the two ways of storing a bundle once, as it comes
 * out of the database. A `BundleHistoryEntry` is therefore known to be readable, and
 * no caller has to ask which of the two kinds it holds.
 *
 * `listBundleHistory` reads the history once. `observeBundleHistory` keeps handing it
 * back as it changes, in this tab and in the others.
 */

import {
  bundleOps,
  isSameCachedFile,
  isSameHandle,
  type StoredBundleOps
} from './supportBundleStorage'

export { isPermissionRequired } from './supportBundleStorage'

import {
  deleteBundleRecords,
  getBundleRecord,
  isQuotaExceeded,
  listBundleRecords,
  type NewSupportBundle,
  type Observable,
  observeBundleRecords,
  putBundleRecord,
  type StoredSupportBundle,
  type SupportBundleMetadata
} from './supportBundleStore'

export type { StoredBundleOps, Observable }
export { clearBundleRecords as clearBundleHistory } from './supportBundleStore'

/**
 * How many bundles are remembered. The history is an LRU cache: once it is full, the
 * entry opened longest ago is dropped to make room.
 */
export const maxHistoryEntries = 30

/**
 * The largest archive, in bytes, that is copied into IndexedDB. A bundle over this
 * size still opens; it just gets no history entry.
 */
export const maxCachedBundleBytes = 256 * 1024 * 1024

/**
 * How many bytes all the entries together may occupy. Past that, the same LRU
 * eviction applies, so a browser that caches copies of the archives remembers fewer
 * bundles than one that stores handles.
 *
 * The budget is larger than `maxCachedBundleBytes`, which is what keeps the deletion
 * pass after a write from deleting the copy that write just added.
 */
export const cachedBundlesByteBudget = 512 * 1024 * 1024

type WindowWithFilePicker = Window & {
  showOpenFilePicker?: (options?: {
    multiple?: boolean
    types?: { description?: string; accept: Record<string, string[]> }[]
  }) => Promise<FileSystemFileHandle[]>
}

/**
 * Whether this browser has `showOpenFilePicker`, and so whether picking a file gives
 * back a handle that can be remembered. False in Firefox and Safari, where the caller
 * offers the user an `<input type=file>` and is handed a plain `File` instead.
 */
export const isBundlePickerSupported = () =>
  typeof window !== 'undefined' &&
  typeof (window as WindowWithFilePicker).showOpenFilePicker === 'function'

/**
 * Whether a history can be kept at all. False where IndexedDB is missing or turned
 * off, as in a Chromium with site data blocked, and there every read and every write
 * below throws.
 *
 * A separate question from `isBundlePickerSupported`: a browser can have
 * `showOpenFilePicker` and still have no usable IndexedDB.
 */
export const isHistorySupported = () => typeof indexedDB !== 'undefined'

/**
 * Whether adding an entry in this browser means copying the whole archive into
 * IndexedDB, because the browser has no `showOpenFilePicker` and so hands the page a
 * `File` rather than a handle that can open the archive again.
 *
 * False when IndexedDB is unusable, since then there is nowhere to put the copy.
 */
export const isBundleCacheRequired = () => isHistorySupported() && !isBundlePickerSupported()

/**
 * Shows the file picker through the File System Access API, so that what comes back is
 * a handle rather than a `File`. Resolves to null when the user dismisses the picker,
 * which the API reports by throwing an `AbortError` rather than by returning nothing.
 */
export const pickSupportBundle = async (): Promise<FileSystemFileHandle | null> => {
  const showOpenFilePicker = (window as WindowWithFilePicker).showOpenFilePicker
  if (!showOpenFilePicker) {
    return null
  }
  try {
    const [handle] = await showOpenFilePicker({
      multiple: false,
      types: [{ description: 'Support bundle', accept: { 'application/zip': ['.zip'] } }]
    })
    return handle ?? null
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      return null
    }
    throw e
  }
}

/** One entry of the history: what the bundle is, and the operations that read it. */
export type BundleHistoryEntry = SupportBundleMetadata & { ops: StoredBundleOps }

const toHistoryEntry = (record: StoredSupportBundle): BundleHistoryEntry | undefined => {
  const ops = bundleOps(record)
  return ops && { id: record.id, name: record.name, openedAt: record.openedAt, ops }
}

/**
 * The whole history, most recently opened first.
 *
 * A record that is neither a reference to a file nor a cached copy is left out, and
 * deleted in the background. No version of this code can read one, so keeping it
 * would leave a row in the user's database that nothing will ever use.
 */
export const listBundleHistory = async (): Promise<BundleHistoryEntry[]> => {
  const entries: BundleHistoryEntry[] = []
  const unreadable: number[] = []
  for (const record of await listBundleRecords()) {
    const entry = toHistoryEntry(record)
    if (entry) {
      entries.push(entry)
    } else {
      unreadable.push(record.id)
    }
  }
  dropUnreadableRecords(unreadable)
  return entries
}

/**
 * Deletes unreadable records, on a later turn of the event loop. A failure to delete
 * them must not fail the read that found them, so the caller does not wait for it.
 */
const dropUnreadableRecords = (ids: number[]) => {
  if (!ids.length) {
    return
  }
  console.warn('Dropping unreadable support bundle history entries:', ids)
  setTimeout(async () => {
    // Dexie refuses a write inside a `liveQuery` querier, since a query that changed the
    // database would re-run itself forever; a deferred write is outside that querier,
    // and the re-read it does trigger finds nothing left to delete.
    try {
      await deleteBundleRecords(ids)
    } catch (error) {
      console.warn('Failed to drop unreadable support bundle history entries:', error)
    }
  })
}

/**
 * The whole history, handed to the caller again after every change to it, whether the
 * change was made in this tab or in another one.
 */
export const observeBundleHistory = (): Observable<BundleHistoryEntry[]> =>
  observeBundleRecords(listBundleHistory)

/**
 * Looks up one entry by its id.
 *
 * Throws when there is no readable entry under that id, which is what a stale link
 * or a cleared history looks like.
 *
 * Whether reading the entry needs the user's permission is not reported here, and
 * nothing asks the browser for it. A caller holding a user gesture calls
 * `ops.requestPermission`, which is silent when permission has already been given;
 * one with no gesture reads and passes the failure to `isPermissionRequired`.
 */
export const resolveStoredBundle = async (id: number | undefined): Promise<BundleHistoryEntry> => {
  const record = id === undefined ? undefined : await getBundleRecord(id)
  const entry = record && toHistoryEntry(record)
  if (!entry) {
    throw new Error(
      'This support bundle is no longer in the browser history. Open it from disk again.'
    )
  }
  return entry
}

/**
 * The records that do not fit in `cachedBundlesByteBudget`, for the caller to delete.
 * Records that occupy nothing are left alone, since deleting them would free nothing.
 *
 * @param mostRecentFirst the records, ordered as `listBundleRecords` returns them. The
 *   result keeps that order. Taking the list as an argument rather than reading it
 *   here lets the tests check the arithmetic without writing half a gigabyte.
 */
export const bundlesOverByteBudget = (
  mostRecentFirst: StoredSupportBundle[]
): StoredSupportBundle[] => {
  const excess: StoredSupportBundle[] = []
  let total = 0
  for (const record of mostRecentFirst.slice(0, maxHistoryEntries)) {
    const bytes = bundleOps(record)?.bytes() ?? 0
    if (bytes === 0) {
      continue
    }
    total += bytes
    if (total > cachedBundlesByteBudget) {
      excess.push(record)
    }
  }
  return excess
}

/** Deletes the entries past `maxHistoryEntries`, counting from the most recent. */
const pruneToCountLimit = async () => {
  const records = await listBundleRecords()
  await deleteBundleRecords(records.slice(maxHistoryEntries).map((record) => record.id))
}

/** Deletes the records that no longer fit in `cachedBundlesByteBudget`. */
const pruneToByteBudget = async () => {
  const over = bundlesOverByteBudget(await listBundleRecords())
  await deleteBundleRecords(over.map((record) => record.id))
}

/** The first record `isSameFile` accepts, searching from the most recently opened. */
const findRecord = async (
  isSameFile: (record: StoredSupportBundle) => boolean | Promise<boolean>
) => {
  for (const record of await listBundleRecords()) {
    if (await isSameFile(record)) {
      return record
    }
  }
  return undefined
}

/**
 * Writes `entry`, or returns null when the browser refused the write because this
 * site has used up its storage quota.
 *
 * The size of that quota depends on the free space on the machine, so no check
 * beforehand can tell whether a given archive will fit.
 */
const putWithinQuota = async (id: number | undefined, entry: NewSupportBundle) => {
  try {
    return await putBundleRecord(id, entry)
  } catch (error) {
    if (isQuotaExceeded(error)) {
      return null
    }
    throw error
  }
}

/**
 * Writes one entry, replacing the record `isSameFile` matches if there is one, and
 * trims the history around it. Returns null when the write was refused.
 */
const addHistoryEntry = async (
  isSameFile: (record: StoredSupportBundle) => boolean | Promise<boolean>,
  entry: NewSupportBundle
): Promise<BundleHistoryEntry | null> => {
  const existing = await findRecord(isSameFile)
  const record = await putWithinQuota(existing?.id, entry)
  if (!record) {
    return null
  }
  // Both passes run for either kind of bundle. A reference to a file occupies
  // nothing, so the byte pass does nothing to a history made only of those.
  await pruneToCountLimit()
  await pruneToByteBudget()
  // `addToBundleHistory` built this record, so it always matches one of the two
  // kinds. The fallback is here only to satisfy the type.
  return toHistoryEntry(record) ?? null
}

/**
 * What a file picker gives back: a `FileSystemFileHandle` in Chromium, and a plain
 * `File` in Firefox and Safari.
 */
export type PickedDiskFile = FileSystemFileHandle | File

/**
 * Adds the bundle the user just picked, as the most recently opened one. Picking a
 * file that is already in the history moves its entry to the front instead of adding
 * a second entry for it.
 *
 * Returns null when no entry could be added, which happens only for a `File`. Its
 * archive has to be copied into IndexedDB, and the copy can be too large or refused
 * for lack of quota. Either way the bundle the caller is holding still opens.
 */
export const addToBundleHistory = async (
  picked: PickedDiskFile
): Promise<BundleHistoryEntry | null> => {
  const openedAt = Date.now()
  if (!(picked instanceof File)) {
    return addHistoryEntry((record) => isSameHandle(record, picked), {
      name: picked.name,
      openedAt,
      handle: picked
    })
  }
  if (picked.size > maxCachedBundleBytes) {
    return null
  }
  return addHistoryEntry((record) => isSameCachedFile(record, picked), {
    name: picked.name,
    openedAt,
    file: picked
  })
}

/** Moves the entry to the front of the history, by recording that it was opened now. */
export const markBundleOpenedNow = async (id: number): Promise<void> => {
  const record = await getBundleRecord(id)
  if (!record) {
    return
  }
  await putBundleRecord(id, { ...record, openedAt: Date.now() })
}
