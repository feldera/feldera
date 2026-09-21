/**
 * The Dexie database that holds the support bundle history, and the reads and writes
 * against it.
 *
 * This module knows the record shape and the database, and nothing about how the
 * archive behind a record is read. `supportBundleStorage.ts` adds that, and
 * `supportBundleHistory.ts` is the API the rest of the application calls.
 *
 * The history needs IndexedDB rather than `localStorage`, which stores only JSON.
 * Neither a `FileSystemFileHandle` nor a `File` has a JSON form, and IndexedDB writes
 * both through structured clone.
 *
 * All tabs of this site share one database, so a bundle opened in one tab can be
 * reopened from another, and `observeBundleRecords` puts it on screen there without a
 * reload.
 */

import Dexie, { liveQuery, type Observable, type Table } from 'dexie'

export type { Observable }

const DB_NAME = 'feldera-support-bundles'

/** The fields every record has, whether its bundle is a reference to a file or a copy. */
export type SupportBundleMetadata = {
  /** The key of the record in IndexedDB. */
  id: number
  /** The name of the file the user picked. */
  name: string
  /**
   * When the bundle was last opened, in milliseconds since the epoch. The history is
   * ordered by this.
   */
  openedAt: number
}

/**
 * A bundle stored as a reference to the file on disk. Reading it goes back through
 * the handle, and needs the user's permission.
 */
export type LinkedSupportBundle = SupportBundleMetadata & { handle: FileSystemFileHandle }

/**
 * A bundle stored as a copy of the whole archive, for browsers that don't expose
 * `FileSystemFileHandle` API.
 */
export type CachedSupportBundle = SupportBundleMetadata & { file: File }

/**
 * A record of the object store.
 *
 * IndexedDB does no type checking on the way out, so a value read back matches this
 * type only as far as the code that wrote it did. A record matching neither variant is
 * what a past or a future version of this module leaves behind. `bundleOps` in
 * `supportBundleStorage.ts` establishes which of the two a record really is, and
 * `listBundleHistory` deletes the records that are neither.
 */
export type StoredSupportBundle = LinkedSupportBundle | CachedSupportBundle

/** A record about to be written, before IndexedDB has assigned it an id. */
export type NewSupportBundle = Omit<LinkedSupportBundle, 'id'> | Omit<CachedSupportBundle, 'id'>

class SupportBundleDatabase extends Dexie {
  /**
   * A write either carries an id, overwriting that record, or omits it and lets
   * IndexedDB assign one.
   */
  bundles!: Table<StoredSupportBundle, number, StoredSupportBundle | NewSupportBundle>

  constructor() {
    super(DB_NAME)
    // `++id` is the whole schema: an auto-incrementing primary key and no secondary
    // index. Nothing about a file makes a stable key of its own, and the history is
    // ordered on `openedAt` and the key together, which no single index expresses.
    this.version(1).stores({ bundles: '++id' })
  }
}

const db = new SupportBundleDatabase()

/**
 * Whether `error` is the browser refusing a write because this site has used up its
 * storage quota.
 *
 * Dexie reports the refusal through an error class of its own rather than passing on
 * the `DOMException` IndexedDB threw, so an `instanceof DOMException` test misses it.
 * Both carry the name.
 */
export const isQuotaExceeded = (error: unknown): boolean =>
  error instanceof Error && error.name === Dexie.errnames.QuotaExceeded

/**
 * Runs `read` now, and again after every change to this database, handing the caller
 * each result.
 *
 * Dexie tells the other tabs of this site about a change as well, so a bundle opened
 * in one tab reaches a history already on screen in another. The returned value
 * follows the Svelte store contract: `$bundles` in a component subscribes to it and
 * unsubscribes when the component goes away.
 */
export const observeBundleRecords = <T>(read: () => Promise<T>): Observable<T> => liveQuery(read)

/** All records, most recently opened first. */
export const listBundleRecords = async (): Promise<StoredSupportBundle[]> => {
  const records = await db.bundles.toArray()
  // Two bundles opened in the same millisecond fall back to their key, which puts
  // the one written later first.
  return records.sort((a, b) => b.openedAt - a.openedAt || b.id - a.id)
}

export const getBundleRecord = (id: number): Promise<StoredSupportBundle | undefined> =>
  db.bundles.get(id)

/**
 * Writes a record and returns it with the id IndexedDB stored it under.
 *
 * @param id the record to overwrite, or undefined to add a new one.
 */
export const putBundleRecord = async <T extends NewSupportBundle>(
  id: number | undefined,
  entry: T
): Promise<T & { id: number }> => {
  const key =
    id === undefined ? await db.bundles.add(entry) : await db.bundles.put({ ...entry, id })
  return { ...entry, id: key }
}

/** Deletes the records with these ids. Ids that are not in the store are ignored. */
export const deleteBundleRecords = (ids: number[]): Promise<void> => db.bundles.bulkDelete(ids)

export const clearBundleRecords = (): Promise<void> => db.bundles.clear()
