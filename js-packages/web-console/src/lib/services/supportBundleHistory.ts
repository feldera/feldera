/**
 * History of the support bundles the user opened from disk.
 *
 * An entry remembers the archive one of two ways, and IndexedDB makes either
 * possible: both a `FileSystemFileHandle` and a `File` are structured-cloneable but
 * not JSON-serializable, so `localStorage` can hold neither.
 *
 *   Browser                     Entry holds     Cost              Re-read needs
 *   Chromium (picker)           handle          a few hundred B   a re-grant
 *   Firefox, Safari (input)     file            the whole archive nothing
 *
 * A handle is preferred wherever it exists: it costs the same few hundred bytes
 * whatever the size of the archive behind it. Browsers with no File System Access API
 * hand out only a `File`, which cannot be re-opened from its path, so the database
 * caches the archive itself; everything specific to those copies lives in
 * `supportBundleCache.ts`.
 *
 * Either way the entry is readable from any tab of this origin, which is how the
 * profile viewer opens a bundle the home page linked to.
 */

const DB_NAME = 'feldera-support-bundles'
const DB_VERSION = 1
const STORE_NAME = 'bundles'

/** Entries past this many, counting from the most recently opened, are dropped. */
export const maxRememberedBundles = 30

/** What the history knows about a bundle besides where to read it. */
type SupportBundleFacts = {
  /** IndexedDB key, and what the profile viewer URL carries. */
  id: number
  /** File name, as shown in the bundle list. */
  name: string
  /** Epoch ms the bundle was last opened; the list is sorted by this. */
  openedAt: number
}

/** A bundle that points at a file on disk. */
export type LinkedSupportBundle = SupportBundleFacts & { handle: FileSystemFileHandle }

/** A bundle the database keeps a copy of, for browsers that hand out no handle. */
export type CachedSupportBundle = SupportBundleFacts & { file: File }

/**
 * A remembered bundle. The two ways of holding a file are alternatives, not optional
 * fields, so reading an entry starts with deciding which kind it is: `isLinkedBundle`
 * here, `isCachedBundle` in the cache module.
 */
export type StoredSupportBundle = LinkedSupportBundle | CachedSupportBundle

/** An entry on its way into the database, before IndexedDB assigns its id. */
export type NewSupportBundle = Omit<LinkedSupportBundle, 'id'> | Omit<CachedSupportBundle, 'id'>

export const isLinkedBundle = (
  bundle: StoredSupportBundle | NewSupportBundle
): bundle is LinkedSupportBundle => 'handle' in bundle

/** Read/write access to a single file, as `navigator.permissions` reports it. */
export type BundlePermissionState = 'granted' | 'denied' | 'prompt'

// The parts of the File System Access API that TypeScript 5.9's lib.dom.d.ts does
// not declare, narrowed to what we call. All three are optional: a browser may ship
// the handles without the permission methods, and outside Chromium none of them
// exists.
type FileHandleWithPermissions = FileSystemFileHandle & {
  queryPermission?: (descriptor: { mode: 'read' }) => Promise<BundlePermissionState>
  requestPermission?: (descriptor: { mode: 'read' }) => Promise<BundlePermissionState>
}
type WindowWithFilePicker = Window & {
  showOpenFilePicker?: (options?: {
    multiple?: boolean
    types?: { description?: string; accept: Record<string, string[]> }[]
  }) => Promise<FileSystemFileHandle[]>
}

/**
 * Whether a bundle can be picked as a handle. False in browsers with no file picker,
 * where callers fall back to a plain file input and the history caches a copy of the
 * archive. A feature check, so the answer is settled before anything renders.
 */
export const isBundlePickerSupported = () =>
  typeof window !== 'undefined' &&
  typeof (window as WindowWithFilePicker).showOpenFilePicker === 'function' &&
  typeof indexedDB !== 'undefined'

/**
 * Shows the file picker. Resolves to null when the user dismisses it, which the
 * API reports as an `AbortError` rather than an empty selection.
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

const promisify = <T>(request: IDBRequest<T>) =>
  new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('IndexedDB request failed'))
  })

const openDatabase = () =>
  new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    request.onupgradeneeded = () => {
      const db = request.result
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        // Keys are generated: the id is an opaque token for URLs, and nothing about
        // the file can serve as a stable primary key.
        db.createObjectStore(STORE_NAME, { keyPath: 'id', autoIncrement: true })
      }
    }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('Cannot open IndexedDB'))
    request.onblocked = () => reject(new Error('IndexedDB upgrade blocked by another tab'))
  })

/**
 * Runs `use` against the bundle store inside one transaction.
 *
 * `use` must issue all of its IndexedDB requests before awaiting anything else: a
 * transaction commits as soon as the browser's event loop finds it idle, so awaiting
 * an unrelated promise in the middle would close the transaction. Callers that need
 * to interleave other async work split it into several transactions.
 */
const withStore = async <T>(
  mode: IDBTransactionMode,
  use: (store: IDBObjectStore) => Promise<T>
): Promise<T> => {
  const db = await openDatabase()
  try {
    const transaction = db.transaction(STORE_NAME, mode)
    const finished = new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve()
      transaction.onerror = () => reject(transaction.error ?? new Error('IndexedDB write failed'))
      transaction.onabort = () => reject(transaction.error ?? new Error('IndexedDB write aborted'))
    })
    const result = await use(transaction.objectStore(STORE_NAME))
    await finished
    return result
  } finally {
    db.close()
  }
}

/** All remembered bundles, most recently opened first. */
export const listSupportBundles = async (): Promise<StoredSupportBundle[]> => {
  const bundles = await withStore('readonly', (store) =>
    promisify(store.getAll() as IDBRequest<StoredSupportBundle[]>)
  )
  // Two bundles opened within the same millisecond fall back to the key, so the
  // order is the order they were stored in.
  return bundles.sort((a, b) => b.openedAt - a.openedAt || b.id - a.id)
}

export const getSupportBundle = async (id: number): Promise<StoredSupportBundle | undefined> =>
  withStore('readonly', (store) =>
    promisify(store.get(id) as IDBRequest<StoredSupportBundle | undefined>)
  )

/**
 * Same file as `handle`, if it is already in the history.
 *
 * `isSameEntry` is the only reliable identity test: two handles for one file are
 * separate objects, and a name matches across directories. A handle from a browser
 * that does not implement `isSameEntry` falls back to a name match.
 */
const findSameEntry = async (
  handle: FileSystemFileHandle
): Promise<StoredSupportBundle | undefined> => {
  const bundles = await listSupportBundles()
  for (const bundle of bundles) {
    if (!isLinkedBundle(bundle)) {
      continue
    }
    // A handle read back from IndexedDB comes from whatever browser stored it, so
    // `isSameEntry` is treated as optional.
    const isSameEntry = (bundle.handle as Partial<FileSystemFileHandle>).isSameEntry
    const same = isSameEntry
      ? await isSameEntry.call(bundle.handle, handle)
      : bundle.handle.name === handle.name
    if (same) {
      return bundle
    }
  }
  return undefined
}

/** Removes entries by id. Shared with `supportBundleCache`. */
export const deleteSupportBundles = async (ids: number[]): Promise<void> => {
  if (!ids.length) {
    return
  }
  await withStore('readwrite', async (store) => {
    for (const id of ids) {
      store.delete(id)
    }
  })
}

/** Drops the entries past `maxRememberedBundles`. Shared with `supportBundleCache`. */
export const pruneToCountLimit = async (): Promise<void> => {
  const bundles = await listSupportBundles()
  await deleteSupportBundles(bundles.slice(maxRememberedBundles).map((bundle) => bundle.id))
}

/**
 * Writes an entry as the most recent one. Callers prune afterwards;
 * `supportBundleCache` adds its own pruning pass on top.
 */
export const putSupportBundle = async (
  id: number | undefined,
  entry: NewSupportBundle
): Promise<StoredSupportBundle> => {
  // A known id updates that entry in place. Omitting the property, rather than
  // setting it to undefined, lets IndexedDB's key generator assign a new id.
  const record = id === undefined ? entry : { ...entry, id }
  const key = await withStore('readwrite', (store) =>
    promisify(store.put(record) as IDBRequest<IDBValidKey>)
  )
  return { ...entry, id: key as number } as StoredSupportBundle
}

/**
 * Records a picked bundle as the most recent one. Re-picking a file already in the
 * history moves that entry to the front instead of duplicating it.
 *
 * Only the count is pruned afterwards: a handle takes almost no space, so remembering
 * one cannot put the cache over its byte budget.
 */
export const rememberSupportBundle = async (
  handle: FileSystemFileHandle
): Promise<StoredSupportBundle> => {
  const existing = await findSameEntry(handle)
  const stored = await putSupportBundle(existing?.id, {
    name: handle.name,
    openedAt: Date.now(),
    handle
  })
  await pruneToCountLimit()
  return stored
}

/** Moves an already remembered bundle to the front of the history. */
export const touchSupportBundle = async (id: number): Promise<void> => {
  const bundle = await getSupportBundle(id)
  if (!bundle) {
    return
  }
  await withStore('readwrite', (store) =>
    promisify(store.put({ ...bundle, openedAt: Date.now() }) as IDBRequest<IDBValidKey>)
  )
}

export const clearSupportBundles = (): Promise<void> =>
  withStore('readwrite', async (store) => {
    store.clear()
  })

/**
 * Whether the bundle can be read without asking the user again. Browsers drop file
 * grants between sessions, so a handle picked in an earlier visit reports 'prompt'. A
 * cached copy belongs to this origin and needs no permission.
 */
export const queryBundleReadPermission = async (
  bundle: StoredSupportBundle
): Promise<BundlePermissionState> => {
  if (!isLinkedBundle(bundle)) {
    return 'granted'
  }
  const queryPermission = (bundle.handle as FileHandleWithPermissions).queryPermission
  // Without the permission API there is nothing to ask: reading either works or
  // throws, and reporting 'granted' lets the caller find out which.
  return queryPermission ? await queryPermission.call(bundle.handle, { mode: 'read' }) : 'granted'
}

/**
 * Asks the user to re-grant read access to a bundle, and reports whether the
 * bundle can be read afterwards.
 *
 * MUST be called from a user-gesture handler: browsers reject a permission request
 * that no click can be attributed to.
 */
export const requestBundleReadPermission = async (
  bundle: StoredSupportBundle
): Promise<boolean> => {
  if (!isLinkedBundle(bundle)) {
    return true
  }
  const requestPermission = (bundle.handle as FileHandleWithPermissions).requestPermission
  if (!requestPermission) {
    return true
  }
  return (await requestPermission.call(bundle.handle, { mode: 'read' })) === 'granted'
}

/**
 * Looks up a remembered bundle and reports whether reading it needs the user to grant
 * access. Throws when the history holds no such entry: a stale link, or a cleared
 * history.
 */
export const resolveStoredBundle = async (
  id: number | undefined
): Promise<{ bundle: StoredSupportBundle; needsPermission: boolean }> => {
  const bundle = id ? await getSupportBundle(id) : undefined
  if (!bundle) {
    throw new Error(
      'This support bundle is no longer in the browser history. Open it from disk again.'
    )
  }
  return {
    bundle,
    needsPermission: (await queryBundleReadPermission(bundle)) !== 'granted'
  }
}

const readFileBytes = async (file: File) => new Uint8Array(await file.arrayBuffer())

/** Reads the whole archive behind a handle. */
export const readSupportBundle = async (handle: FileSystemFileHandle): Promise<Uint8Array> =>
  readFileBytes(await handle.getFile())

/** Reads a remembered bundle, from disk or from the copy the cache keeps. */
export const readStoredBundle = async (bundle: StoredSupportBundle): Promise<Uint8Array> => {
  if (isLinkedBundle(bundle)) {
    return readSupportBundle(bundle.handle)
  }
  // Unreachable through this module, but records come out of IndexedDB untyped: an
  // entry holding neither a handle nor a copy is reported, not crashed on.
  if (!(bundle.file instanceof Blob)) {
    throw new Error(`The history entry for ${bundle.name} does not say where to read the file.`)
  }
  return readFileBytes(bundle.file)
}
