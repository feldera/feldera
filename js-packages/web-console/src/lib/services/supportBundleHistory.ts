/**
 * The history of the support bundles the user opened from disk.
 *
 * Remembering a bundle means remembering how to read the archive again, and there are
 * two ways of doing that, depending on what the browser offers:
 *
 *   Browser          Picking a file gives      An history entry costs   Reading it again needs
 *   Chromium         a FileSystemFileHandle    a few hundred Bytes      the user's permission
 *   Firefox, Safari  a File                    the whole archive        nothing
 *
 * Every browser shows the user a file picker. What differs is what the page is handed
 * back. With the File System Access API, `showOpenFilePicker` returns a
 * `FileSystemFileHandle`: an object that points at the file on disk and can open it
 * again later. Storing that is the better of the two, because it costs the same few
 * hundred bytes however large the archive behind it. Firefox and Safari have neither
 * that API nor its handles, so there the user picks the file with an
 * `<input type=file>` and the page is handed a `File`, which cannot be opened again
 * once the page is reloaded. Such a bundle is remembered by keeping a copy of the
 * archive, and everything specific to those copies is in `supportBundleCache.ts`.
 *
 * The entries live in IndexedDB because it is the only browser storage that holds
 * either kind: a handle and a `File` can both be written by structured clone, and
 * neither can be turned into the JSON that `localStorage` is limited to.
 *
 * Every tab of this site reads one and the same history, so a bundle opened in one tab
 * can be opened again from another.
 */

const DB_NAME = 'feldera-support-bundles'
const DB_VERSION = 1
const STORE_NAME = 'bundles'

/** How many bundles are remembered. Those opened longest ago are dropped first. */
export const maxRememberedBundles = 30

/** The part of a history entry that does not depend on how the archive is stored. */
type SupportBundleFacts = {
  /** The key of the entry in IndexedDB. */
  id: number
  /** The name of the file the user picked. */
  name: string
  /**
   * When the bundle was last opened, in milliseconds since the epoch. The history is
   * ordered by this.
   */
  openedAt: number
}

/** A bundle remembered as a handle, which opens the file on disk again. */
export type LinkedSupportBundle = SupportBundleFacts & { handle: FileSystemFileHandle }

/** A bundle remembered as a copy of the archive, for browsers that give out no handle. */
export type CachedSupportBundle = SupportBundleFacts & { file: File }

/**
 * A remembered bundle. An entry holds either a handle or a copy of the archive, never
 * both and never neither, so reading one starts by asking which of the two it is:
 * `isLinkedBundle` below answers that for handles, and `isCachedBundle` in
 * `supportBundleCache.ts` for copies.
 */
export type StoredSupportBundle = LinkedSupportBundle | CachedSupportBundle

/** An entry about to be written, before IndexedDB has assigned it an id. */
export type NewSupportBundle = Omit<LinkedSupportBundle, 'id'> | Omit<CachedSupportBundle, 'id'>

export const isLinkedBundle = (
  bundle: StoredSupportBundle | NewSupportBundle
): bundle is LinkedSupportBundle => 'handle' in bundle

/** Whether this site may read one particular file, in the words the browser uses. */
export type BundlePermissionState = 'granted' | 'denied' | 'prompt'

// The parts of the File System Access API that TypeScript 5.9's lib.dom.d.ts does not
// declare, narrowed to the calls made here. All three are optional: a browser may have
// the handles and not the permission methods, and outside Chromium it has neither.
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
        // IndexedDB generates the ids, because nothing about a file is stable enough
        // to use as a key: one name can occur in several directories, and a handle
        // carries no identifier of its own.
        db.createObjectStore(STORE_NAME, { keyPath: 'id', autoIncrement: true })
      }
    }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error ?? new Error('Cannot open IndexedDB'))
    request.onblocked = () => reject(new Error('IndexedDB upgrade blocked by another tab'))
  })

/**
 * Runs `use` against the store of bundles, inside a single transaction.
 *
 * `use` has to make all of its IndexedDB requests before it awaits anything else. A
 * transaction is committed as soon as the browser's event loop finds it idle, so
 * awaiting an unrelated promise in the middle of one ends it too early. Callers that
 * have to interleave other asynchronous work use several transactions instead.
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
    // A failing request rejects `use` and aborts the transaction, so both promises
    // reject. Awaiting them together reports the first failure and leaves neither
    // rejection without a handler.
    const [result] = await Promise.all([use(transaction.objectStore(STORE_NAME)), finished])
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
  // Two bundles opened in the same millisecond are ordered by their key instead,
  // which puts the one stored later first.
  return bundles.sort((a, b) => b.openedAt - a.openedAt || b.id - a.id)
}

export const getSupportBundle = async (id: number): Promise<StoredSupportBundle | undefined> =>
  withStore('readonly', (store) =>
    promisify(store.get(id) as IDBRequest<StoredSupportBundle | undefined>)
  )

/**
 * The entry for `handle`, if the history already holds one.
 *
 * `isSameEntry` is the only dependable way to ask whether two handles point at the
 * same file: picking one file twice yields two different objects, and one file name
 * can occur in several directories. Where the browser does not implement
 * `isSameEntry`, the names are compared instead.
 */
const findSameEntry = async (
  handle: FileSystemFileHandle
): Promise<StoredSupportBundle | undefined> => {
  const bundles = await listSupportBundles()
  for (const bundle of bundles) {
    if (!isLinkedBundle(bundle)) {
      continue
    }
    // A handle read back out of IndexedDB was written by whatever browser stored it,
    // so it too may be missing `isSameEntry`.
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

/** Deletes the entries with these ids. Also called from `supportBundleCache.ts`. */
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

/**
 * Deletes everything past `maxRememberedBundles`, counting from the bundle opened most
 * recently. Also called from `supportBundleCache.ts`.
 */
export const pruneToCountLimit = async (): Promise<void> => {
  const bundles = await listSupportBundles()
  await deleteSupportBundles(bundles.slice(maxRememberedBundles).map((bundle) => bundle.id))
}

/**
 * Writes an entry and makes it the most recently opened one. Deleting whatever no
 * longer fits afterwards is left to the caller.
 */
export const putSupportBundle = async (
  id: number | undefined,
  entry: NewSupportBundle
): Promise<StoredSupportBundle> => {
  // Passing an id updates that entry in place. For a new entry the property is left
  // out altogether, rather than set to undefined, so that IndexedDB assigns an id.
  const record = id === undefined ? entry : { ...entry, id }
  const key = await withStore('readwrite', (store) =>
    promisify(store.put(record) as IDBRequest<IDBValidKey>)
  )
  return { ...entry, id: key as number } as StoredSupportBundle
}

/**
 * Remembers a bundle the user chose through `pickSupportBundle`, as the most recently
 * opened one. Choosing a file that is already in the history moves its entry to the
 * front instead of adding a second entry for it.
 *
 * Afterwards only the number of entries is trimmed. A handle takes a few hundred
 * bytes, so remembering one cannot put the database over the size budget that
 * `supportBundleCache.ts` applies to stored copies.
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

/** Marks a remembered bundle as opened now, which moves it to the front of the history. */
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
 * Whether the bundle can be read without asking the user again. Browsers forget
 * permission to read a file from one visit to the next, so a handle chosen during an
 * earlier visit answers 'prompt'. A copy of the archive belongs to this site rather
 * than to the user's disk and needs no permission at all.
 */
export const queryBundleReadPermission = async (
  bundle: StoredSupportBundle
): Promise<BundlePermissionState> => {
  if (!isLinkedBundle(bundle)) {
    return 'granted'
  }
  const queryPermission = (bundle.handle as FileHandleWithPermissions).queryPermission
  // A browser without the permission methods has nothing to ask. Reading the file
  // there either works or throws, and answering 'granted' lets the caller find out
  // which of the two it is.
  return queryPermission ? await queryPermission.call(bundle.handle, { mode: 'read' }) : 'granted'
}

/**
 * Asks the user for permission to read the bundle again, and reports whether it can be
 * read afterwards.
 *
 * MUST be called while handling a click or another user gesture: browsers turn down a
 * permission request that no gesture can be attributed to.
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
 * Looks up a remembered bundle and reports whether reading it needs the user to give
 * permission first. Throws when the history holds no such entry, which is what a stale
 * link or a cleared history looks like.
 */
export const resolveStoredBundle = async (
  id: number | undefined
): Promise<{ bundle: StoredSupportBundle; needsPermission: boolean }> => {
  const bundle = id === undefined ? undefined : await getSupportBundle(id)
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

/** Reads the whole archive the handle points at. */
export const readSupportBundle = async (handle: FileSystemFileHandle): Promise<Uint8Array> =>
  readFileBytes(await handle.getFile())

/** Reads a remembered bundle, either from disk or from the copy in the database. */
export const readStoredBundle = async (bundle: StoredSupportBundle): Promise<Uint8Array> => {
  if (isLinkedBundle(bundle)) {
    return readSupportBundle(bundle.handle)
  }
  // Nothing in this module writes an entry like that, but records come back out of
  // IndexedDB with no type checking, so an entry holding neither a handle nor a copy
  // of the archive is reported to the user rather than crashed on.
  if (!(bundle.file instanceof Blob)) {
    throw new Error(`The history entry for ${bundle.name} does not say where to read the file.`)
  }
  return readFileBytes(bundle.file)
}
