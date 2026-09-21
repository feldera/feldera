/**
 * The two ways a support bundle is stored, and the operations each of them supports.
 *
 * Every browser shows the user a file picker. What the page gets back differs, and
 * so does what the history is able to store:
 *
 *   Browser          Picking a file gives    A record holds           Reading it again needs
 *   Chromium         a FileSystemFileHandle  a handle                 the user's permission
 *   Firefox, Safari  a File                  a copy of the archive    nothing
 *
 * `showOpenFilePicker`, from the File System Access API, returns a
 * `FileSystemFileHandle`: a reference to the file on disk that can open it again
 * later. Storing one takes a few hundred bytes however large the archive is. Firefox
 * and Safari have neither that API nor its handles. There the user picks the file
 * with an `<input type=file>`, which yields a `File` that stops working once the page
 * reloads, so the only way to open the same bundle again is to keep a copy of the
 * archive.
 *
 * `bundleOps` is the one place that decides which of the two a record is. A third way
 * of storing a bundle would need one more `defineStorage` call in `STORAGES`, and no
 * other change.
 */

import type {
  CachedSupportBundle,
  LinkedSupportBundle,
  StoredSupportBundle
} from './supportBundleStore'

/**
 * Operations on a support bundle stored in the history. Available whether the bundle
 * is stored as a reference to the file, or cached.
 */
export type StoredBundleOps = {
  /** How many bytes of this site's storage quota the bundle occupies. */
  bytes: () => number
  /** Reads the whole archive. */
  read: () => Promise<Uint8Array>
  /**
   * Asks the user for permission to read the archive, and reports whether it can be
   * read afterwards. Absent where a bundle of this kind can never need permission,
   * which is what lets a caller skip the question without asking the browser first.
   *
   * MUST be called while handling a click or another user gesture. Browsers refuse a
   * permission request they cannot attribute to one. Asking for permission the user
   * has already given costs nothing: the browser returns the state it holds without
   * showing a prompt.
   */
  requestPermission?: () => Promise<boolean>
}

/**
 * One way of storing a bundle. `match` returns the operations for `record` when that
 * record is stored this way, and undefined when it is not.
 *
 * The record type a given way handles is deliberately absent from this type, so that
 * `STORAGES` can hold both without widening either. `defineStorage` is the only way
 * to build one, and it ties a type guard to the operations on the exact type that
 * guard proves, so an operation that reads a field the guard did not check will not
 * compile.
 */
type BundleStorage = { match: (record: StoredSupportBundle) => StoredBundleOps | undefined }

const defineStorage = <B extends StoredSupportBundle>(
  owns: (record: StoredSupportBundle) => record is B,
  ops: (record: B) => StoredBundleOps
): BundleStorage => ({ match: (record) => (owns(record) ? ops(record) : undefined) })

// The part of the File System Access API that TypeScript 5.9's lib.dom.d.ts leaves
// undeclared, narrowed to the call made here. It is optional: a browser may have the
// handles without the permission method, and outside Chromium it has neither.
type FileHandleWithPermissions = FileSystemFileHandle & {
  requestPermission?: (descriptor: { mode: 'read' }) => Promise<PermissionState>
}

/**
 * Whether `error` is a read that the user has not given permission for.
 *
 * This is the only way to find out where there is no user gesture to carry a
 * permission prompt, as on a page opened at a bundle's URL. `getFile` rejects with
 * `NotAllowedError` when the permission state is not granted, and with
 * `NotFoundError` when the file has been moved or deleted since it was remembered,
 * which no permission would fix.
 */
export const isPermissionRequired = (error: unknown): boolean =>
  error instanceof DOMException && error.name === 'NotAllowedError'

const readFileBytes = async (file: File) => new Uint8Array(await file.arrayBuffer())

// Both guards check the value, not just the presence of the key. IndexedDB does no
// type checking on the way out, and a record whose `file` is not a Blob is as
// unreadable as one with no `file` at all.
const isLinkedBundle = (record: StoredSupportBundle): record is LinkedSupportBundle =>
  'handle' in record && record.handle instanceof Object

const linkedStorage = defineStorage(isLinkedBundle, ({ handle }) => ({
  // A handle is a few hundred bytes whatever the size of the archive behind it, too
  // little for the byte budget to account for.
  bytes: () => 0,
  read: async () => readFileBytes(await handle.getFile()),
  requestPermission: async () => {
    // Outside Chromium there is no permission method to call, and nothing to ask.
    // Saying yes sends the caller on to `read`, which is where such a browser
    // reports the failure anyway.
    const requestPermission = (handle as FileHandleWithPermissions).requestPermission
    if (!requestPermission) {
      return true
    }
    return (await requestPermission.call(handle, { mode: 'read' })) === 'granted'
  }
}))

const isCachedBundle = (record: StoredSupportBundle): record is CachedSupportBundle =>
  'file' in record && record.file instanceof Blob

// No `requestPermission`: the copy is this site's own data rather than a file on the
// user's disk, so there is no permission that can expire and nothing to ask for.
const cachedStorage = defineStorage(isCachedBundle, ({ file }) => ({
  bytes: () => file.size,
  read: () => readFileBytes(file)
}))

const STORAGES: BundleStorage[] = [linkedStorage, cachedStorage]

/**
 * The operations for `record`, or undefined when it is neither a reference to a file
 * nor a cached copy.
 *
 * IndexedDB does no type checking on the way out, so a record that is neither does
 * reach here, and it is for the caller to decide what to do with it.
 */
export const bundleOps = (record: StoredSupportBundle): StoredBundleOps | undefined => {
  for (const storage of STORAGES) {
    const ops = storage.match(record)
    if (ops) {
      return ops
    }
  }
  return undefined
}

/**
 * Whether `record` refers to the same file on disk as `handle`.
 *
 * `isSameEntry` is the only dependable way to compare two handles: picking one file
 * twice yields two different objects, and one file name can occur in several
 * directories. Browsers that do not implement `isSameEntry` fall back to the names.
 */
export const isSameHandle = async (
  record: StoredSupportBundle,
  handle: FileSystemFileHandle
): Promise<boolean> => {
  if (!isLinkedBundle(record)) {
    return false
  }
  // The stored handle came from whatever browser wrote it, so it may be missing
  // `isSameEntry` as well.
  const isSameEntry = (record.handle as Partial<FileSystemFileHandle>).isSameEntry
  return isSameEntry
    ? await isSameEntry.call(record.handle, handle)
    : record.handle.name === handle.name
}

/**
 * Whether `record` caches a copy of `file`.
 *
 * Two `File` objects cannot be asked whether they came from the same file on disk, so
 * the name, the size and the last-modified date are compared instead. Two different
 * files that agree on all three are treated as one, so picking the second replaces
 * the first one's entry. That costs the user an entry rather than data: the file on
 * disk is untouched, and picking it again puts it back.
 */
export const isSameCachedFile = (record: StoredSupportBundle, file: File): boolean =>
  isCachedBundle(record) &&
  record.file.name === file.name &&
  record.file.size === file.size &&
  record.file.lastModified === file.lastModified
