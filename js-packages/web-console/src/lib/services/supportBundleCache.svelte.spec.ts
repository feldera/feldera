/**
 * Tests for the part of the support bundle history implementation that keeps a copy
 * of the whole archive in IndexedDB, for browsers that don't support FileSystemFileHandle.
 *
 * These run in the browser project, not against a simulated DOM, because a real
 * IndexedDB and a real structured clone decide whether a `File` can be stored at all.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { listSupportBundles, putSupportBundle } = vi.hoisted(() => ({
  // Typed like the real functions, so the tests reading them keep their types.
  listSupportBundles: vi.fn<() => Promise<StoredSupportBundle[]>>(),
  putSupportBundle:
    vi.fn<(id: number | undefined, entry: NewSupportBundle) => Promise<StoredSupportBundle>>()
}))

// The history module itself is not replaced: these tests use the real database, the
// real writes and the limit on the number of entries. Two of its functions are
// wrapped in a proxy, so that one test can report archive sizes far larger
// than anything that can be written here, and another can make a write fail.
vi.mock('./supportBundleHistory', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./supportBundleHistory')>()),
  listSupportBundles,
  putSupportBundle
}))

// These imports come after the vi.mock call above, so that the mock is in place.
import {
  cachedBundlesByteBudget,
  cachedBundlesOverBudget,
  isBundleCacheRequired,
  isCachedBundle,
  maxCachedBundleBytes,
  rememberSupportBundleFile
} from './supportBundleCache'
import {
  clearSupportBundles,
  getSupportBundle,
  maxRememberedBundles,
  type NewSupportBundle,
  queryBundleReadPermission,
  readStoredBundle,
  requestBundleReadPermission,
  resolveStoredBundle,
  type StoredSupportBundle
} from './supportBundleHistory'

/** The unmocked history module, to restore the real behaviour after a test faked it. */
const actualHistory =
  await vi.importActual<typeof import('./supportBundleHistory')>('./supportBundleHistory')

/**
 * A history entry that claims to hold an archive of `size` bytes. Only `file.size` is
 * read when the budget is worked out, so those bytes do not have to exist.
 */
const entryOfSize = (id: number, size: number): StoredSupportBundle =>
  ({ id, name: `bundle-${id}.zip`, openedAt: id, file: { size } }) as StoredSupportBundle

/**
 * Makes `Date.now` return a larger value on every call, so that bundles stored one
 * after another never share a timestamp and their order is never ambiguous.
 */
const useCountingClock = () => {
  let now = 1_700_000_000_000
  vi.spyOn(Date, 'now').mockImplementation(() => ++now)
}

describe('supportBundleCache', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    // A test that fails part way through can leave a stubbed global behind, and that
    // would break every test after it.
    vi.unstubAllGlobals()
    listSupportBundles.mockImplementation(actualHistory.listSupportBundles)
    putSupportBundle.mockImplementation(actualHistory.putSupportBundle)
    useCountingClock()
    await clearSupportBundles()
  })

  describe('when a copy of the archive is needed', () => {
    it('needs one only where the browser hands back no file handle', () => {
      // Where `showOpenFilePicker` exists, picking a file gives back a handle to the
      // file on disk, and the history stores that instead of the archive.
      vi.stubGlobal('showOpenFilePicker', vi.fn())
      expect(isBundleCacheRequired()).toBe(false)

      vi.stubGlobal('showOpenFilePicker', undefined)
      expect(isBundleCacheRequired()).toBe(true)
    })

    it('needs none where IndexedDB is unusable', () => {
      // There would be nowhere to put the copy. Answering true here would send the
      // caller on to `indexedDB.open`, which throws when IndexedDB is missing.
      vi.stubGlobal('showOpenFilePicker', undefined)
      vi.stubGlobal('indexedDB', undefined)

      expect(isBundleCacheRequired()).toBe(false)
    })
  })

  describe('remembering a file picked with a file input', () => {
    it('stores the contents of the archive', async () => {
      // Such a file cannot be opened a second time by any other means, so the archive
      // itself goes into the database and is read back out of it.
      const stored = await rememberSupportBundleFile(new File(['PK-not-really'], 'from-input.zip'))

      expect(stored?.name).toBe('from-input.zip')
      const read = await getSupportBundle(stored!.id)
      expect(isCachedBundle(read!)).toBe(true)
      expect(new TextDecoder().decode(await readStoredBundle(read!))).toBe('PK-not-really')
    })

    it('moves a file it already stored to the front instead of duplicating it', async () => {
      const first = await rememberSupportBundleFile(
        new File(['contents'], 'again.zip', { lastModified: 1_000 })
      )
      await rememberSupportBundleFile(new File(['other'], 'other.zip'))

      const again = await rememberSupportBundleFile(
        new File(['contents'], 'again.zip', { lastModified: 1_000 })
      )

      expect(again?.id).toBe(first?.id)
      expect((await listSupportBundles()).map((b) => b.name)).toEqual(['again.zip', 'other.zip'])
    })

    it('tells two different files of the same name apart', async () => {
      // Nothing here identifies the file on disk, so the size and the last-modified
      // date are what decide whether these are two files or one.
      await rememberSupportBundleFile(new File(['one'], 'bundle.zip', { lastModified: 1_000 }))
      await rememberSupportBundleFile(new File(['two'], 'bundle.zip', { lastModified: 2_000 }))

      expect(await listSupportBundles()).toHaveLength(2)
    })

    it('stores no archive larger than the per-bundle limit', async () => {
      const huge = new File(['small enough really'], 'huge.zip')
      // The size is faked rather than allocated, because the limit is 256 MB.
      Object.defineProperty(huge, 'size', { value: maxCachedBundleBytes + 1 })

      expect(await rememberSupportBundleFile(huge)).toBe(null)
      // Storing no copy means adding no history entry either. Returning null rather
      // than throwing leaves the caller free to open the bundle it was given.
      expect(await listSupportBundles()).toEqual([])
    })

    it('stores no archive the browser refused to write', async () => {
      // How much a site may store depends on the free space on the machine, so no
      // check beforehand can tell that a write will be refused. A refused write ends
      // the same way as an archive that is too large: null, and no history entry.
      putSupportBundle.mockRejectedValue(new DOMException('no room', 'QuotaExceededError'))

      expect(await rememberSupportBundleFile(new File(['zip'], 'refused.zip'))).toBe(null)
      expect(await listSupportBundles()).toEqual([])
    })

    it('drops the bundles opened longest ago once there are too many', async () => {
      for (let i = 0; i <= maxRememberedBundles; i++) {
        await rememberSupportBundleFile(new File(['contents'], `bundle-${i}.zip`))
      }

      const bundles = await listSupportBundles()
      expect(bundles).toHaveLength(maxRememberedBundles)
      expect(bundles.at(0)?.name).toBe(`bundle-${maxRememberedBundles}.zip`)
      expect(bundles.map((b) => b.name)).not.toContain('bundle-0.zip')
    })
  })

  describe('the budget on the total size of the stored copies', () => {
    it('reports nothing while the copies fit', () => {
      expect(
        cachedBundlesOverBudget([entryOfSize(2, cachedBundlesByteBudget), entryOfSize(1, 0)])
      ).toEqual([])
    })

    it('reports the copies that do not fit, in the order it was given them', () => {
      const half = cachedBundlesByteBudget / 2
      const newest = entryOfSize(3, half)
      const middle = entryOfSize(2, half)
      const oldest = entryOfSize(1, half)

      // The two most recently opened bundles fill the budget exactly. Counting the
      // third one goes over it, so the third, opened longest ago, is the one to drop.
      expect(cachedBundlesOverBudget([newest, middle, oldest])).toEqual([oldest])
    })

    it('counts no entry that holds a handle', () => {
      const linked = { id: 9, name: 'linked.zip', openedAt: 9, handle: {} } as StoredSupportBundle

      expect(cachedBundlesOverBudget([entryOfSize(2, cachedBundlesByteBudget), linked])).toEqual([])
    })

    it('deletes what no longer fits when a new copy is stored', async () => {
      const older = await rememberSupportBundleFile(new File(['older'], 'older.zip'))
      // The entry for older.zip is really in the database, but half a gigabyte cannot
      // be written here, so the listing is replaced by two entries that merely claim
      // that size. The first of them fills the whole budget, which leaves no room for
      // the second, older.zip.
      listSupportBundles.mockResolvedValue([
        entryOfSize(9999, cachedBundlesByteBudget),
        entryOfSize(older!.id, cachedBundlesByteBudget)
      ])

      await rememberSupportBundleFile(new File(['newest'], 'newest.zip'))

      expect(await getSupportBundle(older!.id)).toBeUndefined()
    })

    it('counts no entry that the limit on the number of entries has dropped', () => {
      // The history keeps maxRememberedBundles entries and deletes the rest, so the
      // budget must not report bytes that nothing can free.
      const bundles = Array.from({ length: maxRememberedBundles + 2 }, (_, i) =>
        entryOfSize(maxRememberedBundles + 2 - i, cachedBundlesByteBudget)
      )

      expect(cachedBundlesOverBudget(bundles)).toEqual(bundles.slice(1, maxRememberedBundles))
    })
  })

  describe('read permission', () => {
    it('needs none for a stored copy', async () => {
      // The copy belongs to this site rather than to the user's disk, so there is no
      // permission that can expire. That is what makes the history worth keeping in a
      // browser that cannot get hold of a file handle.
      const stored = await rememberSupportBundleFile(new File(['zip'], 'copied.zip'))

      expect(await queryBundleReadPermission(stored!)).toBe('granted')
      expect(await requestBundleReadPermission(stored!)).toBe(true)
      expect((await resolveStoredBundle(stored!.id)).needsPermission).toBe(false)
    })
  })
})
