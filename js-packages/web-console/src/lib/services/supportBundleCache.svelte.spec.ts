/**
 * The cached half of the support bundle history: whole archives kept in
 * IndexedDB for browsers that hand out no file handles. Runs in the browser
 * project because a real IndexedDB and a real structured clone are what decide
 * whether a `File` can be stored.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { listSupportBundles } = vi.hoisted(() => ({
  // Typed like the real listing, so the tests reading it keep their types.
  listSupportBundles: vi.fn<() => Promise<StoredSupportBundle[]>>()
}))

// The history module runs for real: the database, the writes and the count limit
// are all genuine. Only the listing is a spy, so one test can report archive sizes
// too large to write here.
vi.mock('./supportBundleHistory', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./supportBundleHistory')>()),
  listSupportBundles
}))

// Imported AFTER vi.mock so the mock takes effect.
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
  queryBundleReadPermission,
  readStoredBundle,
  requestBundleReadPermission,
  resolveStoredBundle,
  type StoredSupportBundle
} from './supportBundleHistory'

/** The real listing, to put back after a test has faked it. */
const actualHistory =
  await vi.importActual<typeof import('./supportBundleHistory')>('./supportBundleHistory')

/**
 * An entry claiming `size` bytes of cached archive. The budget only reads
 * `file.size`, so no bytes have to exist.
 */
const entryOfSize = (id: number, size: number): StoredSupportBundle =>
  ({ id, name: `bundle-${id}.zip`, openedAt: id, file: { size } }) as StoredSupportBundle

/** Distinct, increasing timestamps, so "most recent" is never a coin toss. */
const useCountingClock = () => {
  let now = 1_700_000_000_000
  vi.spyOn(Date, 'now').mockImplementation(() => ++now)
}

describe('supportBundleCache', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    listSupportBundles.mockImplementation(actualHistory.listSupportBundles)
    useCountingClock()
    await clearSupportBundles()
  })

  describe('when the cache is needed', () => {
    it('follows the absence of the file picker', () => {
      // The answer is a feature check, so the interface can rely on it from the
      // first render.
      vi.stubGlobal('showOpenFilePicker', vi.fn())
      expect(isBundleCacheRequired()).toBe(false)

      vi.stubGlobal('showOpenFilePicker', undefined)
      expect(isBundleCacheRequired()).toBe(true)

      vi.unstubAllGlobals()
    })
  })

  describe('caching a file the input handed over', () => {
    it('keeps a copy of a file that comes with no handle', async () => {
      // Why the cache exists for Firefox and Safari: a `File` cannot be reopened
      // from its path, so the archive itself has to survive in the database.
      const stored = await rememberSupportBundleFile(new File(['PK-not-really'], 'from-input.zip'))

      expect(stored?.name).toBe('from-input.zip')
      const read = await getSupportBundle(stored!.id)
      expect(isCachedBundle(read!)).toBe(true)
      expect(new TextDecoder().decode(await readStoredBundle(read!))).toBe('PK-not-really')
    })

    it('moves an already cached file to the front instead of duplicating it', async () => {
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

    it('tells apart two files that share a name', async () => {
      // Same name, different archive: without a handle there is no identity test,
      // so size and modification time have to carry it.
      await rememberSupportBundleFile(new File(['one'], 'bundle.zip', { lastModified: 1_000 }))
      await rememberSupportBundleFile(new File(['two'], 'bundle.zip', { lastModified: 2_000 }))

      expect(await listSupportBundles()).toHaveLength(2)
    })

    it('refuses an archive bigger than the per-bundle limit', async () => {
      const huge = new File(['small enough really'], 'huge.zip')
      // Faked rather than allocated: the limit is hundreds of megabytes.
      Object.defineProperty(huge, 'size', { value: maxCachedBundleBytes + 1 })

      expect(await rememberSupportBundleFile(huge)).toBe(null)
      // Nothing cached means no history entry, and the caller falls back to
      // handing the bytes to the viewer tab.
      expect(await listSupportBundles()).toEqual([])
    })

    it('drops the oldest entries once the count limit is passed', async () => {
      for (let i = 0; i <= maxRememberedBundles; i++) {
        await rememberSupportBundleFile(new File(['contents'], `bundle-${i}.zip`))
      }

      const bundles = await listSupportBundles()
      expect(bundles).toHaveLength(maxRememberedBundles)
      expect(bundles.at(0)?.name).toBe(`bundle-${maxRememberedBundles}.zip`)
      expect(bundles.map((b) => b.name)).not.toContain('bundle-0.zip')
    })
  })

  describe('the byte budget', () => {
    it('keeps every copy that fits', () => {
      expect(
        cachedBundlesOverBudget([entryOfSize(2, cachedBundlesByteBudget), entryOfSize(1, 0)])
      ).toEqual([])
    })

    it('drops the oldest copies once they outgrow the budget', () => {
      const half = cachedBundlesByteBudget / 2
      const newest = entryOfSize(3, half)
      const middle = entryOfSize(2, half)
      const oldest = entryOfSize(1, half)

      // Two halves fit exactly; the third tips the total over, and it is the
      // oldest copy that goes.
      expect(cachedBundlesOverBudget([newest, middle, oldest])).toEqual([oldest])
    })

    it('does not weigh entries that hold a handle', () => {
      const linked = { id: 9, name: 'linked.zip', openedAt: 9, handle: {} } as StoredSupportBundle

      expect(cachedBundlesOverBudget([entryOfSize(2, cachedBundlesByteBudget), linked])).toEqual([])
    })

    it('is applied when a copy is cached', async () => {
      const older = await rememberSupportBundleFile(new File(['older'], 'older.zip'))
      // The entries are real; only the sizes the accounting reads are faked, since
      // half a gigabyte cannot be written here. The phantom newest entry fills the
      // budget on its own, which leaves nothing for the one behind it.
      listSupportBundles.mockResolvedValue([
        entryOfSize(9999, cachedBundlesByteBudget),
        entryOfSize(older!.id, cachedBundlesByteBudget)
      ])

      await rememberSupportBundleFile(new File(['newest'], 'newest.zip'))

      expect(await getSupportBundle(older!.id)).toBeUndefined()
    })

    it('leaves the count limit to the history', () => {
      // Past the count limit the entries are already gone, so the budget must not
      // count bytes it cannot free.
      const bundles = Array.from({ length: maxRememberedBundles + 2 }, (_, i) =>
        entryOfSize(maxRememberedBundles + 2 - i, cachedBundlesByteBudget)
      )

      expect(cachedBundlesOverBudget(bundles)).toEqual(bundles.slice(1, maxRememberedBundles))
    })
  })

  describe('read permission', () => {
    it('asks nobody about a cached copy', async () => {
      // The copy belongs to this origin: there is no file grant to lapse, which is
      // what gives browsers with no picker a working history.
      const stored = await rememberSupportBundleFile(new File(['zip'], 'copied.zip'))

      expect(await queryBundleReadPermission(stored!)).toBe('granted')
      expect(await requestBundleReadPermission(stored!)).toBe(true)
      expect((await resolveStoredBundle(stored!.id)).needsPermission).toBe(false)
    })
  })
})
