/**
 * Tests for the support bundle history: adding, listing and looking up an entry, for
 * both ways of storing a bundle.
 *
 * These run in the browser project rather than against a simulated DOM, because it
 * is a real IndexedDB and a real structured clone that decide whether a
 * `FileSystemFileHandle` or a `File` can be stored at all.
 */

import Dexie from 'dexie'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { deleteBundleRecords, listBundleRecords, putBundleRecord } = vi.hoisted(() => ({
  // Typed like the real functions, so the tests reading them keep their types.
  deleteBundleRecords: vi.fn<(ids: number[]) => Promise<void>>(),
  listBundleRecords: vi.fn<() => Promise<StoredSupportBundle[]>>(),
  putBundleRecord: vi.fn<typeof actualStore.putBundleRecord>()
}))

// The store module is not replaced: these tests run against the real database and
// the real writes. Three of its functions are wrapped so that one test can report
// archive sizes far larger than anything writable here, and others can make a write
// or a delete fail.
vi.mock('./supportBundleStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./supportBundleStore')>()),
  deleteBundleRecords,
  listBundleRecords,
  putBundleRecord
}))

// These imports follow the vi.mock call above, so that the mock is in place.
import {
  addToBundleHistory,
  bundlesOverByteBudget,
  cachedBundlesByteBudget,
  clearBundleHistory,
  isBundleCacheRequired,
  isBundlePickerSupported,
  isHistorySupported,
  listBundleHistory,
  markBundleOpenedNow,
  maxCachedBundleBytes,
  maxHistoryEntries,
  observeBundleHistory,
  pickSupportBundle,
  resolveStoredBundle
} from './supportBundleHistory'
import type { Observable, StoredSupportBundle } from './supportBundleStore'

/** The unwrapped store module, to restore real behaviour after a test faked it. */
const actualStore =
  await vi.importActual<typeof import('./supportBundleStore')>('./supportBundleStore')

/**
 * Stands in for a `FileSystemFileHandle`, which a test cannot construct.
 *
 * The two methods sit on the prototype rather than on the object itself. IndexedDB
 * writes a handle through structured clone, which copies an object's own properties
 * only and rejects functions among them with a `DataCloneError`. A stand-in read back
 * out of the database therefore has the name and the kind and none of the methods,
 * while a real handle keeps its methods.
 */
const fakeHandle = (name: string, contents = 'bundle contents') =>
  Object.create(
    {
      getFile: async () => new File([contents], name),
      isSameEntry: async (other: { name: string }) => other.name === name
    },
    {
      name: { value: name, enumerable: true },
      kind: { value: 'file', enumerable: true }
    }
  ) as FileSystemFileHandle

/**
 * A record whose cached copy claims to be `size` bytes. Working out the budget reads
 * `file.size` and nothing else, so those bytes do not have to exist.
 */
const recordOfSize = (id: number, size: number): StoredSupportBundle => {
  const file = new File([], `bundle-${id}.zip`)
  Object.defineProperty(file, 'size', { value: size })
  return { id, name: file.name, openedAt: id, file }
}

/**
 * Makes `Date.now` return a larger value on every call, so that bundles stored one
 * after another never share a timestamp and their order is never ambiguous.
 */
const useCountingClock = () => {
  let now = 1_700_000_000_000
  vi.spyOn(Date, 'now').mockImplementation(() => ++now)
}

const historyNames = async () => (await listBundleHistory()).map((entry) => entry.name)

/** Collects everything an observable hands over, until the test stops it. */
const watch = <T>(observable: Observable<T>) => {
  const seen: T[] = []
  const subscription = observable.subscribe((value) => seen.push(value))
  return { seen, stop: () => subscription.unsubscribe() }
}

/** Long enough for an unwanted re-read to have arrived, had one been coming. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 100))

describe('supportBundleHistory', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    // A test that fails part way through can leave a stubbed global behind, which
    // would then break every test after it.
    vi.unstubAllGlobals()
    deleteBundleRecords.mockImplementation(actualStore.deleteBundleRecords)
    listBundleRecords.mockImplementation(actualStore.listBundleRecords)
    putBundleRecord.mockImplementation(actualStore.putBundleRecord)
    useCountingClock()
    await clearBundleHistory()
  })

  describe('what the browser allows', () => {
    it('follows IndexedDB for whether a history can be kept', () => {
      expect(isHistorySupported()).toBe(true)

      vi.stubGlobal('indexedDB', undefined)
      expect(isHistorySupported()).toBe(false)
    })

    it('needs a copy of the archive only where showOpenFilePicker is missing', () => {
      // Where `showOpenFilePicker` exists, picking a file gives back a reference to
      // the file on disk, and the history stores that instead of the archive.
      vi.stubGlobal('showOpenFilePicker', vi.fn())
      expect(isBundlePickerSupported()).toBe(true)
      expect(isBundleCacheRequired()).toBe(false)

      vi.stubGlobal('showOpenFilePicker', undefined)
      expect(isBundlePickerSupported()).toBe(false)
      expect(isBundleCacheRequired()).toBe(true)
    })

    it('needs no copy where IndexedDB is unusable', () => {
      // There would be nowhere to put the copy. Answering true would send the caller
      // on to `indexedDB.open`, which throws when IndexedDB is missing.
      vi.stubGlobal('showOpenFilePicker', undefined)
      vi.stubGlobal('indexedDB', undefined)

      expect(isBundleCacheRequired()).toBe(false)
    })
  })

  describe('picking a bundle', () => {
    it('returns the handle the user picked', async () => {
      const handle = fakeHandle('pipeline-a.zip')
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [handle])
      )

      expect(await pickSupportBundle()).toBe(handle)
    })

    it('reports a dismissed picker as no choice rather than an error', async () => {
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => {
          throw new DOMException('The user aborted a request.', 'AbortError')
        })
      )

      expect(await pickSupportBundle()).toBe(null)
    })

    it('propagates a picker failure that is not a dismissal', async () => {
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => {
          throw new DOMException('Not allowed', 'SecurityError')
        })
      )

      await expect(pickSupportBundle()).rejects.toThrow('Not allowed')
    })
  })

  describe('adding a bundle picked as a handle', () => {
    it('adds an entry that reads the file on disk', async () => {
      const entry = await addToBundleHistory(fakeHandle('pipeline-a.zip', 'from-disk'))

      expect(entry?.name).toBe('pipeline-a.zip')
      expect(await historyNames()).toEqual(['pipeline-a.zip'])
      expect(new TextDecoder().decode(await entry!.ops.read())).toBe('from-disk')
    })

    it('lists the most recently opened bundle first', async () => {
      await addToBundleHistory(fakeHandle('first.zip'))
      await addToBundleHistory(fakeHandle('second.zip'))
      await addToBundleHistory(fakeHandle('third.zip'))

      expect(await historyNames()).toEqual(['third.zip', 'second.zip', 'first.zip'])
    })

    it('moves a file it already holds to the front instead of duplicating it', async () => {
      const first = await addToBundleHistory(fakeHandle('first.zip'))
      await addToBundleHistory(fakeHandle('second.zip'))

      const again = await addToBundleHistory(fakeHandle('first.zip'))

      expect(again?.id).toBe(first?.id)
      expect(await historyNames()).toEqual(['first.zip', 'second.zip'])
    })

    it('drops the bundles opened longest ago past the limit on the count', async () => {
      for (let i = 0; i <= maxHistoryEntries; i++) {
        await addToBundleHistory(fakeHandle(`bundle-${i}.zip`))
      }

      const names = await historyNames()
      expect(names).toHaveLength(maxHistoryEntries)
      expect(names.at(0)).toBe(`bundle-${maxHistoryEntries}.zip`)
      expect(names).not.toContain('bundle-0.zip')
    })
  })

  describe('adding a bundle picked as a File', () => {
    it('adds an entry that reads a copy of the archive', async () => {
      // A `File` cannot be opened a second time by any other means, so the archive
      // itself goes into the database and is read back out of it.
      const entry = await addToBundleHistory(new File(['PK-not-really'], 'from-input.zip'))

      expect(entry?.name).toBe('from-input.zip')
      const [listed] = await listBundleHistory()
      expect(new TextDecoder().decode(await listed.ops.read())).toBe('PK-not-really')
    })

    it('moves a file it already holds to the front instead of duplicating it', async () => {
      const first = await addToBundleHistory(
        new File(['contents'], 'again.zip', { lastModified: 1_000 })
      )
      await addToBundleHistory(new File(['other'], 'other.zip'))

      const again = await addToBundleHistory(
        new File(['contents'], 'again.zip', { lastModified: 1_000 })
      )

      expect(again?.id).toBe(first?.id)
      expect(await historyNames()).toEqual(['again.zip', 'other.zip'])
    })

    it('tells two different files of the same name apart', async () => {
      await addToBundleHistory(new File(['one'], 'bundle.zip', { lastModified: 1_000 }))
      await addToBundleHistory(new File(['two'], 'bundle.zip', { lastModified: 2_000 }))

      expect(await historyNames()).toHaveLength(2)
    })

    it('adds no entry for an archive over the per-bundle limit', async () => {
      const huge = new File(['small enough really'], 'huge.zip')
      // The size is faked rather than allocated, because the limit is 256 MB.
      Object.defineProperty(huge, 'size', { value: maxCachedBundleBytes + 1 })

      // Returning null rather than throwing leaves the caller free to open the bundle
      // it is holding.
      expect(await addToBundleHistory(huge)).toBe(null)
      expect(await listBundleHistory()).toEqual([])
    })

    it('adds no entry for an archive the browser refused to write', async () => {
      // The size of the quota depends on the free space on the machine, so no check
      // beforehand can tell that a write will be refused. A refused write ends the
      // same way as an archive that is too large: null, and no entry.
      //
      // Dexie raises an error class of its own here rather than passing on the
      // `DOMException` IndexedDB threw, so a test throwing a `DOMException` would
      // let an `instanceof DOMException` check pass while the real path failed.
      putBundleRecord.mockRejectedValue(new Dexie.QuotaExceededError())

      expect(await addToBundleHistory(new File(['zip'], 'refused.zip'))).toBe(null)
      expect(await listBundleHistory()).toEqual([])
    })

    it('drops the bundles opened longest ago past the limit on the count', async () => {
      for (let i = 0; i <= maxHistoryEntries; i++) {
        await addToBundleHistory(new File(['contents'], `bundle-${i}.zip`))
      }

      const names = await historyNames()
      expect(names).toHaveLength(maxHistoryEntries)
      expect(names.at(0)).toBe(`bundle-${maxHistoryEntries}.zip`)
      expect(names).not.toContain('bundle-0.zip')
    })
  })

  describe('the budget on the total number of bytes', () => {
    it('reports nothing while the entries fit', () => {
      expect(
        bundlesOverByteBudget([recordOfSize(2, cachedBundlesByteBudget), recordOfSize(1, 0)])
      ).toEqual([])
    })

    it('reports the entries that do not fit, in the order it was given them', () => {
      const half = cachedBundlesByteBudget / 2
      const newest = recordOfSize(3, half)
      const middle = recordOfSize(2, half)
      const oldest = recordOfSize(1, half)

      // The two most recently opened bundles fill the budget exactly, so counting the
      // third goes over it. The third, opened longest ago, is the one to drop.
      expect(bundlesOverByteBudget([newest, middle, oldest])).toEqual([oldest])
    })

    it('reports no entry that occupies nothing', () => {
      // Deleting a reference to a file frees no bytes, so it is left alone even when
      // the copies before it have already gone over the budget.
      const linked: StoredSupportBundle = {
        id: 9,
        name: 'linked.zip',
        openedAt: 9,
        handle: fakeHandle('linked.zip')
      }
      const fillsTheBudget = recordOfSize(3, cachedBundlesByteBudget)
      const oneByteOver = recordOfSize(2, 1)

      expect(bundlesOverByteBudget([fillsTheBudget, oneByteOver, linked])).toEqual([oneByteOver])
    })

    it('reports no entry that the limit on the count has dropped', () => {
      // The history keeps maxHistoryEntries entries and deletes the rest, so the
      // budget must not count bytes that nothing can free.
      const records = Array.from({ length: maxHistoryEntries + 2 }, (_, i) =>
        recordOfSize(maxHistoryEntries + 2 - i, cachedBundlesByteBudget)
      )

      expect(bundlesOverByteBudget(records)).toEqual(records.slice(1, maxHistoryEntries))
    })

    it('deletes what no longer fits when a new copy is stored', async () => {
      const older = await addToBundleHistory(new File(['older'], 'older.zip'))
      // The entry for older.zip really is in the database, but half a gigabyte cannot
      // be written here, so the listing is replaced by two records that only claim
      // that size. The first fills the whole budget, leaving no room for the second,
      // older.zip.
      listBundleRecords.mockResolvedValue([
        recordOfSize(9999, cachedBundlesByteBudget),
        recordOfSize(older!.id, cachedBundlesByteBudget)
      ])

      await addToBundleHistory(new File(['newest'], 'newest.zip'))

      listBundleRecords.mockImplementation(actualStore.listBundleRecords)
      expect(await historyNames()).toEqual(['newest.zip'])
    })
  })

  describe('marking a bundle as opened now', () => {
    it('moves it to the front of the history', async () => {
      const first = await addToBundleHistory(fakeHandle('first.zip'))
      await addToBundleHistory(fakeHandle('second.zip'))

      await markBundleOpenedNow(first!.id)

      expect(await historyNames()).toEqual(['first.zip', 'second.zip'])
    })

    it('ignores an id that is no longer in the history', async () => {
      await expect(markBundleOpenedNow(4321)).resolves.toBeUndefined()
      expect(await listBundleHistory()).toEqual([])
    })
  })

  describe('clearing the history', () => {
    it('forgets every entry', async () => {
      await addToBundleHistory(fakeHandle('first.zip'))
      await addToBundleHistory(fakeHandle('second.zip'))

      await clearBundleHistory()

      expect(await listBundleHistory()).toEqual([])
    })
  })

  describe('a record that is neither a reference to a file nor a cached copy', () => {
    /** Writes a record that says nothing about where its archive is. */
    const addUnreadableRecord = async () =>
      (
        await actualStore.putBundleRecord(undefined, {
          name: 'orphan.zip',
          openedAt: 1
        } as unknown as Parameters<typeof actualStore.putBundleRecord>[1])
      ).id

    it('leaves it out of the history and returns the rest', async () => {
      await addToBundleHistory(fakeHandle('readable.zip'))
      await addUnreadableRecord()

      expect(await historyNames()).toEqual(['readable.zip'])
    })

    it('deletes it, so the next read no longer finds it', async () => {
      const id = await addUnreadableRecord()
      vi.spyOn(console, 'warn').mockImplementation(() => {})

      await listBundleHistory()

      // The delete runs after the read has returned, so the test waits for it.
      await vi.waitFor(async () => expect(await actualStore.getBundleRecord(id)).toBeUndefined())
    })

    it('still returns the readable entries when the delete fails', async () => {
      await addToBundleHistory(fakeHandle('readable.zip'))
      await addUnreadableRecord()
      deleteBundleRecords.mockRejectedValue(new Error('IndexedDB is unavailable'))
      vi.spyOn(console, 'warn').mockImplementation(() => {})

      expect(await historyNames()).toEqual(['readable.zip'])
      await vi.waitFor(() => expect(deleteBundleRecords).toHaveBeenCalled())
    })

    it('refuses to open it by id', async () => {
      const id = await addUnreadableRecord()

      await expect(resolveStoredBundle(id)).rejects.toThrow('no longer in the browser history')
    })

    it('settles after deleting it, rather than reading the history forever', async () => {
      // Deleting the record is a change to the database, so a watcher reads the
      // history once more. That read finds nothing left to delete, which is what
      // stops the two from feeding each other.
      await addUnreadableRecord()
      vi.spyOn(console, 'warn').mockImplementation(() => {})
      const watcher = watch(observeBundleHistory())

      try {
        await vi.waitFor(() => expect(watcher.seen.length).toBeGreaterThan(1))
        const reads = watcher.seen.length
        await settle()

        expect(watcher.seen).toHaveLength(reads)
      } finally {
        watcher.stop()
      }
    })
  })

  describe('watching the history', () => {
    it('hands over the history again after a bundle is added', async () => {
      const watcher = watch(observeBundleHistory())

      try {
        await vi.waitFor(() => expect(watcher.seen).toHaveLength(1))
        await addToBundleHistory(fakeHandle('watched.zip'))

        await vi.waitFor(() =>
          expect(watcher.seen.at(-1)?.map((entry) => entry.name)).toEqual(['watched.zip'])
        )
      } finally {
        watcher.stop()
      }
    })
  })

  describe('looking up an entry by its id', () => {
    it('hands back the entry itself, not a wrapper around it', async () => {
      // A cached copy rather than a handle: a stand-in handle read back out of
      // IndexedDB has lost `getFile`, so only a copy can be read here.
      const added = await addToBundleHistory(new File(['archive'], 'pipeline-a.zip'))

      const entry = await resolveStoredBundle(added!.id)

      expect(entry.name).toBe('pipeline-a.zip')
      expect(new TextDecoder().decode(await entry.ops.read())).toBe('archive')
    })

    it('refuses an entry that is no longer in the history', async () => {
      const added = await addToBundleHistory(fakeHandle('pipeline-a.zip'))
      await clearBundleHistory()

      await expect(resolveStoredBundle(added!.id)).rejects.toThrow(
        'no longer in the browser history'
      )
      await expect(resolveStoredBundle(undefined)).rejects.toThrow(
        'no longer in the browser history'
      )
    })
  })
})
