/**
 * Tests for the IndexedDB object store behind the support bundle history.
 *
 * These run in the browser project rather than against a simulated DOM, because it
 * is a real IndexedDB and a real structured clone that decide whether a
 * `FileSystemFileHandle` or a `File` can be stored at all.
 */

import Dexie from 'dexie'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  clearBundleRecords,
  deleteBundleRecords,
  getBundleRecord,
  type LinkedSupportBundle,
  listBundleRecords,
  type Observable,
  observeBundleRecords,
  putBundleRecord
} from './supportBundleStore'

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

const linkedRecord = (name: string, openedAt: number) => ({
  name,
  openedAt,
  handle: fakeHandle(name)
})

/** Collects everything an observable hands over, until the test stops it. */
const watch = <T>(observable: Observable<T>) => {
  const seen: T[] = []
  const subscription = observable.subscribe((value) => seen.push(value))
  return { seen, stop: () => subscription.unsubscribe() }
}

const namesOfLast = (seen: { name: string }[][]) => seen.at(-1)?.map((record) => record.name)

/** Long enough for an unwanted re-read to have arrived, had one been coming. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 100))

describe('supportBundleStore', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    await clearBundleRecords()
  })

  describe('writing records', () => {
    it('gives a new record the id IndexedDB assigned it', async () => {
      const record = await putBundleRecord(undefined, linkedRecord('pipeline-a.zip', 1))

      expect(record.id).toBeTypeOf('number')
      expect((await getBundleRecord(record.id))?.name).toBe('pipeline-a.zip')
    })

    it('overwrites the record an id names rather than adding a second one', async () => {
      const first = await putBundleRecord(undefined, linkedRecord('pipeline-a.zip', 1))

      const again = await putBundleRecord(first.id, linkedRecord('pipeline-a.zip', 2))

      expect(again.id).toBe(first.id)
      expect(await listBundleRecords()).toHaveLength(1)
      expect((await getBundleRecord(first.id))?.openedAt).toBe(2)
    })

    it('keeps a handle usable across a database round trip', async () => {
      // A handle survives structured clone. It could not go into localStorage at
      // all, having no JSON form.
      const { id } = await putBundleRecord(undefined, linkedRecord('pipeline-a.zip', 1))

      const { handle } = (await getBundleRecord(id)) as LinkedSupportBundle
      expect(handle.name).toBe('pipeline-a.zip')
      expect(handle.kind).toBe('file')
    })

    it('keeps the contents of a stored copy of the archive', async () => {
      const file = new File(['PK-not-really'], 'copied.zip')
      const { id } = await putBundleRecord(undefined, { name: file.name, openedAt: 1, file })

      const stored = await getBundleRecord(id)
      expect(await (stored as { file: File }).file.text()).toBe('PK-not-really')
    })
  })

  describe('reading records', () => {
    it('lists the most recently opened record first', async () => {
      await putBundleRecord(undefined, linkedRecord('first.zip', 1))
      await putBundleRecord(undefined, linkedRecord('second.zip', 2))
      await putBundleRecord(undefined, linkedRecord('third.zip', 3))

      expect((await listBundleRecords()).map((record) => record.name)).toEqual([
        'third.zip',
        'second.zip',
        'first.zip'
      ])
    })

    it('breaks a tie on the timestamp with the key, latest first', async () => {
      // Without the tiebreak, two bundles opened in the same millisecond would come
      // back in whatever order IndexedDB happened to return them.
      const older = await putBundleRecord(undefined, linkedRecord('older.zip', 7))
      const newer = await putBundleRecord(undefined, linkedRecord('newer.zip', 7))

      expect((await listBundleRecords()).map((record) => record.id)).toEqual([newer.id, older.id])
    })

    it('reports an unknown id as missing', async () => {
      expect(await getBundleRecord(4321)).toBeUndefined()
    })
  })

  describe('deleting records', () => {
    it('deletes the records the ids name and leaves the rest', async () => {
      const gone = await putBundleRecord(undefined, linkedRecord('gone.zip', 1))
      const kept = await putBundleRecord(undefined, linkedRecord('kept.zip', 2))

      await deleteBundleRecords([gone.id])

      expect((await listBundleRecords()).map((record) => record.id)).toEqual([kept.id])
    })

    it('accepts an id that is not in the store', async () => {
      // Two tabs reading the history at the same time both delete the same
      // unreadable record, so the second delete must not fail.
      await expect(deleteBundleRecords([4321])).resolves.toBeUndefined()
    })

    it('accepts an empty list of ids', async () => {
      await expect(deleteBundleRecords([])).resolves.toBeUndefined()
    })

    it('empties the store', async () => {
      await putBundleRecord(undefined, linkedRecord('first.zip', 1))
      await putBundleRecord(undefined, linkedRecord('second.zip', 2))

      await clearBundleRecords()

      expect(await listBundleRecords()).toEqual([])
    })
  })

  describe('watching the store', () => {
    it('hands over the records it starts with', async () => {
      await putBundleRecord(undefined, linkedRecord('watched.zip', 1))

      const watcher = watch(observeBundleRecords(listBundleRecords))

      try {
        await vi.waitFor(() => expect(namesOfLast(watcher.seen)).toEqual(['watched.zip']))
      } finally {
        watcher.stop()
      }
    })

    it('hands them over again after a write', async () => {
      const watcher = watch(observeBundleRecords(listBundleRecords))

      try {
        await vi.waitFor(() => expect(watcher.seen).toHaveLength(1))
        await putBundleRecord(undefined, linkedRecord('added.zip', 2))

        await vi.waitFor(() => expect(namesOfLast(watcher.seen)).toEqual(['added.zip']))
      } finally {
        watcher.stop()
      }
    })

    it('hands them over after a write through another connection to the database', async () => {
      // A second connection to the same database stands in for a second tab, which
      // a test in one page cannot open. Dexie tells every connection about a write;
      // between real tabs it does so over a BroadcastChannel, which this does not
      // exercise.
      const otherTab = new Dexie('feldera-support-bundles')
      otherTab.version(1).stores({ bundles: '++id' })
      const watcher = watch(observeBundleRecords(listBundleRecords))

      try {
        await vi.waitFor(() => expect(watcher.seen).toHaveLength(1))
        await otherTab.table('bundles').add(linkedRecord('other-tab.zip', 3))

        await vi.waitFor(() => expect(namesOfLast(watcher.seen)).toEqual(['other-tab.zip']))
      } finally {
        watcher.stop()
        otherTab.close()
      }
    })

    it('stops handing them over once the caller unsubscribes', async () => {
      const watcher = watch(observeBundleRecords(listBundleRecords))
      await vi.waitFor(() => expect(watcher.seen).toHaveLength(1))
      watcher.stop()

      await putBundleRecord(undefined, linkedRecord('after.zip', 4))
      await settle()

      expect(watcher.seen).toHaveLength(1)
    })
  })
})
