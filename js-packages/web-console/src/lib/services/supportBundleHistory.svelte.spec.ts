/**
 * The IndexedDB-backed support bundle history. Runs in the browser project because it
 * drives a real IndexedDB and a real structured clone, which decide whether a File
 * System Access handle can be stored at all. The other kind of entry, a cached
 * archive, is covered by `supportBundleCache.svelte.spec.ts`.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  clearSupportBundles,
  getSupportBundle,
  isBundlePickerSupported,
  isLinkedBundle,
  type LinkedSupportBundle,
  listSupportBundles,
  maxRememberedBundles,
  pickSupportBundle,
  queryBundleReadPermission,
  readStoredBundle,
  readSupportBundle,
  rememberSupportBundle,
  requestBundleReadPermission,
  resolveStoredBundle,
  type StoredSupportBundle,
  touchSupportBundle
} from './supportBundleHistory'

/**
 * Stand-in for a `FileSystemFileHandle`, which no test can construct.
 *
 * The methods sit on the prototype: IndexedDB stores a handle with structured clone,
 * which copies own properties only, so a fake carrying own function properties is
 * rejected with a DataCloneError. A fake read back out of the database therefore
 * carries the data and none of the methods, where a real handle keeps both.
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

/** A history entry around `handle`, as the permission calls take one. */
const fakeEntry = (handle: FileSystemFileHandle): StoredSupportBundle => ({
  id: 1,
  name: handle.name,
  openedAt: 1,
  handle
})

/** Distinct, increasing timestamps, so "most recent" is never a coin toss. */
const useCountingClock = () => {
  let now = 1_700_000_000_000
  vi.spyOn(Date, 'now').mockImplementation(() => ++now)
}

describe('supportBundleHistory', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    useCountingClock()
    await clearSupportBundles()
  })

  describe('remembering bundles', () => {
    it('stores a picked bundle with its file name', async () => {
      const stored = await rememberSupportBundle(fakeHandle('pipeline-a.zip'))

      expect(stored.id).toBeTypeOf('number')
      expect(await listSupportBundles()).toEqual([
        {
          id: stored.id,
          name: 'pipeline-a.zip',
          openedAt: stored.openedAt,
          handle: expect.any(Object)
        }
      ])
    })

    it('keeps the handle usable across a database round trip', async () => {
      const { id } = await rememberSupportBundle(fakeHandle('pipeline-a.zip'))

      // Nothing about the entry survives if the handle cannot be cloned, which is
      // what rules out storing bundles in localStorage.
      const stored = await getSupportBundle(id)
      expect(isLinkedBundle(stored!)).toBe(true)
      const { handle } = stored as LinkedSupportBundle
      expect(handle.name).toBe('pipeline-a.zip')
      expect(handle.kind).toBe('file')
    })

    it('lists the most recently opened bundle first', async () => {
      await rememberSupportBundle(fakeHandle('first.zip'))
      await rememberSupportBundle(fakeHandle('second.zip'))
      await rememberSupportBundle(fakeHandle('third.zip'))

      expect((await listSupportBundles()).map((b) => b.name)).toEqual([
        'third.zip',
        'second.zip',
        'first.zip'
      ])
    })

    it('moves an already remembered file to the front instead of duplicating it', async () => {
      const first = await rememberSupportBundle(fakeHandle('first.zip'))
      await rememberSupportBundle(fakeHandle('second.zip'))

      const again = await rememberSupportBundle(fakeHandle('first.zip'))

      expect(again.id).toBe(first.id)
      expect((await listSupportBundles()).map((b) => b.name)).toEqual(['first.zip', 'second.zip'])
    })

    it('drops the oldest bundles past the history limit', async () => {
      for (let i = 0; i <= maxRememberedBundles; i++) {
        await rememberSupportBundle(fakeHandle(`bundle-${i}.zip`))
      }

      const bundles = await listSupportBundles()
      expect(bundles).toHaveLength(maxRememberedBundles)
      expect(bundles.at(0)?.name).toBe(`bundle-${maxRememberedBundles}.zip`)
      // The very first bundle is the one that falls off the end.
      expect(bundles.map((b) => b.name)).not.toContain('bundle-0.zip')
    })
  })

  describe('reading the history', () => {
    it('moves a re-opened bundle to the front', async () => {
      const first = await rememberSupportBundle(fakeHandle('first.zip'))
      await rememberSupportBundle(fakeHandle('second.zip'))

      await touchSupportBundle(first.id)

      expect((await listSupportBundles()).map((b) => b.name)).toEqual(['first.zip', 'second.zip'])
    })

    it('ignores a request to touch a bundle that is gone', async () => {
      await expect(touchSupportBundle(4321)).resolves.toBeUndefined()
      expect(await listSupportBundles()).toEqual([])
    })

    it('reports an unknown id as missing', async () => {
      expect(await getSupportBundle(4321)).toBeUndefined()
    })

    it('forgets every bundle when the history is cleared', async () => {
      await rememberSupportBundle(fakeHandle('first.zip'))
      await rememberSupportBundle(fakeHandle('second.zip'))

      await clearSupportBundles()

      expect(await listSupportBundles()).toEqual([])
    })
  })

  describe('opening a bundle the viewer was linked to', () => {
    it('hands back a bundle that can be read right away', async () => {
      const { id } = await rememberSupportBundle(fakeHandle('pipeline-a.zip'))

      const { bundle, needsPermission } = await resolveStoredBundle(id)

      // A stored fake carries no permission API, which `queryBundleReadPermission`
      // answers with 'granted'. A real handle whose grant has lapsed answers
      // 'prompt'.
      expect(bundle.name).toBe('pipeline-a.zip')
      expect(needsPermission).toBe(false)
    })

    it('refuses a bundle that is no longer in the history', async () => {
      const { id } = await rememberSupportBundle(fakeHandle('pipeline-a.zip'))
      await clearSupportBundles()

      await expect(resolveStoredBundle(id)).rejects.toThrow('no longer in the browser history')
      await expect(resolveStoredBundle(undefined)).rejects.toThrow(
        'no longer in the browser history'
      )
    })
  })

  describe('reading a bundle', () => {
    it('reads the whole archive behind a handle', async () => {
      const bytes = await readSupportBundle(fakeHandle('pipeline-a.zip', 'PK-not-really'))

      expect(new TextDecoder().decode(bytes)).toBe('PK-not-really')
    })

    it('reads a linked bundle through its handle', async () => {
      const stored = await rememberSupportBundle(fakeHandle('linked.zip', 'from-disk'))

      expect(new TextDecoder().decode(await readStoredBundle(stored))).toBe('from-disk')
    })

    it('reports an entry that says nothing about where to read', async () => {
      // Unreachable through this module; records come out of IndexedDB untyped, so a
      // database written by other means is reported, not crashed on.
      const orphan = { id: 1, name: 'orphan.zip', openedAt: 1 } as StoredSupportBundle

      await expect(readStoredBundle(orphan)).rejects.toThrow('does not say where to read')
    })
  })

  describe('read permission', () => {
    it('asks the browser whether the file can still be read', async () => {
      const queryPermission = vi.fn(async () => 'prompt' as const)
      const handle = Object.assign(fakeHandle('pipeline-a.zip'), { queryPermission })

      expect(await queryBundleReadPermission(fakeEntry(handle))).toBe('prompt')
      expect(queryPermission).toHaveBeenCalledWith({ mode: 'read' })
    })

    it('treats a browser without the permission API as granting access', async () => {
      // Reading either works or throws; reporting 'granted' lets the caller find out
      // which, instead of blocking on a prompt it cannot show.
      expect(await queryBundleReadPermission(fakeEntry(fakeHandle('pipeline-a.zip')))).toBe(
        'granted'
      )
    })

    it('reports whether the user granted access', async () => {
      const granted = Object.assign(fakeHandle('a.zip'), {
        requestPermission: async () => 'granted' as const
      })
      const denied = Object.assign(fakeHandle('b.zip'), {
        requestPermission: async () => 'denied' as const
      })

      expect(await requestBundleReadPermission(fakeEntry(granted))).toBe(true)
      expect(await requestBundleReadPermission(fakeEntry(denied))).toBe(false)
    })
  })

  describe('picking a bundle', () => {
    it('returns the handle the user picked', async () => {
      const handle = fakeHandle('pipeline-a.zip')
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [handle])
      )

      expect(isBundlePickerSupported()).toBe(true)
      expect(await pickSupportBundle()).toBe(handle)
      vi.unstubAllGlobals()
    })

    it('reports a dismissed picker as no choice rather than an error', async () => {
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => {
          throw new DOMException('The user aborted a request.', 'AbortError')
        })
      )

      expect(await pickSupportBundle()).toBe(null)
      vi.unstubAllGlobals()
    })

    it('propagates a picker failure that is not a dismissal', async () => {
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => {
          throw new DOMException('Not allowed', 'SecurityError')
        })
      )

      await expect(pickSupportBundle()).rejects.toThrow('Not allowed')
      vi.unstubAllGlobals()
    })
  })
})
