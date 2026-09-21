/**
 * Tests for the two ways a support bundle is stored, and for `bundleOps`, which
 * decides which of the two a given record is.
 *
 * These run in the browser project rather than against a simulated DOM, because the
 * `File` and `Blob` the tests build have to be the ones the application sees.
 */

import { describe, expect, it, vi } from 'vitest'
import {
  bundleOps,
  isPermissionRequired,
  isSameCachedFile,
  isSameHandle
} from './supportBundleStorage'
import type { StoredSupportBundle } from './supportBundleStore'

/**
 * Stands in for a `FileSystemFileHandle`, which a test cannot construct. It has no
 * permission methods, matching a handle read back out of IndexedDB and any handle in
 * a browser outside Chromium.
 */
const fakeHandle = (name: string, contents = 'bundle contents') =>
  ({
    name,
    kind: 'file',
    getFile: async () => new File([contents], name),
    isSameEntry: async (other: { name: string }) => other.name === name
  }) as unknown as FileSystemFileHandle

const linked = (handle: FileSystemFileHandle): StoredSupportBundle => ({
  id: 1,
  name: handle.name,
  openedAt: 1,
  handle
})

const cached = (file: File): StoredSupportBundle => ({ id: 2, name: file.name, openedAt: 2, file })

/** The operations for `record`, which every test in this block expects to exist. */
const opsOf = (record: StoredSupportBundle) => {
  const ops = bundleOps(record)
  expect(ops).toBeDefined()
  return ops!
}

/** Asking for permission to read `record`, which a bundle stored as a handle offers. */
const requestPermissionFor = (record: StoredSupportBundle) => {
  const { requestPermission } = opsOf(record)
  expect(requestPermission).toBeDefined()
  return requestPermission!
}

describe('supportBundleStorage', () => {
  describe('a bundle stored as a handle', () => {
    it('reads the archive through the handle', async () => {
      const ops = opsOf(linked(fakeHandle('linked.zip', 'from-disk')))

      expect(new TextDecoder().decode(await ops.read())).toBe('from-disk')
    })

    it('occupies no bytes worth counting', () => {
      expect(opsOf(linked(fakeHandle('linked.zip'))).bytes()).toBe(0)
    })

    it('asks the browser for permission to read the file', async () => {
      const requestPermission = vi.fn(async () => 'granted' as const)
      const handle = Object.assign(fakeHandle('linked.zip'), { requestPermission })

      expect(await requestPermissionFor(linked(handle))()).toBe(true)
      expect(requestPermission).toHaveBeenCalledWith({ mode: 'read' })
    })

    it('reports a refusal as a refusal', async () => {
      const handle = Object.assign(fakeHandle('linked.zip'), {
        requestPermission: async () => 'denied' as const
      })

      expect(await requestPermissionFor(linked(handle))()).toBe(false)
    })

    it('treats a browser without the permission method as granting access', async () => {
      // There is nothing to ask, so yes is the only useful answer: reading the file
      // either works or throws, and the caller finds out which.
      expect(await requestPermissionFor(linked(fakeHandle('linked.zip')))()).toBe(true)
    })
  })

  describe('a bundle stored as a copy of the archive', () => {
    it('reads the archive out of the copy', async () => {
      const ops = opsOf(cached(new File(['PK-not-really'], 'copied.zip')))

      expect(new TextDecoder().decode(await ops.read())).toBe('PK-not-really')
    })

    it('occupies the size of the copy', () => {
      expect(opsOf(cached(new File(['12345'], 'copied.zip'))).bytes()).toBe(5)
    })

    it('offers nothing to ask permission of', async () => {
      // The copy is this site's own data rather than a file on the user's disk, so
      // there is no permission that can expire. A caller sees that without asking
      // the browser, which is what lets it skip the question.
      expect(opsOf(cached(new File(['zip'], 'copied.zip'))).requestPermission).toBeUndefined()
    })
  })

  describe('a record that is neither a reference to a file nor a cached copy', () => {
    it('reports one that says nothing about where the archive is', () => {
      const orphan = { id: 1, name: 'orphan.zip', openedAt: 1 } as StoredSupportBundle

      expect(bundleOps(orphan)).toBeUndefined()
    })

    it('reports one whose copy of the archive is not a Blob', () => {
      // Nothing writes a record like this, but IndexedDB does no type checking on
      // the way out, so one has to be recognized rather than crashed on.
      const damaged = {
        id: 1,
        name: 'damaged.zip',
        openedAt: 1,
        file: 'not a blob'
      } as unknown as StoredSupportBundle

      expect(bundleOps(damaged)).toBeUndefined()
    })
  })

  describe('recognizing a file the history already holds', () => {
    it('asks the stored handle whether it points at the same file', async () => {
      const record = linked(fakeHandle('same.zip'))

      expect(await isSameHandle(record, fakeHandle('same.zip'))).toBe(true)
      expect(await isSameHandle(record, fakeHandle('other.zip'))).toBe(false)
    })

    it('compares the names where the browser has no isSameEntry', async () => {
      const handle = fakeHandle('same.zip')
      const record = linked({ name: handle.name, kind: 'file' } as FileSystemFileHandle)

      expect(await isSameHandle(record, handle)).toBe(true)
      expect(await isSameHandle(record, fakeHandle('other.zip'))).toBe(false)
    })

    it('matches no handle against a record holding a copy', async () => {
      expect(await isSameHandle(cached(new File(['a'], 'same.zip')), fakeHandle('same.zip'))).toBe(
        false
      )
    })

    it('compares a copy by name, size and last-modified date', () => {
      // Nothing about a `File` identifies the file on disk, so those three fields
      // are what decide whether two of them are two files or one.
      const record = cached(new File(['contents'], 'same.zip', { lastModified: 1_000 }))
      const same = (name: string, contents: string, lastModified: number) =>
        isSameCachedFile(record, new File([contents], name, { lastModified }))

      expect(same('same.zip', 'contents', 1_000)).toBe(true)
      expect(same('other.zip', 'contents', 1_000)).toBe(false)
      expect(same('same.zip', 'longer contents', 1_000)).toBe(false)
      expect(same('same.zip', 'contents', 2_000)).toBe(false)
      // The contents are never read, so two files sharing a name, a size and a date
      // are treated as the same file.
      expect(same('same.zip', 'stnetnoc', 1_000)).toBe(true)
    })

    it('matches no copy against a record holding a handle', () => {
      expect(isSameCachedFile(linked(fakeHandle('same.zip')), new File(['a'], 'same.zip'))).toBe(
        false
      )
    })
  })

  describe('a read the user has not allowed', () => {
    it('recognizes the refusal `getFile` rejects with', () => {
      expect(isPermissionRequired(new DOMException('nope', 'NotAllowedError'))).toBe(true)
    })

    it('leaves a file that is no longer there to the caller', () => {
      // No permission would fix a file the user has moved or deleted, so it must not
      // be reported as one that asking would.
      expect(isPermissionRequired(new DOMException('gone', 'NotFoundError'))).toBe(false)
    })

    it('leaves an ordinary failure to the caller', () => {
      expect(isPermissionRequired(new Error('the archive is not a zip'))).toBe(false)
    })

    it('leaves an error that only borrows the name to the caller', () => {
      // Only the browser raises this one, and nothing wraps it on the way here, so
      // the check is on the class and not only on the name. Dexie's errors need the
      // looser test, `isQuotaExceeded`, for the opposite reason.
      const impostor = Object.assign(new Error('nope'), { name: 'NotAllowedError' })

      expect(isPermissionRequired(impostor)).toBe(false)
    })
  })
})
