/**
 * Tests for the "Open support bundle" button and the dialog it opens: the list of
 * bundles opened before, with the button that picks a new one and "Clear history" in
 * the row below them.
 *
 * These run in the browser project. The history is the real one, IndexedDB included,
 * and the assertions measure what is on screen and what is stored rather than reading
 * class names. The two functions that open a tab are mocked, because a test cannot let
 * a new window through, and so is the request for permission to read a file.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const {
  addToBundleHistory,
  goto,
  openStoredBundleTab,
  openUploadBundleTab,
  requestPermission,
  sendBundle,
  toastError
} = vi.hoisted(() => {
  const sendBundle = vi.fn(async () => {})
  return {
    addToBundleHistory: vi.fn(),
    goto: vi.fn(async () => {}),
    toastError: vi.fn(),
    openStoredBundleTab: vi.fn(),
    openUploadBundleTab: vi.fn(() => ({ send: sendBundle, cancel: vi.fn() })),
    requestPermission: vi.fn(async () => true),
    sendBundle
  }
})

// Reporting a failure and ignoring one look alike from the outside, apart from this
// call, so the tests of the error paths below assert on it.
vi.mock('svelte-french-toast', () => ({
  toast: { error: toastError, success: vi.fn(), dismiss: vi.fn() },
  Toaster: () => {}
}))

vi.mock('$app/navigation', () => ({
  goto,
  invalidateAll: vi.fn(),
  preloadCode: vi.fn(() => Promise.resolve())
}))

vi.mock('$lib/compositions/profileBundleHandoff', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$lib/compositions/profileBundleHandoff')>()),
  openStoredBundleTab,
  openUploadBundleTab
}))
// The history is real, IndexedDB and the written copies of the archives included.
// `addToBundleHistory` is a spy over the real one, so that one test can make it refuse
// a file, which is how an archive too large to copy behaves without one being
// allocated here.
vi.mock('$lib/services/supportBundleHistory', async (importOriginal) => {
  const original = await importOriginal<typeof import('$lib/services/supportBundleHistory')>()
  addToBundleHistory.mockImplementation(original.addToBundleHistory)
  return { ...original, addToBundleHistory }
})
// Only the call that would put a browser permission prompt on screen is stubbed. A
// stand-in handle read back out of the database has no permission method, so the real
// operation answers true without asking, and this spy answers with whatever a test
// asks for instead. An entry stored as a copy still offers nothing, which is what the
// component reads to decide that it need not ask at all.
vi.mock('$lib/services/supportBundleStorage', async (importOriginal) => {
  const original = await importOriginal<typeof import('$lib/services/supportBundleStorage')>()
  return {
    ...original,
    bundleOps: (record: Parameters<typeof original.bundleOps>[0]) => {
      const ops = original.bundleOps(record)
      return ops?.requestPermission ? { ...ops, requestPermission } : ops
    }
  }
})

// These imports come after the vi.mock calls above, so that the mocks are in place.
import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import { clearBundleHistory, listBundleHistory } from '$lib/services/supportBundleHistory'
import { putBundleRecord } from '$lib/services/supportBundleStore'
import OpenSupportBundleButton from './OpenSupportBundleButton.svelte'

/**
 * Stands in for a `FileSystemFileHandle`. Its methods are put on the prototype: the
 * history writes a handle with structured clone, which copies only an object's own
 * properties and turns down functions among them.
 */
const fakeHandle = (name: string) =>
  Object.create(
    { getFile: async () => new File(['bundle contents'], name) },
    { name: { value: name, enumerable: true }, kind: { value: 'file', enumerable: true } }
  ) as FileSystemFileHandle

const bundleName = (index: number) => `pipeline-alpha-support-bundle-2026-01-${index}.zip`
const BUNDLE_COUNT = 10
/** The rows' `px-2`, in px. */
const ROW_PADDING = 8

const seedHistory = async (names: string[]) => {
  await clearBundleHistory()
  for (const name of names) {
    await addToBundleHistory(fakeHandle(name))
  }
}

/**
 * Seeds bundles opened `openedMinutesAgo` ago. `addToBundleHistory` stamps the current
 * time, so an entry with a past `openedAt` is written directly.
 */
const seedHistoryOpenedAgo = async (bundles: { name: string; openedMinutesAgo: number }[]) => {
  await clearBundleHistory()
  for (const { name, openedMinutesAgo } of bundles) {
    await putBundleRecord(undefined, {
      name,
      // Half a minute past the whole minute, so a tick during the test cannot round
      // the rendered value down.
      openedAt: Date.now() - (openedMinutesAgo * 60 + 30) * 1000,
      handle: fakeHandle(name)
    })
  }
}

let mounted: { unmount: () => Promise<void> } | undefined
let modal: { unmount: () => Promise<void> } | undefined

/** Mounts the button on its own, the way the page header renders it. */
const renderButton = () => {
  const rendered = render(OpenSupportBundleButton)
  mounted = rendered as any
  const container = rendered.container
  return {
    container,
    button: container.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!
  }
}

/** Mounts the shared modal host with whatever the button opened. */
const renderOpenDialog = () => {
  const target = document.createElement('div')
  document.body.appendChild(target)
  modal = render(GlobalModal, {
    target,
    props: { dialog: useGlobalDialog().dialog }
  }) as any
  return target
}

/** The button, its dialog, and a history that has finished loading. */
const openDialog = async () => {
  const { container, button } = renderButton()
  button.click()
  const dialog = renderOpenDialog()
  // The names, not their number: the list is module-level state that outlives one
  // test, so a stale list of the same length would satisfy a count.
  const stored = (await listBundleHistory()).map((entry) => entry.name)
  await expect.poll(() => listedNames(dialog)).toEqual(stored)
  return { container, button, dialog }
}

const listedRows = (dialog: HTMLElement) => [
  ...dialog.querySelectorAll<HTMLElement>('[data-testid=btn-open-bundle-from-list]')
]
const rowName = (row: HTMLElement) =>
  row.querySelector<HTMLElement>('[data-testid=box-bundle-name]')!.textContent!.trim()
const rowOpenedAgo = (row: HTMLElement) =>
  row.querySelector<HTMLElement>('[data-testid=box-bundle-opened-ago]')!.textContent!.trim()
const listedNames = (dialog: HTMLElement) => listedRows(dialog).map(rowName)

const click = (element: Element | null | undefined) => (element as HTMLElement).click()

/** What the theme resolves `className` to, measured on a throwaway probe. */
const probeStyle = (container: HTMLElement, className: string) => {
  const probe = document.createElement('span')
  probe.className = className
  container.appendChild(probe)
  const { color, backgroundColor, borderTopColor } = getComputedStyle(probe)
  probe.remove()
  return { color, backgroundColor, borderTopColor }
}

describe('OpenSupportBundleButton.svelte', () => {
  beforeEach(async () => {
    // Skeleton's palette lives behind the theme selector. Without it the color
    // assertions below would compare two inherited blacks.
    document.documentElement.setAttribute('data-theme', 'feldera-modern-theme')
    requestPermission.mockResolvedValue(true)
    await seedHistory(Array.from({ length: BUNDLE_COUNT }, (_, i) => bundleName(i)))
    // Reset, not just clear: one test replaces the implementation with a throw.
    openStoredBundleTab.mockReset()
    toastError.mockClear()
    openUploadBundleTab.mockClear()
    addToBundleHistory.mockClear()
    requestPermission.mockClear()
    goto.mockClear()
  })

  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    await modal?.unmount()
    modal = undefined
    useGlobalDialog().dialog = null
    vi.unstubAllGlobals()
  })

  describe('the button', () => {
    it('is outlined in the primary color and says what it opens', async () => {
      const { container, button } = renderButton()

      expect(button.textContent!.trim()).toBe('Open support bundle')
      const primary = probeStyle(container, 'border-primary-500')
      expect(getComputedStyle(button).borderTopColor).toBe(primary.borderTopColor)
      expect(parseFloat(getComputedStyle(button).borderTopWidth)).toBeGreaterThan(0)
      // Outlined, not filled, because the filled preset belongs to the dialog's own
      // primary action.
      expect(getComputedStyle(button).backgroundColor).not.toBe(
        probeStyle(container, 'preset-filled-primary-500').backgroundColor
      )
    })

    it('opens the dialog and nothing else', async () => {
      const { container, button } = renderButton()

      // Nothing of the dialog exists until the button is clicked.
      expect(container.querySelector('[data-testid=box-all-bundles]')).toBe(null)
      expect(useGlobalDialog().dialog).toBe(null)

      button.click()

      expect(useGlobalDialog().dialog).not.toBe(null)
      const dialog = renderOpenDialog()
      // The dialog's title names the history it lists.
      expect(
        dialog.querySelector<HTMLElement>('[data-testid=box-dialog-title]')!.textContent
      ).toContain('Recent pipeline profiles')
      expect(dialog.textContent!.match(/Recent pipeline profiles/g)).toHaveLength(1)
      expect(dialog.querySelector('[aria-label="Close dialog"]')).toBeTruthy()
    })

    it('takes a class for the caller to place it with, and reports opening', async () => {
      // What the navigation drawer needs: the button centred in its column, and a
      // callback that closes the drawer.
      const onOpen = vi.fn()
      const rendered = render(OpenSupportBundleButton, { btnClass: 'self-center', onOpen })
      mounted = rendered as any
      const button = rendered.container.querySelector<HTMLElement>(
        '[data-testid=btn-open-support-bundle]'
      )!

      expect(getComputedStyle(button).alignSelf).toBe('center')
      expect(onOpen).not.toHaveBeenCalled()

      button.click()

      expect(onOpen).toHaveBeenCalledOnce()
      expect(useGlobalDialog().dialog).not.toBe(null)
    })
  })

  describe('the dialog', () => {
    it('puts the picker in the bottom left corner, below the bundles', async () => {
      const { dialog } = await openDialog()

      const pick = dialog.querySelector<HTMLElement>('[data-testid=btn-pick-support-bundle]')!
      const clear = dialog.querySelector<HTMLElement>('[data-testid=btn-clear-bundle-history]')!
      const list = dialog.querySelector<HTMLElement>('[data-testid=box-all-bundles]')!
      expect(pick.textContent!.trim()).toBe('Upload support bundle')
      // Below the bundles, sharing a row with "Clear history" and left of it.
      expect(pick.getBoundingClientRect().top).toBeGreaterThanOrEqual(
        list.getBoundingClientRect().bottom - 1
      )
      expect(pick.getBoundingClientRect().right).toBeLessThan(clear.getBoundingClientRect().left)
      // Flush with the row's left edge, so it lines up with the bundles above it.
      const row = clear.parentElement!.getBoundingClientRect()
      expect(Math.abs(pick.getBoundingClientRect().left - row.left)).toBeLessThanOrEqual(1)
      // Outlined in primary, like the button that opened the dialog.
      expect(getComputedStyle(pick).borderTopColor).toBe(
        probeStyle(dialog, 'border-primary-500').borderTopColor
      )
      expect(getComputedStyle(pick).backgroundColor).not.toBe(
        probeStyle(dialog, 'preset-filled-primary-500').backgroundColor
      )
    })

    it('lists every remembered bundle', async () => {
      const { dialog } = await openDialog()

      expect(listedNames(dialog)).toEqual((await listBundleHistory()).map((b) => b.name))
      expect(listedNames(dialog)).toHaveLength(BUNDLE_COUNT)
    })

    it('says how long ago each bundle was opened', async () => {
      await seedHistoryOpenedAgo([
        { name: 'yesterday.zip', openedMinutesAgo: 26 * 60 + 5 },
        { name: 'just-now.zip', openedMinutesAgo: 0 }
      ])
      const { dialog } = await openDialog()
      const rows = listedRows(dialog)

      // Days, hours and minutes, like the pipelines table's status age.
      expect(rowOpenedAgo(rows.find((row) => rowName(row) === 'yesterday.zip')!)).toBe(
        '1d 2h 5m ago'
      )
      expect(rowOpenedAgo(rows.find((row) => rowName(row) === 'just-now.zip')!)).toBe('< 1m ago')
    })

    it('puts the elapsed time at the right edge of the row', async () => {
      await seedHistoryOpenedAgo([{ name: 'one.zip', openedMinutesAgo: 5 }])
      const { dialog } = await openDialog()
      const row = listedRows(dialog)[0]
      const name = row.querySelector<HTMLElement>('[data-testid=box-bundle-name]')!
      const ago = row.querySelector<HTMLElement>('[data-testid=box-bundle-opened-ago]')!

      // After the name and hard against the row's padding, so the times line up down
      // the list whatever the names are.
      expect(name.getBoundingClientRect().right).toBeLessThanOrEqual(
        ago.getBoundingClientRect().left
      )
      expect(row.getBoundingClientRect().right - ago.getBoundingClientRect().right).toBeCloseTo(
        ROW_PADDING,
        0
      )
      // A dimmer color than the name beside it.
      expect(getComputedStyle(ago).color).toBe(probeStyle(dialog, 'text-surface-700-300').color)
      expect(getComputedStyle(ago).color).not.toBe(getComputedStyle(name).color)
    })

    it('says the bundles are cached where the browser gives out no handles', async () => {
      // Whether handles exist is known before anything is drawn, so the title is
      // right on the very first render.
      vi.stubGlobal('showOpenFilePicker', undefined)
      const { dialog } = await openDialog()

      expect(
        dialog.querySelector<HTMLElement>('[data-testid=box-dialog-title]')!.textContent
      ).toContain('Recent pipeline profiles (cached in the browser)')
    })

    it('says nothing about caching where bundles are opened from disk', async () => {
      vi.stubGlobal('showOpenFilePicker', vi.fn())
      const { dialog } = await openDialog()

      const title = dialog.querySelector<HTMLElement>('[data-testid=box-dialog-title]')!
      expect(title.textContent!.trim()).toBe('Recent pipeline profiles')
    })

    it('cuts a name off with an ellipsis and keeps the list inside the screen', async () => {
      const longName = `${'pipeline-with-an-unreasonably-long-name-'.repeat(4)}.zip`
      await seedHistory(['short.zip', longName])
      const { dialog } = await openDialog()

      const box = dialog.querySelector<HTMLElement>('[data-testid=box-all-bundles]')!
      const rows = listedRows(dialog)
      const long = rows.find((row) => rowName(row) === longName)!
      const short = rows.find((row) => rowName(row) === 'short.zip')!
      const name = long.querySelector<HTMLElement>('[data-testid=box-bundle-name]')!

      expect(getComputedStyle(name).textOverflow).toBe('ellipsis')
      expect(name.scrollWidth).toBeGreaterThan(name.clientWidth)
      expect(long.title).toBe(longName)
      // The list does not widen past the screen, so nothing scrolls sideways.
      expect(box.scrollWidth).toBe(box.clientWidth)
      expect(long.getBoundingClientRect().width).toBeLessThanOrEqual(box.clientWidth)
      expect(box.getBoundingClientRect().width).toBeLessThanOrEqual(
        box.parentElement!.getBoundingClientRect().width
      )
      expect(box.getBoundingClientRect().right).toBeLessThanOrEqual(window.innerWidth)
      // The name truncates, and the elapsed time keeps its position.
      expect(rowOpenedAgo(long)).toMatch(/ago$/)
      expect(
        long.getBoundingClientRect().right -
          long
            .querySelector<HTMLElement>('[data-testid=box-bundle-opened-ago]')!
            .getBoundingClientRect().right
      ).toBeCloseTo(ROW_PADDING, 0)
      // Every row spans the list, so its hover highlight does too.
      expect(long.offsetWidth).toBe(box.clientWidth)
      expect(short.offsetWidth).toBe(box.clientWidth)
    })

    it('clears the history', async () => {
      const { dialog } = await openDialog()

      click(dialog.querySelector('[data-testid=btn-clear-bundle-history]'))

      await expect.poll(async () => await listBundleHistory()).toEqual([])
      await expect.poll(() => listedRows(dialog)).toEqual([])
      expect(dialog.textContent).toContain('No support bundles opened recently')
    })
  })

  describe('opening a remembered bundle', () => {
    it('opens it in a new page and makes it the most recent', async () => {
      const { dialog } = await openDialog()
      const row = listedRows(dialog)[1]
      const name = rowName(row)
      const { id } = (await listBundleHistory()).find((b) => b.name === name)!

      click(row)

      // Asking the browser for permission comes first, so this entry, which is stored
      // as a reference to a file, opens a tick after the click rather than inside it.
      await expect.poll(() => openStoredBundleTab.mock.calls).toContainEqual([id])
      // Opening a bundle moves it to the front of the history, and the dialog
      // closes.
      await expect.poll(async () => (await listBundleHistory())[0].name).toBe(name)
      expect(useGlobalDialog().dialog).toBe(null)
    })

    it('asks for access, then opens the profile', async () => {
      // A browser restart leaves the history intact and the permissions gone. Asking
      // costs nothing where they are not, so the request goes out either way.
      const { dialog } = await openDialog()
      const row = listedRows(dialog)[0]
      const { id } = (await listBundleHistory()).find((b) => b.name === rowName(row))!

      click(row)

      // The prompt comes first, then the profile opens, with nothing in between.
      await expect.poll(() => requestPermission.mock.calls.length).toBe(1)
      await expect.poll(() => openStoredBundleTab.mock.calls).toContainEqual([id])
      expect(dialog.querySelector('[data-testid=box-support-bundle-confirm]')).toBe(null)
      expect(goto).not.toHaveBeenCalled()
    })

    it('opens a bundle stored as a copy inside the click that asked for it', async () => {
      // A copy of the archive is this site's own data, so there is nothing to ask and
      // nothing to await. Safari blocks a new tab that any suspension separates from
      // the click that opened it, and every bundle is a copy there.
      await clearBundleHistory()
      await addToBundleHistory(new File(['bundle contents'], 'cached.zip'))
      const { dialog } = await openDialog()

      click(listedRows(dialog)[0])

      // Asserted without awaiting: the tab has to have been opened by now.
      expect(openStoredBundleTab).toHaveBeenCalledOnce()
      expect(requestPermission).not.toHaveBeenCalled()
    })

    it('leaves the bundle unopened when access is refused', async () => {
      requestPermission.mockResolvedValue(false)
      const { dialog } = await openDialog()

      click(listedRows(dialog)[0])

      await expect.poll(() => requestPermission.mock.calls.length).toBe(1)
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(goto).not.toHaveBeenCalled()
      // The message says what to do next, not only what failed.
      await expect
        .poll(() => toastError.mock.calls.at(0)?.[0])
        .toMatch(/needs access to the file\. Click it again/)
      // The dialog stays open, so the user can try again.
      expect(useGlobalDialog().dialog).not.toBe(null)
    })

    it('reports a rejected permission request instead of failing silently', async () => {
      // `requestPermission` rejects, rather than answering false, when the click that
      // would justify the question has already been used up.
      requestPermission.mockRejectedValue(new Error('NotAllowedError'))
      const { dialog } = await openDialog()

      click(listedRows(dialog)[0])

      await expect.poll(() => toastError.mock.calls.at(0)?.[0]).toContain('NotAllowedError')
      expect(requestPermission).toHaveBeenCalledOnce()
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(useGlobalDialog().dialog).not.toBe(null)
    })

    it('leaves the bundle where it is when the browser blocks the new page', async () => {
      openStoredBundleTab.mockImplementation(() => {
        throw new Error('Browser blocked the popup.')
      })
      const { dialog } = await openDialog()
      const row = listedRows(dialog)[1]
      const name = rowName(row)
      const firstBefore = (await listBundleHistory())[0].name

      click(row)

      // Nothing opened, so the history order is untouched and the dialog stays up for
      // another try.
      await expect
        .poll(() => toastError.mock.calls.at(0)?.[0])
        .toContain('Browser blocked the popup')
      expect((await listBundleHistory())[0].name).toBe(firstBefore)
      expect(name).not.toBe(firstBefore)
      expect(useGlobalDialog().dialog).not.toBe(null)
    })
  })

  describe('picking a bundle from disk', () => {
    it('picks straight from the button and confirms in the popup', async () => {
      const { dialog } = await openDialog()
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [fakeHandle('picked.zip')])
      )

      click(dialog.querySelector('[data-testid=btn-pick-support-bundle]'))

      // No menu stands between the button and the file picker here, because in this
      // mode the popup holds nothing but the confirmation.
      await expect
        .poll(() => dialog.querySelector('[data-testid=btn-confirm-view-profile]'))
        .toBeTruthy()
      expect(dialog.querySelector('[data-testid=btn-upload-support-bundle]')).toBe(null)
      expect(dialog.querySelector('[data-testid=btn-download-support-bundle]')).toBe(null)
      expect(dialog.textContent).toContain('picked.zip')
      // The confirmation opens upwards, because the button it hangs from sits at the
      // bottom edge of the dialog and a downward popup would be off screen.
      const pick = dialog.querySelector<HTMLElement>('[data-testid=btn-pick-support-bundle]')!
      const confirm = dialog.querySelector<HTMLElement>('[data-testid=box-support-bundle-menu]')!
      expect(confirm.getBoundingClientRect().bottom).toBeLessThanOrEqual(
        pick.getBoundingClientRect().top + 1
      )
      expect(confirm.getBoundingClientRect().top).toBeGreaterThanOrEqual(0)
      // Opening the viewer needs a click of its own, or the browser blocks the new
      // tab as a popup.
      expect(openStoredBundleTab).not.toHaveBeenCalled()

      // Picking remembers the bundle, so the list shows it and the viewer points at
      // the history entry.
      await expect.poll(async () => (await listBundleHistory())[0].name).toBe('picked.zip')
      const { id } = (await listBundleHistory())[0]
      await expect.poll(() => listedNames(dialog)[0]).toBe('picked.zip')

      click(dialog.querySelector('[data-testid=btn-confirm-view-profile]'))

      // The viewer re-reads the file itself, so no bytes are handed over.
      expect(openStoredBundleTab).toHaveBeenCalledWith(id)
      expect(openUploadBundleTab).not.toHaveBeenCalled()
      expect(useGlobalDialog().dialog).toBe(null)
    })

    it('confirms the file chosen last, and nothing when the picker is dismissed', async () => {
      const { dialog } = await openDialog()
      const pick = () => click(dialog.querySelector('[data-testid=btn-pick-support-bundle]'))
      const confirmation = () =>
        dialog.querySelector<HTMLElement>('[data-testid=btn-confirm-view-profile]')
      const confirmedName = () =>
        dialog.querySelector<HTMLElement>('[data-testid=box-support-bundle-menu]')?.textContent
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [fakeHandle('picked.zip')])
      )

      pick()
      await expect.poll(confirmation).toBeTruthy()
      expect(confirmedName()).toContain('picked.zip')

      // Choosing again and then dismissing the picker: the API reports a dismissal by
      // throwing, so the second click leaves nothing to confirm.
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => {
          throw new DOMException('The user aborted a request.', 'AbortError')
        })
      )
      pick()

      // The confirmation named a file the user has moved on from, so it goes, and a
      // dismissed picker is not a failure to report.
      await expect.poll(confirmation).toBe(null)
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(toastError).not.toHaveBeenCalled()

      // The next file takes its place, rather than reviving the one before it.
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [fakeHandle('second.zip')])
      )
      pick()

      await expect.poll(confirmation).toBeTruthy()
      expect(confirmedName()).toContain('second.zip')
      expect(confirmedName()).not.toContain('picked.zip')
    })

    /**
     * Chooses a file through the `<input type=file>` the button falls back to where
     * the browser has no `showOpenFilePicker`.
     */
    const pickThroughInput = async (dialog: HTMLElement, file: File) => {
      vi.stubGlobal('showOpenFilePicker', undefined)
      click(dialog.querySelector('[data-testid=btn-pick-support-bundle]'))
      const input = dialog.querySelector<HTMLInputElement>(
        '[data-testid=input-upload-support-bundle]'
      )!
      const transfer = new DataTransfer()
      transfer.items.add(file)
      input.files = transfer.files
      input.dispatchEvent(new Event('change', { bubbles: true }))

      await expect
        .poll(() => dialog.querySelector('[data-testid=btn-confirm-view-profile]'))
        .toBeTruthy()
      click(dialog.querySelector('[data-testid=btn-confirm-view-profile]'))
    }

    it('remembers a bundle the file input handed over', async () => {
      // Firefox and Safari take this route for every bundle: no `showOpenFilePicker`
      // means no handle, so the history keeps a copy of the archive instead.
      const { dialog } = await openDialog()

      await pickThroughInput(dialog, new File(['bundle contents'], 'from-input.zip'))

      const stored = (await listBundleHistory()).find((b) => b.name === 'from-input.zip')!
      expect(stored).toBeTruthy()
      // The viewer reads the copy, so nothing is handed over and the bundle stays in
      // the history.
      expect(openStoredBundleTab).toHaveBeenCalledWith(stored.id)
      expect(openUploadBundleTab).not.toHaveBeenCalled()
      expect((await listBundleHistory())[0].name).toBe('from-input.zip')
    })

    it('hands the bytes over for an archive the history will not copy', async () => {
      // Null is what `addToBundleHistory` answers for an archive larger than
      // `maxCachedBundleBytes`, and for one the browser refused to store.
      addToBundleHistory.mockResolvedValueOnce(null)
      const { dialog } = await openDialog()

      await pickThroughInput(dialog, new File(['bundle contents'], 'huge.zip'))

      // With nothing in the history to point the viewer at, the bytes go to the new
      // tab.
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(openUploadBundleTab).toHaveBeenCalledOnce()
      await expect.poll(() => sendBundle.mock.calls.length).toBe(1)
      expect((await listBundleHistory()).map((b) => b.name)).not.toContain('huge.zip')
    })
  })
})
