/**
 * The home page's "Pipeline profiles" section: one row of remembered support
 * bundles, the opener that picks a new one, and the dialog listing them all.
 *
 * The history here is the real IndexedDB-backed one - only the tab-opening
 * helpers are mocked, since a test cannot let a new window through. What each
 * assertion measures is geometry or the real stored state, not class names.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const {
  goto,
  openStoredBundleTab,
  openUploadBundleTab,
  queryBundleReadPermission,
  rememberSupportBundleFile,
  requestBundleReadPermission,
  sendBundle
} = vi.hoisted(() => {
  const sendBundle = vi.fn(async () => {})
  return {
    goto: vi.fn(async () => {}),
    openStoredBundleTab: vi.fn(),
    openUploadBundleTab: vi.fn(() => ({ send: sendBundle, cancel: vi.fn() })),
    queryBundleReadPermission: vi.fn(async () => 'granted' as string),
    rememberSupportBundleFile: vi.fn(),
    requestBundleReadPermission: vi.fn(async () => true),
    sendBundle
  }
})

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
// Everything about the history is real, IndexedDB included; only the two calls
// that would put a browser permission prompt on screen are stood in for. A
// stored fake handle carries no permission API, so by default the real ones
// answer "granted" — which is the case these override.
vi.mock('$lib/services/supportBundleHistory', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$lib/services/supportBundleHistory')>()),
  queryBundleReadPermission,
  requestBundleReadPermission
}))
// The cache runs for real as well; `rememberSupportBundleFile` is a spy only so a
// single test can make it decline a file, which is what an archive too big to
// cache looks like from here.
vi.mock('$lib/services/supportBundleCache', async (importOriginal) => {
  const original = await importOriginal<typeof import('$lib/services/supportBundleCache')>()
  rememberSupportBundleFile.mockImplementation(original.rememberSupportBundleFile)
  return { ...original, rememberSupportBundleFile }
})

// Imported AFTER vi.mock so the mocks take effect.
import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import { loadSupportBundleHistory } from '$lib/compositions/useSupportBundleHistory.svelte'
import {
  clearSupportBundles,
  listSupportBundles,
  rememberSupportBundle
} from '$lib/services/supportBundleHistory'
import PipelineProfiles from './PipelineProfiles.svelte'

/**
 * Stand-in for a `FileSystemFileHandle`. The methods live on the prototype
 * because IndexedDB stores the handle with structured clone, which copies own
 * properties only and rejects own function properties.
 */
const fakeHandle = (name: string) =>
  Object.create(
    { getFile: async () => new File(['bundle contents'], name) },
    { name: { value: name, enumerable: true }, kind: { value: 'file', enumerable: true } }
  ) as FileSystemFileHandle

// Long enough that every tile hits its width cap, so how many fit is decided by
// the row's width alone.
const bundleName = (index: number) => `pipeline-alpha-support-bundle-2026-01-${index}.zip`
const BUNDLE_COUNT = 10
/** The row's `gap-3`, in px, as the component counts it. */
const TILE_GAP = 12

/** Re-reads the history so the rows pick up a changed permission answer. */
const setReadPermission = async (state: 'granted' | 'prompt') => {
  queryBundleReadPermission.mockResolvedValue(state)
  await loadSupportBundleHistory()
}

const seedHistory = async (names: string[]) => {
  await clearSupportBundles()
  for (const name of names) {
    await rememberSupportBundle(fakeHandle(name))
  }
  await loadSupportBundleHistory()
}

let mounted: { unmount: () => Promise<void> } | undefined
let modal: { unmount: () => Promise<void> } | undefined

/** Mounts the section in a container of known width and waits for the row to settle. */
const renderProfiles = async (width: number, { expectTiles = true } = {}) => {
  const rendered = render(PipelineProfiles)
  mounted = rendered as any
  const container = rendered.container
  container.style.width = `${width}px`

  const section = container.querySelector<HTMLElement>('[data-testid=box-pipeline-profiles]')!
  await expect.poll(() => section.clientWidth).toBeGreaterThan(0)
  if (expectTiles) {
    // The history is read from IndexedDB and the row is measured before any tile
    // can be placed, so both happen after the mount.
    await expect.poll(() => tiles(container).length).toBeGreaterThan(0)
  }
  await new Promise((settled) => requestAnimationFrame(() => requestAnimationFrame(settled)))
  return { container, section }
}

/** Mounts the shared modal host with whatever the section opened. */
const renderOpenDialog = () => {
  const target = document.createElement('div')
  document.body.appendChild(target)
  modal = render(GlobalModal, {
    target,
    props: { dialog: useGlobalDialog().dialog }
  }) as any
  return target
}

const tiles = (container: HTMLElement) => [
  ...container.querySelectorAll<HTMLElement>('[data-testid=btn-open-recent-bundle]')
]

/** The link (or hint) that closes the row, right after the tiles. */
const trailingItem = (container: HTMLElement) =>
  container.querySelector<HTMLElement>('[data-testid=box-profiles-trailing]')!

const click = (element: Element | null | undefined) => (element as HTMLElement).click()

/** What the theme resolves `className` to, measured on a throwaway probe. */
const probeStyle = (container: HTMLElement, className: string) => {
  const probe = document.createElement('span')
  probe.className = className
  container.appendChild(probe)
  const { color, borderTopColor } = getComputedStyle(probe)
  probe.remove()
  return { color, borderTopColor }
}

describe('PipelineProfiles.svelte', () => {
  beforeEach(async () => {
    // Skeleton's palette lives behind the theme selector; without it every
    // "purple" assertion below would compare two inherited blacks.
    document.documentElement.setAttribute('data-theme', 'feldera-modern-theme')
    queryBundleReadPermission.mockResolvedValue('granted')
    requestBundleReadPermission.mockResolvedValue(true)
    await seedHistory(Array.from({ length: BUNDLE_COUNT }, (_, i) => bundleName(i)))
    openStoredBundleTab.mockClear()
    openUploadBundleTab.mockClear()
    requestBundleReadPermission.mockClear()
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

  describe('the row', () => {
    it('leads with the opener, highlighted in the primary color', async () => {
      const { container } = await renderProfiles(1200)

      const opener = container.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!
      const row = container.querySelector<HTMLElement>('[data-testid=box-profiles-row]')!
      // First in the row, ahead of any remembered bundle.
      expect(row.firstElementChild!.contains(opener)).toBe(true)
      const primary = probeStyle(container, 'text-primary-500 border-primary-500')
      expect(getComputedStyle(opener).color).toBe(primary.color)
      expect(getComputedStyle(opener).borderTopColor).toBe(primary.borderTopColor)
      // ...and it does not look like the remembered bundles next to it.
      expect(getComputedStyle(opener).color).not.toBe(getComputedStyle(tiles(container)[0]).color)
    })

    it('leads the title with the film icon, muted and title-sized', async () => {
      const { container } = await renderProfiles(1200)

      const titleRow = container.querySelector<HTMLElement>('[data-testid=box-profiles-title]')!
      const icon = titleRow.firstElementChild as HTMLElement
      const title = titleRow.children[1] as HTMLElement
      // Which glyph it is can only be read off the class: this browser has no icon
      // font loaded, so every `fd` span measures the same.
      expect(icon.className).toContain('fd-film')
      // Sized with the title and muted like the other sections' icons.
      expect(getComputedStyle(icon).fontSize).toBe(getComputedStyle(title).fontSize)
      expect(getComputedStyle(icon).color).toBe(probeStyle(container, 'text-surface-500').color)
      expect(title.textContent!.trim()).toBe('Pipeline profiles')
    })

    it('keeps a bundle name on one line, ellipsized at a fixed width', async () => {
      const { container } = await renderProfiles(1600)

      const tile = tiles(container)[0]
      const style = getComputedStyle(tile)
      expect(style.whiteSpace).toBe('nowrap')
      expect(style.textOverflow).toBe('ellipsis')
      expect(style.overflowX).toBe('hidden')
      // The cap holds and the name is actually cut short rather than laid out wide.
      expect(tile.offsetWidth).toBeLessThanOrEqual(parseFloat(style.maxWidth))
      expect(tile.scrollWidth).toBeGreaterThan(tile.clientWidth)
    })

    it('shows the bundles that fit and no more', async () => {
      const { container } = await renderProfiles(1200)
      const narrow = tiles(container)

      expect(narrow.length).toBeGreaterThan(0)
      expect(narrow.length).toBeLessThan(BUNDLE_COUNT)
      // Each tile keeps the width its name asks for — here the cap, since every
      // seeded name is long — rather than being squeezed to share the row.
      const cap = parseFloat(getComputedStyle(narrow[0]).maxWidth)
      for (const tile of narrow) {
        expect(tile.offsetWidth).toBe(cap)
      }
      // The row is as full as it can be: one more tile would run into the link
      // that follows the tiles.
      const trailing = trailingItem(container).getBoundingClientRect()
      const last = narrow.at(-1)!.getBoundingClientRect()
      expect(last.right).toBeLessThanOrEqual(trailing.left + 1)
      expect(last.right + TILE_GAP + cap).toBeGreaterThan(trailing.left)

      // Widening the row fits more of them.
      container.style.width = '1900px'
      await expect.poll(() => tiles(container).length).toBeGreaterThan(narrow.length)
      expect(tiles(container).at(-1)!.getBoundingClientRect().right).toBeLessThanOrEqual(
        trailingItem(container).getBoundingClientRect().left + 1
      )
    })

    it('leaves room for the gaps between the tiles', async () => {
      const { container } = await renderProfiles(1600)
      const cap = parseFloat(getComputedStyle(tiles(container)[0]).maxWidth)
      const row = container.querySelector<HTMLElement>('[data-testid=box-profiles-row]')!
      const opener = container.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!

      // Leave the tiles room for four of them edge to edge, but not for the gaps
      // between them: a fourth tile appears here only if the gaps are forgotten.
      const forTiles = Math.round(4 * cap + 1.5 * TILE_GAP)
      const neededRow =
        forTiles + opener.offsetWidth + trailingItem(container).offsetWidth + 2 * TILE_GAP
      container.style.width = `${container.clientWidth + (neededRow - row.clientWidth)}px`
      await expect.poll(() => tiles(container).length).toBeLessThan(4)

      const shown = tiles(container).length
      expect(shown).toBeGreaterThan(0)
      expect(shown * cap + (shown - 1) * TILE_GAP).toBeLessThanOrEqual(forTiles)
    })

    it('offers the two purple links to the full list', async () => {
      const { container } = await renderProfiles(1200)

      const purple = probeStyle(container, 'text-primary-500').color
      const viewAll = container.querySelector<HTMLElement>('[data-testid=btn-view-all-bundles]')!
      const viewRecent = container.querySelector<HTMLElement>(
        '[data-testid=btn-view-recent-bundles]'
      )!
      expect(getComputedStyle(viewAll).color).toBe(purple)
      expect(getComputedStyle(viewRecent).color).toBe(purple)
      // The trailing link follows the last tile at the row's own gap, rather than
      // being pushed out to the far edge.
      const last = tiles(container).at(-1)!.getBoundingClientRect()
      expect(viewRecent.getBoundingClientRect().left - last.right).toBeCloseTo(TILE_GAP, 0)
    })

    it('says where bundles will appear while the history is empty', async () => {
      await seedHistory([])
      const { container, section } = await renderProfiles(1200, { expectTiles: false })

      expect(tiles(section)).toEqual([])
      expect(section.querySelector('[data-testid=btn-view-all-bundles]')).toBe(null)
      expect(section.textContent).toContain('Bundles you open appear here')
      // The opener is the one thing that always belongs in the row.
      const opener = container.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!
      expect(opener).toBeTruthy()
      // The hint sits next to the opener, not across the row from it.
      const hint = trailingItem(container).getBoundingClientRect()
      expect(hint.left - opener.getBoundingClientRect().right).toBeLessThanOrEqual(3 * TILE_GAP)
    })
  })

  describe('opening a bundle', () => {
    it('opens a remembered bundle in a new page and makes it the most recent', async () => {
      const { container } = await renderProfiles(1600)
      const second = tiles(container)[1]
      const name = second.textContent!.trim()
      const { id } = (await listSupportBundles()).find((b) => b.name === name)!

      click(second)

      expect(openStoredBundleTab).toHaveBeenCalledWith(id)
      // Opening it moved it to the front of the history, on disk and on screen.
      await expect.poll(async () => (await listSupportBundles())[0].name).toBe(name)
      await expect.poll(() => tiles(container)[0].textContent!.trim()).toBe(name)
    })

    it('asks for access and goes straight to the profile', async () => {
      // What a browser restart leaves behind: the history is intact, the grants
      // are not.
      await setReadPermission('prompt')
      const { container } = await renderProfiles(1600)
      const tile = tiles(container)[0]
      const name = tile.textContent!.trim()
      const { id } = (await listSupportBundles()).find((b) => b.name === name)!

      click(tile)

      // The prompt comes first, then the profile opens — nothing stands between
      // the click and the profile.
      await expect.poll(() => requestBundleReadPermission.mock.calls.length).toBe(1)
      await expect.poll(() => openStoredBundleTab.mock.calls).toContainEqual([id])
      expect(container.querySelector('[data-testid=box-support-bundle-confirm]')).toBe(null)
      expect(goto).not.toHaveBeenCalled()
    })

    it('leaves the bundle unopened when access is refused', async () => {
      await setReadPermission('prompt')
      requestBundleReadPermission.mockResolvedValue(false)
      const { container } = await renderProfiles(1600)

      click(tiles(container)[0])

      await expect.poll(() => requestBundleReadPermission.mock.calls.length).toBe(1)
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(goto).not.toHaveBeenCalled()
    })

    it('asks for access from the full list as well', async () => {
      await setReadPermission('prompt')
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))
      const dialog = renderOpenDialog()
      const row = dialog.querySelector<HTMLElement>('[data-testid=btn-open-bundle-from-list]')!
      const name = row.textContent!.trim()
      const { id } = (await listSupportBundles()).find((b) => b.name === name)!

      click(row)

      await expect.poll(() => requestBundleReadPermission.mock.calls.length).toBe(1)
      await expect.poll(() => openStoredBundleTab.mock.calls).toContainEqual([id])
      // The dialog gets out of the way of the tab it opened.
      expect(useGlobalDialog().dialog).toBe(null)
    })

    it('lists every bundle in the dialog, including the ones the row omits', async () => {
      const { container } = await renderProfiles(1200)
      const shown = tiles(container).map((tile) => tile.textContent!.trim())

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))
      const dialog = renderOpenDialog()

      const listed = [
        ...dialog.querySelectorAll<HTMLElement>('[data-testid=btn-open-bundle-from-list]')
      ].map((row) => row.textContent!.trim())
      expect(listed).toHaveLength(BUNDLE_COUNT)
      expect(listed).toEqual(expect.arrayContaining(shown))
    })

    it('says the bundles are cached where the browser gives out no handles', async () => {
      // Known from a feature check, so the title is right on the first render
      // rather than after the first bundle is opened.
      vi.stubGlobal('showOpenFilePicker', undefined)
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))

      expect(renderOpenDialog().textContent).toContain(
        'Recent pipeline profiles (cached in the browser)'
      )
    })

    it('says nothing about caching where bundles are opened from disk', async () => {
      vi.stubGlobal('showOpenFilePicker', vi.fn())
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))

      const dialog = renderOpenDialog()
      expect(dialog.textContent).toContain('Recent pipeline profiles')
      expect(dialog.textContent).not.toContain('cached in the browser')
    })

    it('scrolls a long name sideways instead of cutting it off', async () => {
      const longName = `${'pipeline-with-an-unreasonably-long-name-'.repeat(4)}.zip`
      await seedHistory(['short.zip', longName])
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))
      const dialog = renderOpenDialog()

      const box = dialog.querySelector<HTMLElement>('[data-testid=box-all-bundles]')!
      const rows = [
        ...dialog.querySelectorAll<HTMLElement>('[data-testid=btn-open-bundle-from-list]')
      ]
      const long = rows.find((row) => row.textContent!.trim() === longName)!
      const short = rows.find((row) => row.textContent!.trim() === 'short.zip')!
      // Nothing here checks the `scrollbar` class: this browser reports its
      // scrollbar styling as 'auto' either way, and hides the gutter entirely.
      const style = getComputedStyle(box)
      // The row keeps the whole name, the list is what scrolls, and the dialog
      // itself stays the width it was.
      expect(style.overflowX).toBe('auto')
      expect(long.scrollWidth).toBe(long.clientWidth)
      expect(long.offsetWidth).toBeGreaterThan(box.clientWidth)
      expect(box.scrollWidth).toBeGreaterThan(box.clientWidth)
      // A short name still fills the row, so its hover highlight does too.
      expect(short.offsetWidth).toBe(box.clientWidth)
    })

    it('opens a bundle picked from the dialog list', async () => {
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-recent-bundles]'))
      const dialog = renderOpenDialog()
      const row = dialog.querySelector<HTMLElement>('[data-testid=btn-open-bundle-from-list]')!
      const name = row.textContent!.trim()
      const { id } = (await listSupportBundles()).find((b) => b.name === name)!

      click(row)

      expect(openStoredBundleTab).toHaveBeenCalledWith(id)
      // The dialog has done its job and steps out of the way.
      expect(useGlobalDialog().dialog).toBe(null)
    })

    it('clears the history from the dialog', async () => {
      const { container } = await renderProfiles(1200)

      click(container.querySelector('[data-testid=btn-view-all-bundles]'))
      const dialog = renderOpenDialog()
      click(dialog.querySelector('[data-testid=btn-clear-bundle-history]'))

      await expect.poll(async () => await listSupportBundles()).toEqual([])
      await expect.poll(() => tiles(container)).toEqual([])
      expect(dialog.textContent).toContain('No support bundles opened recently')
    })
  })

  describe('picking a bundle from disk', () => {
    it('picks straight from the button and confirms in the popup', async () => {
      const { container } = await renderProfiles(1200)
      vi.stubGlobal(
        'showOpenFilePicker',
        vi.fn(async () => [fakeHandle('picked.zip')])
      )

      click(container.querySelector('[data-testid=btn-open-support-bundle]'))

      // No menu stands between the button and the file picker: the popup carries
      // the confirmation alone.
      await expect
        .poll(() => container.querySelector('[data-testid=btn-confirm-view-profile]'))
        .toBeTruthy()
      expect(container.querySelector('[data-testid=btn-upload-support-bundle]')).toBe(null)
      expect(container.querySelector('[data-testid=btn-download-support-bundle]')).toBe(null)
      expect(container.textContent).toContain('picked.zip')
      // Opening the viewer waits for a click of its own, or the browser would
      // block the new tab as a popup.
      expect(openStoredBundleTab).not.toHaveBeenCalled()

      // Picking already remembered it, so the row shows it and the viewer can be
      // pointed at the history entry.
      await expect.poll(async () => (await listSupportBundles())[0].name).toBe('picked.zip')
      const { id } = (await listSupportBundles())[0]
      await expect.poll(() => tiles(container)[0].textContent!.trim()).toBe('picked.zip')

      click(container.querySelector('[data-testid=btn-confirm-view-profile]'))

      // The viewer re-reads the file itself; no bytes are handed over.
      expect(openStoredBundleTab).toHaveBeenCalledWith(id)
      expect(openUploadBundleTab).not.toHaveBeenCalled()
    })

    /** Drives the file input the button falls back to without a picker. */
    const pickThroughInput = async (container: HTMLElement, file: File) => {
      vi.stubGlobal('showOpenFilePicker', undefined)
      click(container.querySelector('[data-testid=btn-open-support-bundle]'))
      const input = container.querySelector<HTMLInputElement>(
        '[data-testid=input-upload-support-bundle]'
      )!
      const transfer = new DataTransfer()
      transfer.items.add(file)
      input.files = transfer.files
      input.dispatchEvent(new Event('change', { bubbles: true }))

      await expect
        .poll(() => container.querySelector('[data-testid=btn-confirm-view-profile]'))
        .toBeTruthy()
      click(container.querySelector('[data-testid=btn-confirm-view-profile]'))
    }

    it('remembers a bundle the file input handed over', async () => {
      // Firefox and Safari take this path for every bundle: no picker, so no
      // handle, and the history keeps a copy of the archive instead.
      const { container } = await renderProfiles(1200)

      await pickThroughInput(container, new File(['bundle contents'], 'from-input.zip'))

      const stored = (await listSupportBundles()).find((b) => b.name === 'from-input.zip')!
      expect(stored).toBeTruthy()
      // The viewer reads the copy, so nothing is handed over and the row keeps
      // the bundle for next time.
      expect(openStoredBundleTab).toHaveBeenCalledWith(stored.id)
      expect(openUploadBundleTab).not.toHaveBeenCalled()
      await expect.poll(() => tiles(container)[0].textContent!.trim()).toBe('from-input.zip')
    })

    it('hands the bytes over for an archive the history will not copy', async () => {
      // What `rememberSupportBundleFile` reports for an archive past
      // `maxStoredBundleBytes`, or one the storage quota refused.
      rememberSupportBundleFile.mockResolvedValueOnce(null)
      const { container } = await renderProfiles(1200)

      await pickThroughInput(container, new File(['bundle contents'], 'huge.zip'))

      // With nothing in the history to point the viewer at, the bytes go to the
      // new tab instead.
      expect(openStoredBundleTab).not.toHaveBeenCalled()
      expect(openUploadBundleTab).toHaveBeenCalledOnce()
      await expect.poll(() => sendBundle.mock.calls.length).toBe(1)
      expect((await listSupportBundles()).map((b) => b.name)).not.toContain('huge.zip')
    })
  })
})
