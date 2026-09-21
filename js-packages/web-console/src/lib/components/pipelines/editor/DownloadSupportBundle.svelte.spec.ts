/**
 * Tests for the pipeline editor's support bundle dropdown: the menu entries specific to
 * the editor, and the two ways a bundle reaches the viewer from here.
 * `showOpenFilePicker` gives back a handle, which the history stores as it is, and an
 * `<input type=file>` gives back a `File`, of which the history keeps a copy.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const {
  openRemoteBundleTab,
  openStoredBundleTab,
  openUploadBundleTab,
  sendBundle,
  showOpenFilePicker
} = vi.hoisted(() => {
  const sendBundle = vi.fn(async (_bundle: ArrayBuffer) => {})
  return {
    openRemoteBundleTab: vi.fn(),
    openStoredBundleTab: vi.fn(),
    openUploadBundleTab: vi.fn(() => ({ send: sendBundle, cancel: vi.fn() })),
    sendBundle,
    showOpenFilePicker: vi.fn()
  }
})

vi.mock('$lib/compositions/profileBundleHandoff', () => ({
  openRemoteBundleTab,
  openUploadBundleTab,
  openStoredBundleTab,
  receiveUploadedBundle: vi.fn()
}))
vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({
    downloadPipelineSupportBundle: vi.fn(() => ({
      dataPromise: Promise.resolve({}),
      cancel: vi.fn()
    }))
  })
}))

// These imports come after the vi.mock calls above, so that the mocks are in place.
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import { clearBundleHistory, listBundleHistory } from '$lib/services/supportBundleHistory'
import DownloadSupportBundle from './DownloadSupportBundle.svelte'

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

const PIPELINE = 'my-pipeline'

let mounted: { unmount: () => Promise<void> } | undefined

const renderControls = () => {
  const rendered = render(DownloadSupportBundle, { pipelineName: PIPELINE })
  mounted = rendered as any
  return rendered.container
}

const find = (container: HTMLElement, testid: string) =>
  container.querySelector<HTMLElement>(`[data-testid=${testid}]`)

const click = (element: Element | null | undefined) => (element as HTMLElement).click()

/** Opens the dropdown and returns once its menu is on screen. */
const openDropdown = async (container: HTMLElement) => {
  click(container.querySelector('[aria-label="Support bundle options"]'))
  await expect.poll(() => find(container, 'box-support-bundle-menu')).toBeTruthy()
}

/** Chooses a file the way a browser without `showOpenFilePicker` does, through the input. */
const pickThroughFileInput = (container: HTMLElement, name: string) => {
  const input = find(container, 'input-upload-support-bundle') as HTMLInputElement
  const transfer = new DataTransfer()
  transfer.items.add(new File(['bundle contents'], name))
  input.files = transfer.files
  input.dispatchEvent(new Event('change', { bubbles: true }))
}

describe('DownloadSupportBundle.svelte', () => {
  beforeEach(() => {
    localStorage.setItem('layout/pipelines/supportBundle/collect', 'true')
    vi.clearAllMocks()
  })

  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    useGlobalDialog().dialog = null
    vi.unstubAllGlobals()
  })

  it('opens the viewer for the pipeline straight from the button', async () => {
    const container = renderControls()

    click(find(container, 'btn-view-profile'))

    expect(openRemoteBundleTab).toHaveBeenCalledWith(PIPELINE, true)
  })

  it('offers download, collect and upload in the dropdown', async () => {
    const container = renderControls()

    await openDropdown(container)

    expect(find(container, 'btn-download-support-bundle')).toBeTruthy()
    expect(find(container, 'btn-upload-support-bundle')).toBeTruthy()
    expect(container.querySelector('input[type=checkbox]')).toBeTruthy()
  })

  it('closes the dropdown and opens the download dialog', async () => {
    const container = renderControls()
    await openDropdown(container)

    click(find(container, 'btn-download-support-bundle'))

    expect(useGlobalDialog().dialog).not.toBe(null)
    await expect.poll(() => find(container, 'box-support-bundle-menu')).toBe(null)
  })

  it('remembers a bundle picked here and links the viewer to it', async () => {
    // The bundle goes into the history, so that the viewer tab can read the file from
    // disk itself.
    await clearBundleHistory()
    vi.stubGlobal('showOpenFilePicker', showOpenFilePicker)
    showOpenFilePicker.mockResolvedValue([fakeHandle('bundle-from-picker.zip')])
    const container = renderControls()
    await openDropdown(container)

    click(find(container, 'btn-upload-support-bundle'))

    await expect.poll(() => find(container, 'btn-confirm-view-profile')).toBeTruthy()
    expect(container.textContent).toContain('bundle-from-picker.zip')
    await expect
      .poll(async () => (await listBundleHistory())[0]?.name)
      .toBe('bundle-from-picker.zip')
    const { id } = (await listBundleHistory())[0]

    click(find(container, 'btn-confirm-view-profile'))

    expect(openStoredBundleTab).toHaveBeenCalledWith(id)
    expect(openUploadBundleTab).not.toHaveBeenCalled()
  })

  it('remembers a bundle from the file input where the browser has no picker', async () => {
    await clearBundleHistory()
    vi.stubGlobal('showOpenFilePicker', undefined)
    const container = renderControls()
    await openDropdown(container)

    click(find(container, 'btn-upload-support-bundle'))
    expect(showOpenFilePicker).not.toHaveBeenCalled()
    pickThroughFileInput(container, 'bundle-from-input.zip')
    await expect.poll(() => find(container, 'btn-confirm-view-profile')).toBeTruthy()
    expect(container.textContent).toContain('bundle-from-input.zip')
    // Picking a file must not open the tab by itself. `window.open` has to run inside
    // the click on the confirmation, or the browser blocks it as a popup.
    expect(openStoredBundleTab).not.toHaveBeenCalled()
    await expect
      .poll(async () => (await listBundleHistory())[0]?.name)
      .toBe('bundle-from-input.zip')
    const { id } = (await listBundleHistory())[0]

    click(find(container, 'btn-confirm-view-profile'))

    // The history holds a copy of the archive, so the viewer reads it from there and
    // nothing is handed from one tab to the other.
    expect(openStoredBundleTab).toHaveBeenCalledWith(id)
    expect(openUploadBundleTab).not.toHaveBeenCalled()
    expect(sendBundle).not.toHaveBeenCalled()
  })
})
