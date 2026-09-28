/**
 * Tests for how the profile viewer gets hold of a bundle from the user's disk.
 *
 * Where the history has an entry for the bundle, the viewer reads the archive itself
 * instead of having the bytes handed to it: a link then survives a reload, and a bundle
 * picked in this tab leaves behind a URL that opens it again. The `profiler-layout`
 * rendering is mocked out, so what these tests cover is which bundle reaches the viewer
 * and by which of the two routes.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const { PLATFORM_VERSION } = vi.hoisted(() => ({ PLATFORM_VERSION: '1.0.0' }))

const {
  addToBundleHistory,
  getSuitableProfiles,
  isBundlePickerSupported,
  markBundleOpenedNow,
  pickSupportBundle,
  processProfileFiles,
  readArchive,
  receiveUploadedBundle,
  replaceState,
  requestPermission,
  resolveStoredBundle
} = vi.hoisted(() => ({
  addToBundleHistory: vi.fn(),
  getSuitableProfiles: vi.fn(),
  isBundlePickerSupported: vi.fn(() => true),
  markBundleOpenedNow: vi.fn(),
  pickSupportBundle: vi.fn(),
  processProfileFiles: vi.fn(),
  readArchive: vi.fn(),
  receiveUploadedBundle: vi.fn(),
  replaceState: vi.fn(),
  requestPermission: vi.fn(),
  resolveStoredBundle: vi.fn()
}))

// Only the parts the page drives. The diagram, the ELK layout and the zip reader have
// their own tests in profiler-layout.
vi.mock('profiler-layout', () => ({
  createLoadGuard:
    ({ setLoading, onFinally }: { setLoading: (l: boolean) => void; onFinally?: () => void }) =>
    async (work: () => Promise<void>, onError: (e: unknown) => void) => {
      setLoading(true)
      try {
        await work()
      } catch (e) {
        onError(e)
      } finally {
        setLoading(false)
        onFinally?.()
      }
    },
  getSuitableProfiles,
  processProfileFiles,
  SupportBundleViewerLayout: () => {}
}))
vi.mock('virtual:feldera-triage-plugins', () => ({
  default: [],
  createBundle: vi.fn(),
  TriageResults: class {
    results = []
  }
}))
vi.mock('$app/navigation', () => ({
  replaceState,
  goto: vi.fn(),
  invalidateAll: vi.fn(),
  preloadCode: vi.fn(() => Promise.resolve())
}))
vi.mock('$app/state', () => ({
  page: {
    url: new URL('http://localhost/profile-viewer'),
    data: {
      feldera: {
        version: PLATFORM_VERSION,
        revision: '0',
        edition: 'Enterprise',
        changelog: 'https://example.com/changelog',
        unstableFeatures: [],
        permissions: ['read', 'write']
      },
      auth: {
        logout: vi.fn(),
        profile: { name: 'Ada', email: 'ada@example.com' },
        userInfo: {},
        accessToken: ''
      }
    }
  }
}))
vi.mock('$lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/services/redirectTarget', () => ({
  takeRedirectTarget: vi.fn(),
  stashRedirectTarget: vi.fn()
}))
vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({
    getPipelineSupportBundle: vi.fn(() => ({ dataPromise: new Promise(() => {}) }))
  })
}))
// `storedBundleUrl` is the real one, so that the URL these tests assert on is the URL
// a new tab would be opened with.
vi.mock('$lib/compositions/profileBundleHandoff', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$lib/compositions/profileBundleHandoff')>()),
  receiveUploadedBundle,
  openRemoteBundleTab: vi.fn(),
  openStoredBundleTab: vi.fn(),
  openUploadBundleTab: vi.fn()
}))
// Only the functions the page calls are replaced. `isPermissionRequired` is left as it
// is, so that these tests fail if the page stops recognizing a refused read.
vi.mock('$lib/services/supportBundleHistory', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$lib/services/supportBundleHistory')>()),
  addToBundleHistory,
  isBundlePickerSupported,
  markBundleOpenedNow,
  pickSupportBundle,
  resolveStoredBundle
}))

// These imports come after the vi.mock calls above, so that the mocks are in place.
import ProfileViewerPage from './+page.svelte'

const BUNDLE_BYTES = new Uint8Array([1, 2, 3])

/** A history entry, with the two operations the page calls under the test's control. */
const historyEntry = () => ({
  id: 7,
  name: 'checkout-2026-01-14.zip',
  openedAt: 1,
  ops: { bytes: () => 0, read: readArchive, requestPermission }
})
const BUNDLE = historyEntry()

/**
 * Stands in for the `FileSystemFileHandle` the picker returns. Only `name` and
 * `getFile` are reached from here.
 */
const fakeHandle = (name: string) => ({
  name,
  kind: 'file',
  getFile: async () => new File(['bundle contents'], name)
})

type PageData = {
  pipelineName?: string
  source?: 'remote' | 'upload'
  collect?: boolean
  bundle?: number | 'invalid'
  channel?: string
}

let mounted: { unmount: () => Promise<void> } | undefined

const renderViewer = (data: PageData = {}) => {
  const rendered = render(ProfileViewerPage, {
    props: {
      data: { pipelineName: '', source: 'remote', collect: true, channel: '', ...data }
    } as any
  })
  mounted = rendered as any
  return rendered.container
}

const find = (container: HTMLElement, testid: string) =>
  container.querySelector<HTMLElement>(`[data-testid=${testid}]`)

describe('profile viewer — uploaded bundles', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    isBundlePickerSupported.mockReturnValue(true)
    readArchive.mockResolvedValue(BUNDLE_BYTES)
    requestPermission.mockResolvedValue(true)
    markBundleOpenedNow.mockResolvedValue(undefined)
    getSuitableProfiles.mockReturnValue([[new Date('2026-01-14T00:00:00Z'), []]])
    processProfileFiles.mockResolvedValue({
      profile: {},
      dataflow: {},
      sources: [],
      logText: '',
      globalMetrics: {},
      runtimeConfig: {},
      pipelineName: 'checkout'
    })
    // Nothing is waiting on the other side of a handoff channel in these tests.
    receiveUploadedBundle.mockReturnValue(new Promise(() => {}))
  })

  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
  })

  it('reads a linked bundle itself instead of waiting for a handoff', async () => {
    resolveStoredBundle.mockResolvedValue(historyEntry())

    renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(resolveStoredBundle).toHaveBeenCalledWith(BUNDLE.id)
    expect(getSuitableProfiles).toHaveBeenCalledWith(BUNDLE_BYTES)
    // The page reads first and asks only if that fails, so the common case, where
    // the browser still holds the permission, costs no prompt and no query.
    expect(requestPermission).not.toHaveBeenCalled()
    // The cross-tab handoff carries only bundles with no history entry.
    expect(receiveUploadedBundle).not.toHaveBeenCalled()
  })

  it('waits for the handoff when the link carries no bundle', async () => {
    renderViewer({ source: 'upload', channel: 'channel-1' })

    await expect.poll(() => receiveUploadedBundle.mock.calls.length).toBe(1)
    expect(receiveUploadedBundle).toHaveBeenCalledWith('channel-1')
    expect(resolveStoredBundle).not.toHaveBeenCalled()
  })

  it('moves a reopened bundle to the front of the history', async () => {
    // The history is ordered by when each bundle was last opened, and opening one
    // through its link is an opening like any other.
    resolveStoredBundle.mockResolvedValue(historyEntry())

    renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(markBundleOpenedNow).toHaveBeenCalledWith(BUNDLE.id)
  })

  it('loads the bundle even when recording the reopen fails', async () => {
    // The history is a convenience. Failing to update it must not cost the user the
    // bundle they asked for.
    resolveStoredBundle.mockResolvedValue(historyEntry())
    markBundleOpenedNow.mockRejectedValue(new Error('the database is gone'))

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(container.textContent).not.toContain('the database is gone')
  })

  it('reports a link whose bundle id is not an id at all', async () => {
    // A truncated or hand-edited `?bundle=` names no entry, and has to say so. Reading
    // it as an absent id would send the page to the handoff instead, where it would
    // wait out the timeout and then blame a tab the user never opened.
    resolveStoredBundle.mockRejectedValue(new Error('This support bundle is no longer here.'))

    const container = renderViewer({ source: 'upload', bundle: 'invalid' })

    await expect
      .poll(() => container.textContent)
      .toContain('This support bundle is no longer here.')
    expect(resolveStoredBundle).toHaveBeenCalledWith(undefined)
    expect(receiveUploadedBundle).not.toHaveBeenCalled()
  })

  it('reports a permission the user refused, and what to do about it', async () => {
    resolveStoredBundle.mockResolvedValue(historyEntry())
    readArchive.mockRejectedValueOnce(new DOMException('no permission', 'NotAllowedError'))
    requestPermission.mockResolvedValue(false)

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })
    await expect.poll(() => find(container, 'btn-open-stored-bundle')).toBeTruthy()

    find(container, 'btn-open-stored-bundle')!.click()

    // Dismissing the browser's prompt leaves the user where they started, so the
    // message has to name the way out rather than stop at what failed.
    await expect.poll(() => container.textContent).toContain(BUNDLE.name)
    expect(container.textContent).toContain('Open from disk')
    expect(container.textContent).toContain('allow access when the browser asks')
    expect(processProfileFiles).not.toHaveBeenCalled()
  })

  it('keeps the empty state when the user dismisses the file picker', async () => {
    // Dismissing the operating system's dialog is not an error: nothing loads, nothing
    // is remembered, and no error is shown.
    pickSupportBundle.mockResolvedValue(null)

    const container = renderViewer()
    const openFromDisk = [...container.querySelectorAll('button')].find((button) =>
      button.textContent?.includes('Open a support bundle')
    )!

    openFromDisk.click()

    await expect.poll(() => pickSupportBundle.mock.calls.length).toBe(1)
    // Let the dismissed pick settle before checking that nothing moved.
    await new Promise((resolve) => setTimeout(resolve))
    expect(addToBundleHistory).not.toHaveBeenCalled()
    expect(processProfileFiles).not.toHaveBeenCalled()
    expect(replaceState).not.toHaveBeenCalled()
  })

  it('asks for access when the browser dropped the read permission', async () => {
    resolveStoredBundle.mockResolvedValue(historyEntry())
    readArchive.mockRejectedValueOnce(new DOMException('no permission', 'NotAllowedError'))

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })

    // The read is what reports that permission is missing, and asking for it needs a
    // click of its own, so nothing loads until the user clicks. This is what the page
    // looks like when the link is opened directly, in a later session.
    await expect.poll(() => find(container, 'btn-open-stored-bundle')).toBeTruthy()
    expect(find(container, 'box-support-bundle-confirm')!.textContent).toContain(BUNDLE.name)
    expect(processProfileFiles).not.toHaveBeenCalled()

    find(container, 'btn-open-stored-bundle')!.click()

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(requestPermission).toHaveBeenCalledOnce()
  })

  it('reports a read that failed for any other reason', async () => {
    // Only a refused read offers the user something to do about it. Anything else is
    // an error, and drawing the permission button for it would send the user round a
    // loop that cannot end.
    resolveStoredBundle.mockResolvedValue(historyEntry())
    readArchive.mockRejectedValue(new DOMException('The file is no longer there', 'NotFoundError'))

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect.poll(() => container.textContent).toContain('The file is no longer there')
    expect(find(container, 'btn-open-stored-bundle')).toBe(null)
  })

  it('reports a bundle that has fallen out of the history', async () => {
    resolveStoredBundle.mockRejectedValue(new Error('This support bundle is no longer here.'))

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect
      .poll(() => container.textContent)
      .toContain('This support bundle is no longer here.')
    expect(processProfileFiles).not.toHaveBeenCalled()
  })

  it('rewrites the URL after a bundle is picked here, so a reload reopens it', async () => {
    const handle = fakeHandle(BUNDLE.name)
    pickSupportBundle.mockResolvedValue(handle)
    addToBundleHistory.mockResolvedValue({ ...historyEntry(), id: 42 })

    // No pipeline and no upload, so the empty state offers to open a bundle.
    const container = renderViewer()
    const openFromDisk = [...container.querySelectorAll('button')].find((button) =>
      button.textContent?.includes('Open a support bundle')
    )!
    expect(openFromDisk).toBeTruthy()

    openFromDisk.click()

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    // Picking a file remembers it in the history, and the URL then names that entry.
    expect(addToBundleHistory).toHaveBeenCalledWith(handle)
    expect(replaceState).toHaveBeenCalledWith('/profile-viewer?source=upload&bundle=42', {})
  })

  it('remembers a bundle from the file input by keeping a copy of it', async () => {
    // The route taken where `showOpenFilePicker` is missing. The copy kept in the
    // history gives that browser a URL worth reloading, just as a handle does in
    // Chromium.
    isBundlePickerSupported.mockReturnValue(false)
    addToBundleHistory.mockResolvedValue({ ...historyEntry(), id: 11 })

    const container = renderViewer()
    const input = find(container, 'input-open-support-bundle') as HTMLInputElement
    const transfer = new DataTransfer()
    transfer.items.add(new File(['bundle contents'], 'from-input.zip'))
    input.files = transfer.files
    input.dispatchEvent(new Event('change', { bubbles: true }))

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    // A browser with no picker hands over a `File`, which is what tells the history to
    // keep a copy of the archive rather than a reference to the file on disk.
    expect(addToBundleHistory).toHaveBeenCalledOnce()
    expect(addToBundleHistory.mock.calls[0][0]).toBeInstanceOf(File)
    expect(pickSupportBundle).not.toHaveBeenCalled()
    expect(replaceState).toHaveBeenCalledWith('/profile-viewer?source=upload&bundle=11', {})
  })

  it('keeps the URL unchanged when the history does not store the bundle', async () => {
    isBundlePickerSupported.mockReturnValue(false)
    addToBundleHistory.mockResolvedValue(null)

    const container = renderViewer()
    const input = find(container, 'input-open-support-bundle') as HTMLInputElement
    const transfer = new DataTransfer()
    transfer.items.add(new File(['bundle contents'], 'huge.zip'))
    input.files = transfer.files
    input.dispatchEvent(new Event('change', { bubbles: true }))

    // The bundle still opens, but nothing can open it a second time, so the URL names
    // no history entry.
    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(replaceState).not.toHaveBeenCalled()
  })
})
