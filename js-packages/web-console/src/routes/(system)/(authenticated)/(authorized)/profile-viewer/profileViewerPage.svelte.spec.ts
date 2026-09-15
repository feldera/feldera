/**
 * How the profile viewer gets hold of an uploaded bundle.
 *
 * The viewer prefers the File System Access handle the bundle history keeps, because it
 * can then read the archive from disk itself: a link survives a reload, and a bundle
 * picked in this tab leaves a URL that reopens it. The heavy `profiler-layout`
 * rendering is mocked out, so what is under test is which bundle reaches the viewer,
 * and how.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const { PLATFORM_VERSION } = vi.hoisted(() => ({ PLATFORM_VERSION: '1.0.0' }))

const {
  getSuitableProfiles,
  isBundlePickerSupported,
  pickSupportBundle,
  processProfileFiles,
  readStoredBundle,
  readSupportBundle,
  receiveUploadedBundle,
  rememberSupportBundle,
  rememberSupportBundleFile,
  replaceState,
  requestBundleReadPermission,
  resolveStoredBundle
} = vi.hoisted(() => ({
  getSuitableProfiles: vi.fn(),
  isBundlePickerSupported: vi.fn(() => true),
  pickSupportBundle: vi.fn(),
  processProfileFiles: vi.fn(),
  readStoredBundle: vi.fn(),
  readSupportBundle: vi.fn(),
  receiveUploadedBundle: vi.fn(),
  rememberSupportBundle: vi.fn(),
  rememberSupportBundleFile: vi.fn(),
  replaceState: vi.fn(),
  requestBundleReadPermission: vi.fn(),
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
vi.mock('$lib/compositions/profileBundleHandoff', () => ({
  receiveUploadedBundle,
  openRemoteBundleTab: vi.fn(),
  openStoredBundleTab: vi.fn(),
  openUploadBundleTab: vi.fn()
}))
vi.mock('$lib/services/supportBundleHistory', () => ({
  isBundlePickerSupported,
  pickSupportBundle,
  readStoredBundle,
  readSupportBundle,
  rememberSupportBundle,
  requestBundleReadPermission,
  resolveStoredBundle,
  listSupportBundles: vi.fn(async () => []),
  queryBundleReadPermission: vi.fn(async () => 'granted'),
  touchSupportBundle: vi.fn(),
  clearSupportBundles: vi.fn()
}))
// Mocked whole, because the real module reaches into the history module for IndexedDB
// access, which the mock above does not provide.
vi.mock('$lib/services/supportBundleCache', () => ({
  isBundleCacheRequired: vi.fn(() => false),
  rememberSupportBundleFile
}))

// Imported AFTER vi.mock so the mocks take effect.
import ProfileViewerPage from './+page.svelte'

const BUNDLE = { id: 7, name: 'checkout-2026-01-14.zip', openedAt: 1, handle: { name: 'x' } }
const BUNDLE_BYTES = new Uint8Array([1, 2, 3])

type PageData = {
  pipelineName?: string
  source?: 'remote' | 'upload'
  collect?: boolean
  bundle?: number
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
    readStoredBundle.mockResolvedValue(BUNDLE_BYTES)
    readSupportBundle.mockResolvedValue(BUNDLE_BYTES)
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
    resolveStoredBundle.mockResolvedValue({ bundle: BUNDLE, needsPermission: false })

    renderViewer({ source: 'upload', bundle: BUNDLE.id })

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(resolveStoredBundle).toHaveBeenCalledWith(BUNDLE.id)
    expect(readStoredBundle).toHaveBeenCalledWith(BUNDLE)
    expect(getSuitableProfiles).toHaveBeenCalledWith(BUNDLE_BYTES)
    // The cross-tab handoff carries only bundles with no history entry.
    expect(receiveUploadedBundle).not.toHaveBeenCalled()
  })

  it('waits for the handoff when the link carries no bundle', async () => {
    renderViewer({ source: 'upload', channel: 'channel-1' })

    await expect.poll(() => receiveUploadedBundle.mock.calls.length).toBe(1)
    expect(receiveUploadedBundle).toHaveBeenCalledWith('channel-1')
    expect(resolveStoredBundle).not.toHaveBeenCalled()
  })

  it('asks for access when the browser dropped the read permission', async () => {
    resolveStoredBundle.mockResolvedValue({ bundle: BUNDLE, needsPermission: true })
    requestBundleReadPermission.mockResolvedValue(true)

    const container = renderViewer({ source: 'upload', bundle: BUNDLE.id })

    // Nothing is read without a click of its own, which is what the grant needs. This
    // is the page reached by opening the link directly, after a browser restart.
    await expect.poll(() => find(container, 'btn-open-stored-bundle')).toBeTruthy()
    expect(find(container, 'box-support-bundle-confirm')!.textContent).toContain(BUNDLE.name)
    expect(readStoredBundle).not.toHaveBeenCalled()

    find(container, 'btn-open-stored-bundle')!.click()

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(requestBundleReadPermission).toHaveBeenCalledWith(BUNDLE)
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
    pickSupportBundle.mockResolvedValue({ name: BUNDLE.name, kind: 'file' })
    rememberSupportBundle.mockResolvedValue({ ...BUNDLE, id: 42 })

    // No pipeline and no upload, so the empty state offers to open a bundle.
    const container = renderViewer()
    const openFromDisk = [...container.querySelectorAll('button')].find((button) =>
      button.textContent?.includes('Open a support bundle')
    )!
    expect(openFromDisk).toBeTruthy()

    openFromDisk.click()

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    // Picking records the bundle, and the URL names that entry.
    expect(rememberSupportBundle).toHaveBeenCalledOnce()
    expect(replaceState).toHaveBeenCalledWith('/profile-viewer?source=upload&bundle=42', {})
  })

  it('remembers a bundle from the file input by keeping a copy of it', async () => {
    // The path browsers without a file picker take. The copy in the history gives them
    // a reloadable URL, as a handle does in Chromium.
    isBundlePickerSupported.mockReturnValue(false)
    rememberSupportBundleFile.mockResolvedValue({ ...BUNDLE, id: 11 })

    const container = renderViewer()
    const input = find(container, 'input-open-support-bundle') as HTMLInputElement
    const transfer = new DataTransfer()
    transfer.items.add(new File(['bundle contents'], 'from-input.zip'))
    input.files = transfer.files
    input.dispatchEvent(new Event('change', { bubbles: true }))

    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(rememberSupportBundleFile).toHaveBeenCalledOnce()
    expect(rememberSupportBundle).not.toHaveBeenCalled()
    expect(replaceState).toHaveBeenCalledWith('/profile-viewer?source=upload&bundle=11', {})
  })

  it('leaves no URL behind for a bundle the history would not take', async () => {
    isBundlePickerSupported.mockReturnValue(false)
    rememberSupportBundleFile.mockResolvedValue(null)

    const container = renderViewer()
    const input = find(container, 'input-open-support-bundle') as HTMLInputElement
    const transfer = new DataTransfer()
    transfer.items.add(new File(['bundle contents'], 'huge.zip'))
    input.files = transfer.files
    input.dispatchEvent(new Event('change', { bubbles: true }))

    // The bundle still opens, but it cannot be reopened, so the URL names no entry.
    await expect.poll(() => processProfileFiles.mock.calls.length).toBe(1)
    expect(replaceState).not.toHaveBeenCalled()
  })
})
