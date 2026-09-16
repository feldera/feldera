/**
 * The shell owns the "unreachable instance" toast, which `Toaster` renders from the root
 * layout and which never expires on its own. Leaving the shell, for the profile viewer or the
 * tenant picker, has to take the toast down: those pages read no live instance, and with the
 * shell's pollers gone nothing would ever dismiss it.
 */
import { createRawSnippet } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const toast = vi.hoisted(() => ({ toastMain: vi.fn(), dismissMain: vi.fn() }))
const api = vi.hoisted(() => ({ isNetworkHealthy: true, isAuthHealthy: true }))

vi.mock('$lib/compositions/useToastNotification', () => ({ useToast: () => toast }))
vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({
    get isNetworkHealthy() {
      return api.isNetworkHealthy
    },
    get isAuthHealthy() {
      return api.isAuthHealthy
    }
  })
}))
// The shell's pollers and the version check; each one talks to the manager, and none of them
// is what this file is about.
vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  useRefreshPipelineList: () => {}
}))
vi.mock('$lib/compositions/health/useClusterHealth.svelte', () => ({
  useRefreshClusterHealth: () => {},
  useClusterHealth: () => ({ current: undefined })
}))
vi.mock('$lib/compositions/usePipelineAction.svelte', () => ({ usePipelineAction: () => {} }))
vi.mock('$lib/compositions/configCache', () => ({ fetchConfigs: async () => ({}) }))
vi.mock('$app/navigation', () => ({
  invalidateAll: vi.fn(),
  afterNavigate: () => {},
  beforeNavigate: () => {}
}))
vi.mock('$app/state', () => ({ page: { url: new URL('http://localhost/'), data: {} } }))
// Chrome that pulls in the API client or the editor bundle.
vi.mock('$lib/components/pipelines/CreatePipelineButton.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/layout/NavigationExtras.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/BookADemo.svelte', () => ({ default: () => {} }))

// Imported AFTER vi.mock so the mocks take effect.
import ShellLayout from './+layout.svelte'

const renderShell = () =>
  render(ShellLayout, {
    children: createRawSnippet(() => ({ render: () => '<div>pipelines</div>' })),
    data: { feldera: { version: '1.0', revision: 'abc' } }
  } as any)

afterEach(() => {
  api.isNetworkHealthy = true
  api.isAuthHealthy = true
  toast.toastMain.mockReset()
  toast.dismissMain.mockReset()
})

describe('(shell) layout', () => {
  it('dismisses the unreachable-instance toast on the way out', async () => {
    api.isNetworkHealthy = false
    const shell = renderShell()
    await vi.waitFor(() => expect(toast.toastMain).toHaveBeenCalled())
    toast.dismissMain.mockReset()

    // What navigating to a page outside the shell does.
    await shell.unmount()

    expect(toast.dismissMain).toHaveBeenCalled()
  })

  it('dismisses the re-authentication toast on the way out', async () => {
    api.isAuthHealthy = false
    const shell = renderShell()
    await vi.waitFor(() => expect(toast.toastMain).toHaveBeenCalled())
    toast.dismissMain.mockReset()

    await shell.unmount()

    expect(toast.dismissMain).toHaveBeenCalled()
  })
})
