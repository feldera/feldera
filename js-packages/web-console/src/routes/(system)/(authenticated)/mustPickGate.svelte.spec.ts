// The must-pick gate in the authenticated layout must react to layout data:
// a session can lose its acting tenant after init (invalid-selection recovery
// re-runs load() without a reload), and the layout must then swap the normal
// UI for the tenant picker and stop its pollers.

import { createRawSnippet } from 'svelte'
import { describe, expect, it, vi } from 'vitest'

import { tenantAccessLost } from '$lib/compositions/tenantAccess.svelte'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const polling = vi.hoisted(() => ({
  pipelineList: undefined as (() => boolean) | undefined,
  clusterHealth: undefined as (() => boolean) | undefined
}))

vi.mock('$app/navigation', () => ({ invalidateAll: vi.fn(async () => {}) }))
vi.mock('$app/state', () => ({ page: { url: new URL('http://localhost/'), data: {} } }))
// Stub every heavy child and composition: this test drives only the gate and
// the poller guards; the real TenantPicker stays in to assert what renders.
vi.mock('$lib/components/common/SvelteKitTopLoader.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/dialogs/GlobalModal.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/layout/LineBanner.svelte', () => ({
  default: () => {},
  BannerButton: () => {}
}))
vi.mock('$lib/components/layout/NavigationExtras.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/layout/OverlayDrawer.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/AuthErrorToast.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/BookADemo.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/pipelines/CreatePipelineButton.svelte', () => ({ default: () => {} }))
vi.mock('$lib/compositions/configCache', () => ({
  fetchConfigs: vi.fn(async () => ({ config: undefined, sessionConfig: undefined }))
}))
vi.mock('$lib/compositions/health/useClusterHealth.svelte', () => ({
  useClusterHealth: () => ({
    current: { api: 'healthy', compiler: 'healthy', runner: 'healthy' }
  }),
  useRefreshClusterHealth: (shouldPoll?: () => boolean) => {
    polling.clusterHealth = shouldPoll
  }
}))
vi.mock('$lib/compositions/layout/useAdaptiveDrawer.svelte', () => ({
  useAdaptiveDrawer: () => ({ value: false })
}))
vi.mock('$lib/compositions/layout/useContextDrawer.svelte', () => ({
  useContextDrawer: () => ({ content: null })
}))
vi.mock('$lib/compositions/layout/useGlobalDialog.svelte', () => ({
  useGlobalDialog: () => ({ dialog: null })
}))
vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  useRefreshPipelineList: (shouldPoll?: () => boolean) => {
    polling.pipelineList = shouldPoll
  }
}))
vi.mock('$lib/compositions/usePipelineAction.svelte', () => ({ usePipelineAction: () => {} }))
vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ isNetworkHealthy: true, isAuthHealthy: true })
}))
vi.mock('$lib/compositions/useSystemMessages', () => ({
  useSystemMessages: () => ({ displayedMessages: [], dismiss: () => {}, upsert: () => {} })
}))
vi.mock('$lib/compositions/useToastNotification', () => ({
  useToast: () => ({ toastMain: () => {}, dismissMain: () => {}, toastError: () => {} })
}))
vi.mock('$lib/compositions/switchTenant', () => ({ switchTenant: vi.fn() }))

// Imported AFTER vi.mock so the mocks take effect.
import Layout from './+layout.svelte'

const children = createRawSnippet(() => ({
  render: () => '<div data-testid="page-content">page content</div>'
}))

describe('authenticated layout must-pick gate', () => {
  it('engages when the layout data turns unresolved after init and stops the pollers', async () => {
    const screen = await render(Layout as any, {
      data: { auth: 'none', feldera: undefined } as any,
      children
    })
    await expect.element(page.getByTestId('page-content')).toBeInTheDocument()
    expect(polling.pipelineList?.()).toBe(true)
    expect(polling.clusterHealth?.()).toBe(true)

    await screen.rerender({
      data: {
        auth: 'none',
        feldera: undefined,
        unresolvedTenant: {
          memberships: [{ tenantId: 't-acme', name: 'acme', role: 'admin' }]
        }
      } as any
    })

    // Reverting the gate to an init-time capture leaves the page content up
    // and the pollers running, failing all four assertions below.
    await expect.element(page.getByRole('heading', { name: 'Choose a tenant' })).toBeInTheDocument()
    await expect.element(page.getByTestId('page-content')).not.toBeInTheDocument()
    expect(polling.pipelineList?.()).toBe(false)
    expect(polling.clusterHealth?.()).toBe(false)
  })

  it('engages when the last membership is removed mid-session, without any refetch', async () => {
    tenantAccessLost.reset()
    await render(Layout as any, {
      data: { auth: 'none', feldera: undefined } as any,
      children
    })
    await expect.element(page.getByTestId('page-content')).toBeInTheDocument()

    // No fetch carries this news: it arrives as a failed request, which the
    // global error interceptor turns into `tenantAccessLost`. The layout data
    // stays exactly as it was.
    tenantAccessLost.mark()

    await expect
      .element(page.getByRole('heading', { name: 'No tenant access' }))
      .toBeInTheDocument()
    await expect.element(page.getByTestId('page-content')).not.toBeInTheDocument()
    expect(polling.pipelineList?.()).toBe(false)
    expect(polling.clusterHealth?.()).toBe(false)
    tenantAccessLost.reset()
  })
})
