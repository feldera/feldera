/**
 * The right navigation drawer: on a screen too narrow for the page header, the drawer
 * holds creating a pipeline, opening a support bundle, booking a demo, and the
 * documentation and community links.
 *
 * The layout is rendered whole, so the bundle button opens into the drawer's own dialog
 * host, as it does in the running app.
 */

import { createRawSnippet } from 'svelte'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

vi.mock('$app/state', () => ({
  page: {
    url: new URL('http://localhost/'),
    data: { feldera: { permissions: ['read:pipeline', 'write:pipeline'] } }
  }
}))
vi.mock('$app/navigation', () => ({
  goto: vi.fn(),
  invalidateAll: vi.fn(),
  preloadCode: vi.fn(() => Promise.resolve()),
  // The layout's top loader subscribes to navigation; nothing navigates here.
  afterNavigate: vi.fn(),
  beforeNavigate: vi.fn(),
  onNavigate: vi.fn()
}))
// Booking a demo tags its URL from the analytics loader, which no test loads.
vi.mock('$lib/services/analytics', () => ({ captureEvent: vi.fn() }))
// The layout's own pollers: nothing here talks to a backend.
vi.mock('$lib/compositions/configCache', () => ({ fetchConfigs: vi.fn(async () => ({})) }))
vi.mock('$lib/compositions/health/useClusterHealth.svelte', () => ({
  useClusterHealth: () => ({ current: { api: 'healthy', compiler: 'healthy', runner: 'healthy' } }),
  useRefreshClusterHealth: vi.fn()
}))
vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  useRefreshPipelineList: vi.fn(),
  usePipelineList: () => ({
    get pipelines() {
      return []
    }
  }),
  useUpdatePipelineList: () => ({
    updatePipelines: vi.fn(),
    updatePipeline: vi.fn(),
    discardPendingListRefresh: vi.fn()
  })
}))
vi.mock('$lib/compositions/usePipelineAction.svelte', () => ({ usePipelineAction: vi.fn() }))
vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ isNetworkHealthy: true, isAuthHealthy: true })
}))

// Imported AFTER vi.mock so the mocks take effect.
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import AuthorizedLayout from './+layout.svelte'

const pageContent = createRawSnippet(() => ({ render: () => '<div>PAGE</div>' }))

/** The drawer's `p-4`, in px. */
const DRAWER_PADDING = 16

let mounted: { unmount: () => Promise<void> } | undefined

/** Mounts the layout with the right drawer already pulled out. */
const renderDrawer = async () => {
  // The drawer remembers whether it is open, and closes itself on a screen wide enough
  // not to need it. The test iframe is narrower than that.
  localStorage.setItem('layout/drawer/right', 'true')
  const rendered = render(AuthorizedLayout, {
    children: pageContent,
    data: { feldera: { version: '1.0.0', revision: '0' } } as any
  })
  mounted = rendered as any
  const container = rendered.container
  const drawer = [...container.querySelectorAll<HTMLElement>('[role=dialog]')].find((panel) =>
    panel.textContent?.includes('Book a demo')
  )!
  await expect.poll(() => drawer.getBoundingClientRect().width).toBeGreaterThan(0)
  return { container, drawer }
}

/** Where the drawer's panel is: off to the right once it closes. */
const isDrawerOpen = (drawer: HTMLElement) =>
  !drawer.parentElement!.className.includes('translate-x-full')

const labelled = (drawer: HTMLElement, text: string) =>
  [...drawer.querySelectorAll<HTMLElement>('button,a')].find((control) =>
    control.textContent?.includes(text)
  )!

describe('(authorized) right drawer', () => {
  beforeEach(() => {
    document.documentElement.setAttribute('data-theme', 'feldera-modern-theme')
    useGlobalDialog().dialog = null
  })

  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    useGlobalDialog().dialog = null
    localStorage.removeItem('layout/drawer/right')
  })

  it('offers the support bundle dialog between New Pipeline and Book a demo', async () => {
    const { drawer } = await renderDrawer()

    expect(isDrawerOpen(drawer)).toBe(true)
    const bundles = drawer.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!
    const top = (control: HTMLElement) => control.getBoundingClientRect().top
    expect(top(labelled(drawer, 'New Pipeline'))).toBeLessThan(top(bundles))
    expect(top(bundles)).toBeLessThan(top(labelled(drawer, 'Book a demo')))
    // Centred in the drawer's column, like its neighbours: as wide as its label, with
    // equal space on either side.
    const button = bundles.getBoundingClientRect()
    const column = drawer.getBoundingClientRect()
    expect(button.width).toBeLessThan(column.width - 2 * DRAWER_PADDING)
    expect(Math.abs(button.left - column.left - (column.right - button.right))).toBeLessThanOrEqual(
      1
    )
  })

  it('steps aside for the dialog it opens', async () => {
    const { container, drawer } = await renderDrawer()

    drawer.querySelector<HTMLElement>('[data-testid=btn-open-support-bundle]')!.click()

    // The dialog covers the screen, so the drawer retracts.
    await expect.poll(() => isDrawerOpen(drawer)).toBe(false)
    expect(container.querySelector('[data-testid=box-all-bundles]')).toBeTruthy()
  })
})
