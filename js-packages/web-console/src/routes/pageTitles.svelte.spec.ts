/**
 * Each page sets its own browser tab title (issue #5805).
 *
 * Svelte does not restore `document.title` when a page unmounts. A page without a `<title>`
 * keeps the title of the previous page. This spec covers the pages that have no spec of
 * their own.
 */

import type { Component } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

// The page chrome and content do not affect the title.
vi.mock('#lib/components/admin/AdminPage.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/layout/AppHeader.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/layout/NavigationExtras.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/layout/PipelineBreadcrumbs.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/other/BookADemo.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/other/DemosGrid.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/pipelines/CreatePipelineButton.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/pipelines/editor/WarningBanner.svelte', () => ({ default: () => {} }))
vi.mock('#lib/compositions/layout/useAdaptiveDrawer.svelte.ts', () => ({
  useAdaptiveDrawer: () => ({ isMobileDrawer: false, value: false })
}))
vi.mock('#lib/compositions/useDemos.svelte.ts', () => ({
  useDemos: () => ({ current: [] })
}))
vi.mock('#lib/compositions/health/useClusterHealth.svelte.ts', () => ({
  useClusterHealth: () => ({ current: undefined })
}))
vi.mock('#lib/compositions/useEdition.svelte.ts', () => ({
  useIsEnterprise: () => ({ current: false })
}))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({ getClusterEvents: async () => [] })
}))
vi.mock('$app/state', () => ({
  page: { status: 500, error: { message: 'Internal Error' }, url: new URL('http://localhost/') }
}))

import RootErrorPage from './+error.svelte'
import AuthenticatedErrorPage from './(system)/(authenticated)/+error.svelte'
import AdminPage from './(system)/(authenticated)/(authorized)/(shell)/admin/+page.svelte'
import DemosPage from './(system)/(authenticated)/(authorized)/(shell)/demos/+page.svelte'
import HealthPage from './(system)/(authenticated)/(authorized)/(shell)/health/+page.svelte'

let mounted: { unmount: () => Promise<void> } | undefined

afterEach(async () => {
  await mounted?.unmount()
  mounted = undefined
})

describe('page titles', () => {
  it.each([
    ['/admin', 'Administration | Feldera', AdminPage],
    ['/demos', 'Demos | Feldera', DemosPage],
    ['/health', 'Health | Feldera', HealthPage],
    ['the root error page', 'Error | Feldera', RootErrorPage],
    ['the signed-in error page', 'Error | Feldera', AuthenticatedErrorPage]
  ] as [
    string,
    string,
    Component
  ][])('%s replaces a stale tab title with %s', async (_page, expectedTitle, PageComponent) => {
    document.title = 'nb-unihost-2 | Profile Viewer'

    mounted = render(PageComponent) as unknown as typeof mounted

    await expect.poll(() => document.title).toBe(expectedTitle)
  })
})
