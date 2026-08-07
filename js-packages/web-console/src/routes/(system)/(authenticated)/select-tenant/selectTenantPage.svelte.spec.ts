/**
 * The tenant page renders the app header and the picker, nothing else: with no
 * acting tenant there are no pipelines, banners or health to show. The header
 * needs no special mode for this — its tenant-scoped items gate on permissions,
 * which fall back to the read floor when the session reports no tenant, so what
 * is left is the logo, the theme switch and sign-out.
 */
import { describe, expect, it, vi } from 'vitest'
import { page as browser } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

vi.mock('$app/state', () => ({
  page: {
    url: new URL('http://localhost/select-tenant/'),
    data: {
      // No `feldera`: this is exactly the state the page renders in.
      auth: {
        logout: vi.fn(),
        profile: { name: 'Ada', email: 'ada@example.com' },
        userInfo: {},
        accessToken: ''
      }
    }
  }
}))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), invalidateAll: vi.fn() }))
vi.mock('$lib/compositions/switchTenant', () => ({ switchTenant: vi.fn() }))
// Both exports: `auth.ts` reaches this module too, through the header.
vi.mock('$lib/services/redirectTarget', () => ({
  takeRedirectTarget: vi.fn(),
  stashRedirectTarget: vi.fn()
}))
// Dialog bodies the profile menu can open; they fetch on import and none of them
// is reachable in this state.
vi.mock('$lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))

// Imported AFTER vi.mock so the mocks take effect.
import SelectTenantPage from './+page.svelte'

// The page reads only `memberships`; the rest of its PageData is merged-in
// parent layout data that plays no part in what renders here.
const data = {
  memberships: [
    { tenantId: 't-acme', name: 'acme', role: 'admin' },
    { tenantId: 't-beta', name: 'beta', role: 'read' }
  ]
} as any

// The profile trigger's own label is width-gated, so identify it by its icon.
const profileTrigger = () =>
  document.querySelector<HTMLButtonElement>('button:has(.fd-circle-user)')

describe('/select-tenant page', () => {
  it('shows the picker under the header, with no app chrome', async () => {
    await render(SelectTenantPage, { data })

    await expect.element(browser.getByRole('heading', { name: 'Choose a tenant' })).toBeVisible()
    // The header: a logo linking home, and the profile trigger.
    expect(document.querySelector('a[href="/"]')).not.toBeNull()
    expect(profileTrigger()).not.toBeNull()
    // None of the app shell: no create-pipeline drawer, banners or version notice.
    expect(document.body.textContent).not.toContain('Create new pipeline')
    expect(document.body.textContent).not.toContain('Book a demo')
  })

  it('offers the theme switch and sign-out, but nothing tenant-scoped', async () => {
    await render(SelectTenantPage, { data })
    profileTrigger()!.click()

    await expect.poll(() => document.body.textContent).toContain('Sign Out')
    expect(document.body.textContent).toContain('Theme')
    // Permissions fall back to the read floor without a tenant, so every
    // write-gated item hides itself and the page needs no special mode.
    const menu = document.body.textContent ?? ''
    expect(menu).not.toContain('Admin Dashboard')
    expect(menu).not.toContain('Manage API keys')
    expect(menu).not.toContain('Manage OIDC trust')
    // Cluster health is read-role, so the read floor does NOT hide it: the entry
    // gates on a resolved tenant instead. Without one it would show a status
    // nothing polled and link into the gated group.
    expect(menu).not.toContain('Feldera Health')
  })
})
