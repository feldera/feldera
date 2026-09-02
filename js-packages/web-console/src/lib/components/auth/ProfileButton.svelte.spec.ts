/**
 * The profile menu is the one piece of chrome that renders on both sides of the
 * `(authorized)` gate, so its tenant-scoped entries have to hide themselves when
 * no tenant is resolved. Every one of them does that through `<RBAC>`, including
 * the read-role cluster health entry: a session without an acting tenant holds no
 * permissions at all, so no gate needs to ask about tenants directly.
 *
 * Fixtures derive `role` and `permissions` from a raw session role string the way
 * `+layout.ts` does, so relaxing that rule fails these tests rather than silently
 * restoring a menu of dead links.
 */
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import type { ClusterHealthStatus } from '$lib/compositions/health/useClusterHealth.svelte'
import { permissionsOf, roleOf } from '$lib/services/rbac'

// Hoisted so the `$app/state` factory below can close over it: each test sets
// `state.data` to the session shape it is about.
const state = vi.hoisted(() => ({
  url: new URL('http://localhost/'),
  data: {} as Record<string, unknown>
}))
vi.mock('$app/state', () => ({ page: state }))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), invalidateAll: vi.fn() }))
vi.mock('$lib/compositions/switchTenant', () => ({ switchTenant: vi.fn() }))
// Dialog bodies the menu can open; they fetch on import.
vi.mock('$lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))

// Imported AFTER vi.mock so the mocks take effect.
import ProfileButton from './ProfileButton.svelte'

const auth = {
  logout: vi.fn(),
  profile: { name: 'Ada', email: 'ada@example.com' },
  userInfo: {},
  accessToken: ''
}

// Takes the session payload's `role` as the server spells it, and derives the
// rest the way `buildFelderaData` does. The server sends `role` and the acting
// tenant fields together or not at all.
const session = (sessionRole: string | null, tenantId: string) => {
  const role = roleOf(sessionRole)
  return {
    auth,
    feldera: {
      tenantId,
      tenantName: tenantId ? 'acme' : '',
      role,
      permissions: permissionsOf(role),
      memberships: [],
      edition: 'Open source',
      version: '1.0',
      revision: 'abc',
      unstableFeatures: []
    }
  }
}

const healthStatus: ClusterHealthStatus = {
  api: 'healthy',
  compiler: 'healthy',
  runner: 'healthy',
  stale: false,
  recordedAt: null
}

const openMenu = async (status: ClusterHealthStatus = healthStatus) => {
  await render(ProfileButton, { healthStatus: status })
  document.querySelector<HTMLButtonElement>('button:has(.fd-circle-user)')!.click()
  await expect.poll(() => document.body.textContent).toContain('Sign Out')
  return document.body.textContent ?? ''
}

describe('ProfileButton', () => {
  it('offers cluster health to the lowest role that resolves a tenant', async () => {
    state.data = session('read', 't-acme')
    expect(await openMenu()).toContain('Feldera Health')
  })

  // Nothing labels the health dot, so the colour it carries is the assertion.
  it('marks stale monitoring data as a warning rather than as healthy', async () => {
    state.data = session('read', 't-acme')
    await openMenu({ ...healthStatus, stale: true })
    expect(document.querySelector('.bg-success-500')).toBeNull()
    expect(document.querySelector('.bg-warning-500')).not.toBeNull()
  })

  it('keeps the recorded major issue red once the data goes stale', async () => {
    state.data = session('read', 't-acme')
    await openMenu({ ...healthStatus, stale: true, runner: 'major_issue' })
    expect(document.querySelector('.bg-error-500')).not.toBeNull()
    expect(document.querySelector('.bg-warning-500')).toBeNull()
  })

  it('hides cluster health when no session data resolved at all', async () => {
    state.data = { auth }
    expect(await openMenu()).not.toContain('Feldera Health')
  })

  it('hides cluster health when the session named no role', async () => {
    // What the server sends without an acting tenant: no role, so no permissions.
    state.data = session(null, '')
    const menu = await openMenu()
    expect(menu).not.toContain('Feldera Health')
    // The write-gated entries close through the same empty list.
    expect(menu).not.toContain('Admin Dashboard')
    expect(menu).not.toContain('Manage API keys')
  })

  it('still offers cluster health to an owner, so the gate is not rank-based', async () => {
    state.data = session('owner', 't-acme')
    expect(await openMenu()).toContain('Feldera Health')
  })
})
