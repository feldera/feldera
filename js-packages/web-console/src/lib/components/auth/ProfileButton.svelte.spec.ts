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
import { statusToneColors } from 'common-ui/statusTone'
import { beforeAll, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { ClusterHealthStatus } from '#lib/compositions/health/useClusterHealth.svelte.ts'
import { permissionsOf, roleOf } from '#lib/services/rbac.ts'
// The app loads the icon font in `+layout.svelte`; the screenshots need it too.
import '#assets/icons/fd.css'
import '#assets/fonts/generic-icons.css'

// Hoisted so the `$app/state` factory below can close over it: each test sets
// `state.data` to the session shape it is about.
const state = vi.hoisted(() => ({
  url: new URL('http://localhost/'),
  data: {} as Record<string, unknown>
}))
vi.mock('$app/state', () => ({ page: state }))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), refreshAll: vi.fn() }))
vi.mock('#lib/compositions/switchTenant.ts', () => ({ switchTenant: vi.fn() }))
// Dialog bodies the menu can open; they fetch on import.
vi.mock('#lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))

// Imported AFTER vi.mock so the mocks take effect.
import ProfileButton from './ProfileButton.svelte'

/** Finds an element with the health indicator color of a status tone. */
const indicatorOf = (tone: keyof typeof statusToneColors) =>
  document.querySelector(`.${statusToneColors[tone].healthIndicator}`)

const auth = {
  logout: vi.fn(),
  profile: { name: 'Ada', email: 'ada@example.com' },
  userInfo: {},
  accessToken: ''
}

// Takes the session payload's `role` as the server spells it, and derives the
// rest the way `buildFelderaData` does. The server sends `role` and the acting
// tenant fields together or not at all.
const session = (sessionRole: string | null, tenantId: string, tenantName = 'acme') => {
  const role = roleOf(sessionRole)
  return {
    auth,
    feldera: {
      tenantId,
      tenantName: tenantId ? tenantName : '',
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
  recordedAt: new Date('2026-05-01T12:00:00Z')
}

const openMenu = async (status: ClusterHealthStatus = healthStatus) => {
  await render(ProfileButton, { healthStatus: status })
  document.querySelector<HTMLButtonElement>('button:has(.fd-circle-user)')!.click()
  await expect.poll(() => document.body.textContent).toContain('Sign Out')
  return document.body.textContent ?? ''
}

describe('ProfileButton', () => {
  it('gives no verdict on cluster health before a poll has answered', async () => {
    // Only the app shell polls; a page outside it, like the profile viewer, must not show the
    // cluster as healthy when it is not, and could not know either way.
    state.data = session('read', 't-acme')
    await render(ProfileButton, { healthStatus: undefined })
    document.querySelector<HTMLButtonElement>('button:has(.fd-circle-user)')!.click()
    await expect.poll(() => document.body.textContent).toContain('Feldera Health')
    const dot = document.querySelector('.h-2\\.5.rounded-full')!
    for (const colors of Object.values(statusToneColors)) {
      expect(dot.className).not.toContain(colors.healthIndicator)
    }
  })

  it('offers cluster health to the lowest role that resolves a tenant', async () => {
    state.data = session('read', 't-acme')
    expect(await openMenu()).toContain('Feldera Health')
  })

  // Nothing labels the health dot, so the colour it carries is the assertion.
  it('marks stale monitoring data as a warning rather than as healthy', async () => {
    state.data = session('read', 't-acme')
    await openMenu({ ...healthStatus, stale: true })
    expect(indicatorOf('jade')).toBeNull()
    expect(indicatorOf('warning')).not.toBeNull()
  })

  it('keeps the recorded major issue red once the data goes stale', async () => {
    state.data = session('read', 't-acme')
    await openMenu({ ...healthStatus, stale: true, runner: 'major_issue' })
    expect(indicatorOf('error')).not.toBeNull()
    expect(indicatorOf('warning')).toBeNull()
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

  it('shows a pending sign-out and ignores clicks until the provider page opens', async () => {
    state.data = session('read', 't-acme')
    auth.logout.mockReset().mockReturnValue(new Promise(() => {}))
    await openMenu()
    const signOut = document.querySelector<HTMLButtonElement>('[data-testid="btn-sign-out"]')!

    signOut.click()
    await expect.poll(() => signOut.textContent).toContain('Signing out…')
    expect(signOut.disabled).toBe(true)
    signOut.click()
    expect(auth.logout).toHaveBeenCalledTimes(1)
  })

  it('restores the sign-out button when sign-out fails', async () => {
    state.data = session('read', 't-acme')
    const failure = new Error('sign-out failed')
    auth.logout.mockReset().mockRejectedValue(failure)
    // The button rethrows the failure, so it reaches the global error handler.
    const unhandled = new Promise<unknown>((resolve) =>
      window.addEventListener(
        'unhandledrejection',
        (event) => {
          event.preventDefault()
          resolve(event.reason)
        },
        { once: true }
      )
    )
    await openMenu()
    const signOut = document.querySelector<HTMLButtonElement>('[data-testid="btn-sign-out"]')!

    signOut.click()
    expect(await unhandled).toBe(failure)
    await expect.poll(() => signOut.textContent).toContain('Sign Out')
    expect(signOut.disabled).toBe(false)
  })

  describe('screenshots', () => {
    // The browser is slow while the other test files run. Thus, the screenshot can take longer.
    const screenshotOptions = { timeout: 15_000 }
    // Makes each screenshot stable:
    // - A desktop viewport is wide enough for the `xl:` breakpoint of the app header.
    // - The theme is the one of `src/app.html`.
    beforeAll(async () => {
      await page.viewport(1280, 720)
      document.documentElement.dataset.theme = 'feldera-modern-theme'
      return () => {
        delete document.documentElement.dataset.theme
      }
    })

    /** Renders the logged-in button as the app header does, and returns its locator. */
    const renderButton = async (tenantName: string) => {
      state.data = session('read', 't-acme', tenantName)
      await render(ProfileButton, { compactBreakpoint: 'xl:', healthStatus })
      return page.elementLocator(document.querySelector('button:has(.fd-circle-user)')!)
    }

    it('shows a short tenant name in full', async () => {
      const button = await renderButton('acme')
      await expect.element(button).toMatchScreenshot('tenant-name-short', screenshotOptions)
    })

    it('truncates a long tenant name', async () => {
      const button = await renderButton('feldera-engineering-analytics-production-europe-west')
      await expect.element(button).toMatchScreenshot('tenant-name-long', screenshotOptions)
    })

    // A per-user personal tenant is named after the OIDC `sub`, a UUID on AWS Cognito.
    it('truncates a UUID tenant name', async () => {
      const button = await renderButton('3f2c9a6e-8b1d-4c57-9e0a-2d7f1b6c4e93')
      await expect.element(button).toMatchScreenshot('tenant-name-uuid', screenshotOptions)
    })
  })
})
