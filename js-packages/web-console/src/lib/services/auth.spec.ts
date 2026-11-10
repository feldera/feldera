// Unit tests for the per-user tenant selection: the saved selection is keyed
// to the logged-in user's OIDC `sub`, is unreadable before a user is known,
// and never leaks between users sharing a browser.

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@axa-fr/oidc-client', () => ({ OidcClient: { get: vi.fn() } }))

import {
  getSelectedTenant,
  isOidcCallbackPage,
  leaveOidcCallback,
  setSelectedTenant,
  setSelectedTenantUser
} from './auth'

class MemoryStorage {
  private store = new Map<string, string>()
  getItem = (k: string) => (this.store.has(k) ? (this.store.get(k) as string) : null)
  setItem = (k: string, v: string) => {
    this.store.set(k, String(v))
  }
  removeItem = (k: string) => {
    this.store.delete(k)
  }
}

let storage: MemoryStorage

beforeEach(() => {
  storage = new MemoryStorage()
  vi.stubGlobal('window', { localStorage: storage })
})

describe('per-user tenant selection', () => {
  // Must run first: it relies on the module's pristine no-user-known state,
  // which later tests replace by naming a user.
  it('exposes no selection and persists nothing before a user is known', () => {
    expect(getSelectedTenant()).toBeUndefined()
    setSelectedTenant('acme')
    expect(getSelectedTenant()).toBeUndefined()
  })

  it('persists under a key derived from the user sub', () => {
    setSelectedTenantUser('user-a')
    setSelectedTenant('acme')
    expect(storage.getItem('session/selected_tenant/user-a')).toBe('acme')
    expect(getSelectedTenant()).toBe('acme')
  })

  it('loads the saved selection when the user becomes known', () => {
    storage.setItem('session/selected_tenant/user-a', 'acme')
    setSelectedTenantUser('user-a')
    expect(getSelectedTenant()).toBe('acme')
  })

  it('does not carry one user selection over to another', () => {
    setSelectedTenantUser('user-a')
    setSelectedTenant('acme')
    setSelectedTenantUser('user-b')
    expect(getSelectedTenant()).toBeUndefined()
  })

  it('clears the saved selection when set to undefined', () => {
    setSelectedTenantUser('user-a')
    setSelectedTenant('acme')
    setSelectedTenant(undefined)
    expect(getSelectedTenant()).toBeUndefined()
    expect(storage.getItem('session/selected_tenant/user-a')).toBeNull()
  })

  // Logout does not clear the selection (see onBeforeLogout in +layout.ts):
  // the key is per user, so a returning user resumes their tenant.
  it('restores the same user selection across a logout/login cycle', () => {
    setSelectedTenantUser('user-a')
    setSelectedTenant('acme')
    setSelectedTenantUser('user-a')
    expect(getSelectedTenant()).toBe('acme')
  })

  it('drops the legacy user-agnostic key so it cannot leak into a login', () => {
    storage.setItem('session/selected_tenant', 'stale')
    setSelectedTenantUser('user-a')
    expect(storage.getItem('session/selected_tenant')).toBeNull()
    expect(getSelectedTenant()).toBeUndefined()
  })
})

// A browser with only the `location` and `history` parts that the OIDC
// callback helpers use. Records each navigation with the URL it leaves, which
// the next page gets as `document.referrer`.
const stubBrowserAt = (href: string) => {
  const navigations: { from: string; to: string }[] = []
  const location = {
    href,
    get pathname() {
      return new URL(location.href).pathname
    },
    replace: (to: string) => {
      navigations.push({ from: location.href, to })
    }
  }
  const history = {
    replaceState: (_data: unknown, _unused: string, url: string) => {
      location.href = new URL(url, location.href).href
    }
  }
  vi.stubGlobal('window', { location, history })
  return navigations
}

describe('leaveOidcCallback', () => {
  it.each([
    ['code and state', '?code=secret-code&state=secret-state'],
    ['code, state and session_state', '?code=c&state=s&session_state=ss'],
    ['an access token in the fragment', '#access_token=secret-token'],
    ['a query and a fragment', '?code=c&state=s#access_token=t'],
    ['nothing', '']
  ])('leaves a callback URL with %s without the query and fragment', (_, suffix) => {
    const navigations = stubBrowserAt(`https://console.example.com/auth/callback/${suffix}`)

    leaveOidcCallback()

    expect(navigations).toEqual([{ from: 'https://console.example.com/auth/callback/', to: '/' }])
  })
})

describe('isOidcCallbackPage', () => {
  it.each([
    ['https://console.example.com/auth/callback/?code=c&state=s', true],
    ['https://console.example.com/auth/callback/', true],
    ['https://console.example.com/', false],
    ['https://console.example.com/auth/callback/extra/', false],
    ['https://console.example.com/pipelines/?next=/auth/callback/', false]
  ])('on %s is %s', (href, expected) => {
    stubBrowserAt(href)
    expect(isOidcCallbackPage()).toBe(expected)
  })
})
