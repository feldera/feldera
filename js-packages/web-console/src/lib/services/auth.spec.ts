// Unit tests for the per-user tenant selection: the saved selection is keyed
// to the logged-in user's OIDC `sub`, is unreadable before a user is known,
// and never leaks between users sharing a browser.

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@axa-fr/oidc-client', () => ({ OidcClient: { get: vi.fn() } }))

import { getSelectedTenant, setSelectedTenant, setSelectedTenantUser } from './auth'

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
