// switchTenant persists the selection, drops the config caches, and restarts
// the app: on the home page by default (header switcher), or on the current
// URL with `reloadInPlace` (must-pick gate, preserving deep links).

import { beforeEach, describe, expect, it, vi } from 'vitest'

const setSelectedTenant = vi.hoisted(() => vi.fn())
const clearConfigCaches = vi.hoisted(() => vi.fn())

vi.mock('$lib/services/auth', () => ({
  setSelectedTenant: (...args: unknown[]) => setSelectedTenant(...args)
}))
vi.mock('$lib/compositions/configCache', () => ({
  clearConfigCaches: () => clearConfigCaches()
}))
vi.mock('$lib/functions/svelte', () => ({
  resolve: (path: string) => path
}))

import { switchTenant } from './switchTenant'

const location = { assign: vi.fn(), reload: vi.fn() }

beforeEach(() => {
  vi.stubGlobal('window', { location })
  setSelectedTenant.mockReset()
  clearConfigCaches.mockReset()
  location.assign.mockReset()
  location.reload.mockReset()
})

describe('switchTenant', () => {
  it('saves the selection, clears the caches, and restarts on the home page', () => {
    switchTenant('t-acme')
    expect(setSelectedTenant.mock.calls).toEqual([['t-acme']])
    expect(clearConfigCaches).toHaveBeenCalledOnce()
    expect(location.assign.mock.calls).toEqual([['/']])
    expect(location.reload).not.toHaveBeenCalled()
  })

  it('reloads the current URL with reloadInPlace, preserving a deep link', () => {
    switchTenant('t-acme', { reloadInPlace: true })
    expect(setSelectedTenant.mock.calls).toEqual([['t-acme']])
    expect(clearConfigCaches).toHaveBeenCalledOnce()
    expect(location.reload).toHaveBeenCalledOnce()
    expect(location.assign).not.toHaveBeenCalled()
  })
})
