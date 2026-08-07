// Losing the last membership mid-session is reported by whatever request
// happens to fail, so the global error interceptor is what notices it. All it
// can do is re-run the loaders: the root `load()` then resolves no acting
// tenant and the `(authorized)` gate redirects to the tenant page.

import { beforeEach, describe, expect, it, vi } from 'vitest'

const invalidateAll = vi.hoisted(() => vi.fn())
vi.mock('@axa-fr/oidc-client', () => ({ OidcClient: { get: vi.fn() } }))
vi.mock('$app/navigation', () => ({ invalidateAll: () => invalidateAll() }))

import { errorResponseMiddleware } from '$lib/services/auth'
import { errorCodeOf, isTenantRecheckPending, resetTenantRecheck } from './tenantAccess'

const rejection = (code: string) => new Error(code, { cause: { error_code: code } })

beforeEach(() => {
  resetTenantRecheck()
  invalidateAll.mockReset()
})

describe('errorCodeOf', () => {
  it('reads the code from the cause an SDK rejection carries', () => {
    expect(errorCodeOf(rejection('NoTenantMemberships'))).toBe('NoTenantMemberships')
  })

  it('reads a code sitting directly on the error body', () => {
    expect(errorCodeOf({ error_code: 'NotATenantMember' })).toBe('NotATenantMember')
  })

  it('is undefined for anything else', () => {
    expect(errorCodeOf(new Error('boom'))).toBeUndefined()
    expect(errorCodeOf(undefined)).toBeUndefined()
  })
})

describe('tenant re-check', () => {
  it('starts unarmed', () => {
    expect(isTenantRecheckPending()).toBe(false)
  })

  it('re-runs the loaders when a response reports no memberships', () => {
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    expect(invalidateAll).toHaveBeenCalledOnce()
    expect(isTenantRecheckPending()).toBe(true)
  })

  it('re-runs them once however many requests report it', () => {
    // The re-run itself fetches /config, which fails the same way while no
    // tenant resolves; without the latch that is an endless loop.
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    expect(invalidateAll).toHaveBeenCalledOnce()
  })

  it('ignores codes that a retry or a reload can still recover from', () => {
    for (const code of ['NotATenantMember', 'UnknownTenantName', 'AmbiguousTenantMembership']) {
      errorResponseMiddleware(rejection(code), undefined)
      expect(isTenantRecheckPending(), code).toBe(false)
    }
    expect(invalidateAll).not.toHaveBeenCalled()
  })

  it('ignores ordinary failures, including network errors with no response', () => {
    errorResponseMiddleware(new TypeError('Failed to fetch'), undefined)
    errorResponseMiddleware({ message: 'nope' }, { status: 500 } as Response)
    expect(isTenantRecheckPending()).toBe(false)
    expect(invalidateAll).not.toHaveBeenCalled()
  })

  it('still tags the error with status, as before', () => {
    const error: any = rejection('NoTenantMemberships')
    const out: any = errorResponseMiddleware(error, { status: 403 } as Response)
    expect(out.status).toBe(403)
    expect(isTenantRecheckPending()).toBe(true)
  })

  it('re-arms once access is regained, so a later revocation is noticed', () => {
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    resetTenantRecheck()
    expect(isTenantRecheckPending()).toBe(false)
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    expect(invalidateAll).toHaveBeenCalledTimes(2)
  })
})
