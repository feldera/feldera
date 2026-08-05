// Losing the last membership mid-session is reported by whatever request
// happens to fail, so the global error interceptor is what notices it.

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@axa-fr/oidc-client', () => ({ OidcClient: { get: vi.fn() } }))

import { errorResponseMiddleware } from '$lib/services/auth'
import { errorCodeOf, tenantAccessLost } from './tenantAccess.svelte'

const rejection = (code: string) => new Error(code, { cause: { error_code: code } })

beforeEach(() => {
  tenantAccessLost.reset()
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

describe('tenant access lost', () => {
  it('starts clear', () => {
    expect(tenantAccessLost.current).toBe(false)
  })

  it('is marked when a response reports no memberships', () => {
    errorResponseMiddleware(rejection('NoTenantMemberships'), undefined)
    expect(tenantAccessLost.current).toBe(true)
  })

  it('ignores codes that a retry or a reload can still recover from', () => {
    for (const code of ['NotATenantMember', 'UnknownTenantName', 'AmbiguousTenantMembership']) {
      errorResponseMiddleware(rejection(code), undefined)
      expect(tenantAccessLost.current, code).toBe(false)
    }
  })

  it('ignores ordinary failures, including network errors with no response', () => {
    errorResponseMiddleware(new TypeError('Failed to fetch'), undefined)
    errorResponseMiddleware({ message: 'nope' }, { status: 500 } as Response)
    expect(tenantAccessLost.current).toBe(false)
  })

  it('still tags the error with status, as before', () => {
    const error: any = rejection('NoTenantMemberships')
    const out: any = errorResponseMiddleware(error, { status: 403 } as Response)
    expect(out.status).toBe(403)
    expect(tenantAccessLost.current).toBe(true)
  })

  it('clears once access is regained', () => {
    tenantAccessLost.mark()
    tenantAccessLost.reset()
    expect(tenantAccessLost.current).toBe(false)
  })
})
