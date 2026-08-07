/**
 * The tenant page sits outside the `(authorized)` group so it stays reachable
 * while the gate is closed. Reaching it with a tenant already resolved means
 * there is nothing to pick, so it hands the user back to whatever the gate
 * interrupted.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

const takeRedirectTarget = vi.hoisted(() => vi.fn())

vi.mock('$lib/services/redirectTarget', () => ({
  takeRedirectTarget: () => takeRedirectTarget()
}))
vi.mock('$lib/functions/svelte', () => ({ resolve: (path: string) => path }))

import { load } from './+page'

const run = (data: Record<string, unknown>) => (load as any)({ parent: async () => data })

beforeEach(() => {
  takeRedirectTarget.mockReset()
})

describe('/select-tenant', () => {
  it('hands the memberships to the picker while none is resolved', async () => {
    const memberships = [{ tenantId: 't-acme', name: 'acme', role: 'admin' }]
    await expect(run({ unresolvedTenant: { memberships } })).resolves.toEqual({ memberships })
    expect(takeRedirectTarget).not.toHaveBeenCalled()
  })

  it('renders the no-access case as an empty list, not a redirect', async () => {
    await expect(run({ unresolvedTenant: { memberships: [] } })).resolves.toEqual({
      memberships: []
    })
  })

  it('returns to the interrupted page when a tenant is already resolved', async () => {
    takeRedirectTarget.mockReturnValue('http://localhost/pipelines/foo/')
    const thrown: any = await run({ feldera: { tenantId: 't-acme' } }).catch((e: any) => e)
    expect(thrown.status).toBe(307)
    expect(thrown.location).toBe('http://localhost/pipelines/foo/')
  })

  it('goes home when a tenant is resolved and nothing was interrupted', async () => {
    takeRedirectTarget.mockReturnValue(undefined)
    const thrown: any = await run({ feldera: { tenantId: 't-acme' } }).catch((e: any) => e)
    expect(thrown.location).toBe('/')
  })
})
