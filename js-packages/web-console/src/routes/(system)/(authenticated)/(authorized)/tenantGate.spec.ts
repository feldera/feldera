/**
 * The gate that keeps this group from loading or rendering without an acting
 * tenant. It has to be a redirect rather than a branch in the layout component:
 * a component-level branch still lets nested `load` functions run, and several
 * of them fetch tenant-scoped resources (a pipeline preload, the demo list) or
 * redirect on a permission check that cannot pass without a tenant.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

const stashRedirectTarget = vi.hoisted(() => vi.fn())

vi.mock('$lib/services/redirectTarget', () => ({
  stashRedirectTarget: (href: string) => stashRedirectTarget(href)
}))
vi.mock('$lib/functions/svelte', () => ({ resolve: (path: string) => path }))

import { load } from './+layout'

const run = (data: Record<string, unknown>, href = 'http://localhost/pipelines/foo/') =>
  (load as any)({ parent: async () => data, url: new URL(href) })

beforeEach(() => {
  stashRedirectTarget.mockReset()
})

describe('(authorized) gate', () => {
  it('lets the group load when a tenant is resolved', async () => {
    await expect(run({ feldera: { tenantId: 't-acme' } })).resolves.toEqual({})
    expect(stashRedirectTarget).not.toHaveBeenCalled()
  })

  it('redirects to the tenant page when none is resolved', async () => {
    // A thrown redirect is what stops the sibling loaders; returning early would
    // let them run.
    const thrown: any = await run({ unresolvedTenant: { memberships: [] } }).catch((e: any) => e)
    expect(thrown.status).toBe(307)
    expect(thrown.location).toBe('/select-tenant/')
  })

  it('stashes the page asked for, so picking a tenant returns to it', async () => {
    await run({ unresolvedTenant: { memberships: [] } }).catch(() => {})
    expect(stashRedirectTarget.mock.calls).toEqual([['http://localhost/pipelines/foo/']])
  })
})
