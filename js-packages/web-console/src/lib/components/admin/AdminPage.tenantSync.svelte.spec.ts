/**
 * The admin page's tenant store is shared with the TenantList it renders, so a
 * tenant created (or deleted) in the list reaches the header's picker without a
 * reload. This mounts the real TenantList, unlike AdminPage.svelte.spec.ts,
 * which stubs it to test the header gate alone.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

const tenantsState = vi.hoisted(() => ({
  list: [{ id: 't-acme', name: 'acme', initial_provider: 'oidc' }] as any[]
}))

vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return {
          role: 'owner',
          permissions: permissionsOf('owner'),
          tenantId: 't-acme',
          tenantName: 'acme'
        }
      }
    }
  }
}))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), invalidateAll: vi.fn(async () => {}) }))
vi.mock('$lib/compositions/configCache', () => ({ clearConfigCaches: vi.fn() }))
vi.mock('$lib/services/auth', () => ({ setSelectedTenant: vi.fn() }))
// Only the members table is stubbed: it fetches on mount and has no part in
// what the picker offers.
vi.mock('$lib/components/admin/UserRoleTable.svelte', () => ({ default: () => {} }))
vi.mock('$lib/services/pipelineManager', () => ({
  getTenants: vi.fn(async () => [...tenantsState.list]),
  getAuthConfig: vi.fn(async () => undefined),
  getConfiguredOwners: vi.fn(async () => undefined),
  createTenant: vi.fn(async (name: string) => {
    tenantsState.list.push({ id: `t-${name}`, name, initial_provider: 'oidc' })
  }),
  deleteTenant: vi.fn(async (id: string) => {
    tenantsState.list = tenantsState.list.filter((t) => t.id !== id)
  }),
  renameTenant: vi.fn(async () => ({}))
}))

// Imported AFTER vi.mock so the mocks take effect.
import AdminPage from './AdminPage.svelte'

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

const headingText = () =>
  Array.from(document.querySelectorAll('h2')).map((h) => h.textContent ?? '')
const usersHeading = () => headingText().find((t) => t.includes('Users & roles')) ?? ''
const optionLabels = () =>
  Array.from(document.querySelectorAll('[role="option"]')).map((e) => e.textContent?.trim() ?? '')

describe('AdminPage — tenant list and picker share one store', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    tenantsState.list = [{ id: 't-acme', name: 'acme', initial_provider: 'oidc' }]
    vi.clearAllMocks()
  })

  it('offers a newly created tenant in the picker without a reload', async () => {
    mountTarget = document.createElement('div')
    document.body.appendChild(mountTarget)
    mounted = render(AdminPage, { target: mountTarget }) as any

    // Only the acting tenant exists, so there is nothing to switch to yet.
    await expect.poll(() => document.body.textContent).toContain('t-acme')
    expect(usersHeading()).not.toContain('select a different tenant')

    await page.getByPlaceholder('acme-prod').fill('beta')
    await page.getByRole('button', { name: 'Create' }).click()

    // With a store per component the picker never learns about 'beta' and this
    // is where the test fails.
    await expect.poll(usersHeading).toContain('select a different tenant')
    await page.getByText('select a different tenant').click()
    await expect.poll(optionLabels).toEqual(['beta'])
  })
})
