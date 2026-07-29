/**
 * Gating and behavior tests for the admin page header's tenant picker. The
 * "Users & roles" header lets an owner switch which tenant's members show
 * below; the trigger (the tenant name plus a "select a different tenant" hint)
 * is owner-only (`write:tenant`). A non-owner sees the tenant name as plain
 * text. The picker omits the tenant already shown and closes on selection.
 *
 * The child tables (UserRoleTable, TenantList) are stubbed out: they mount
 * monaco-backed dialogs and fetch on mount, none of which the header gate
 * touches. That keeps this test to AdminPage's own markup.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

type Role = 'read' | 'write' | 'admin' | 'owner'
const roleState = vi.hoisted(() => ({ current: 'owner' as Role }))
const current = vi.hoisted(() => ({ id: '', name: 'acme-tenant' }))
const tenantsState = vi.hoisted(() => ({ list: [] as { id: string; name: string }[] }))

vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return {
          role: roleState.current,
          permissions: permissionsOf(roleState.current),
          tenantId: current.id,
          tenantName: current.name
        }
      }
    }
  }
}))
// Stub the heavy child tables to empty components.
vi.mock('$lib/components/admin/UserRoleTable.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/admin/TenantList.svelte', () => ({ default: () => {} }))
vi.mock('$lib/services/pipelineManager', () => ({
  getTenants: vi.fn(async () => tenantsState.list),
  getConfiguredOwners: vi.fn(async () => undefined)
}))

// Imported AFTER vi.mock so the mocks take effect.
import AdminPage from './AdminPage.svelte'

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

function mountPage(role: Role) {
  roleState.current = role
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  mounted = render(AdminPage, { target: mountTarget }) as any
}

const headingText = () =>
  Array.from(document.querySelectorAll('h2')).map((h) => h.textContent ?? '')
const usersHeading = () => headingText().find((t) => t.includes('Users & roles')) ?? ''
const optionLabels = () =>
  Array.from(document.querySelectorAll('[role="option"]')).map((e) => e.textContent?.trim() ?? '')

describe('AdminPage — write:tenant header picker', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    roleState.current = 'owner'
    current.id = ''
    current.name = 'acme-tenant'
    tenantsState.list = []
    vi.clearAllMocks()
  })

  it('offers the tenant picker to an owner', () => {
    mountPage('owner')
    expect(usersHeading()).toContain('acme-tenant')
    expect(usersHeading()).toContain('select a different tenant')
  })

  it('shows the tenant name as plain text to a non-owner admin', () => {
    mountPage('admin')
    // Reverting the gate (rendering the picker unconditionally) surfaces the
    // hint here and fails this test.
    expect(usersHeading()).toContain('acme-tenant')
    expect(usersHeading()).not.toContain('select a different tenant')
  })

  it('lists the other tenants and omits the one already shown', async () => {
    current.id = 't-acme'
    current.name = 'acme'
    tenantsState.list = [
      { id: 't-acme', name: 'acme' },
      { id: 't-beta', name: 'beta' },
      { id: 't-gamma', name: 'gamma' }
    ]
    mountPage('owner')
    await page.getByText('select a different tenant').click()
    // Dropping the `t.id !== adminTenant` filter re-adds "acme" and fails this.
    await expect.poll(optionLabels).toEqual(['beta', 'gamma'])
  })

  it('switches the shown tenant and closes on selection', async () => {
    current.id = 't-acme'
    current.name = 'acme'
    tenantsState.list = [
      { id: 't-acme', name: 'acme' },
      { id: 't-beta', name: 'beta' }
    ]
    mountPage('owner')
    await page.getByText('select a different tenant').click()
    await expect.poll(optionLabels).toEqual(['beta'])
    await page.getByRole('option', { name: 'beta' }).click()
    // Header now reflects the new tenant, and the list is gone (auto-closed).
    await expect.poll(usersHeading).toContain('beta')
    await expect.poll(optionLabels).toEqual([])
  })
})
