/**
 * Gating and behavior tests for the admin page header's tenant picker. The
 * "Users & roles" header lets an owner switch which tenant's members show
 * below; the trigger (the tenant name plus a "select a different tenant" hint)
 * is owner-only (`write:tenant`). A non-owner sees the tenant name as plain
 * text; an owner with no other tenant sees the trigger inert, so an empty list
 * cannot open. The picker omits the tenant already shown and closes on
 * selection.
 *
 * The child tables (UserRoleTable, TenantList) are stubbed out: they mount
 * monaco-backed dialogs and fetch on mount, none of which the header gate
 * touches. That keeps this test to AdminPage's own markup.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '#lib/services/rbac.ts'

type Role = 'read' | 'write' | 'admin' | 'owner'
const roleState = vi.hoisted(() => ({ current: 'owner' as Role }))
const current = vi.hoisted(() => ({ id: '', name: 'acme-tenant' }))
const tenantsState = vi.hoisted(() => ({ list: [] as { id: string; name: string }[] }))
const ownersState = vi.hoisted(() => ({
  configured: undefined as
    | {
        owners: string[]
        owner_trusts: { issuer: string; subject: string; audience?: string | null }[]
      }
    | undefined
}))

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
vi.mock('#lib/components/admin/UserRoleTable.svelte', () => ({ default: () => {} }))
vi.mock('#lib/components/admin/TenantList.svelte', () => ({ default: () => {} }))
vi.mock('#lib/services/pipelineManager.ts', () => ({
  getTenants: vi.fn(async () => tenantsState.list),
  getConfiguredOwners: vi.fn(async () => ownersState.configured)
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
const triggerButton = () =>
  Array.from(document.querySelectorAll<HTMLButtonElement>('h2 button')).find((b) =>
    b.textContent?.includes(current.name)
  )

describe('AdminPage - write:tenant header picker', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    roleState.current = 'owner'
    current.id = ''
    current.name = 'acme-tenant'
    tenantsState.list = []
    ownersState.configured = undefined
    vi.clearAllMocks()
  })

  it('offers the tenant picker to an owner with another tenant to switch to', async () => {
    current.id = 't-acme'
    current.name = 'acme'
    tenantsState.list = [
      { id: 't-acme', name: 'acme' },
      { id: 't-beta', name: 'beta' }
    ]
    mountPage('owner')
    await expect.poll(usersHeading).toContain('acme')
    await expect.poll(usersHeading).toContain('select a different tenant')
    await expect.poll(() => triggerButton()?.disabled).toBe(false)
  })

  it('shows the tenant name as plain text to a non-owner admin', () => {
    mountPage('admin')
    // Reverting the gate (rendering the picker unconditionally) surfaces the
    // hint here and fails this test.
    expect(usersHeading()).toContain('acme-tenant')
    expect(usersHeading()).not.toContain('select a different tenant')
    expect(triggerButton()).toBeUndefined()
  })

  it('shows the tenant name as plain text to an owner of a single tenant', async () => {
    current.id = 't-acme'
    current.name = 'acme'
    tenantsState.list = [{ id: 't-acme', name: 'acme' }]
    mountPage('owner')
    await expect.poll(usersHeading).toContain('acme')
    // Nothing to switch to, so no trigger and no way to open an empty list.
    // Dropping `canSwitchTenant` from the gate surfaces the hint and fails here.
    expect(usersHeading()).not.toContain('select a different tenant')
    expect(triggerButton()).toBeUndefined()
    expect(optionLabels()).toEqual([])
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

describe('AdminPage - platform owners', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    ownersState.configured = undefined
    vi.clearAllMocks()
  })

  const ownersSectionText = () =>
    Array.from(document.querySelectorAll('section')).find((s) =>
      s.querySelector('h2')?.textContent?.includes('Platform owners')
    )?.textContent ?? ''

  it('renders trusts that differ only by audience, and repeated entries once', async () => {
    const trust = (audience: string) => ({
      issuer: 'https://token.actions.githubusercontent.com',
      subject: 'repo:feldera/feldera:*',
      audience
    })
    ownersState.configured = {
      owners: ['ops@acme.test', 'ops@acme.test'],
      owner_trusts: [
        trust('https://github.com/feldera'),
        trust('cleanup-job'),
        trust('cleanup-job')
      ]
    }
    mountPage('owner')
    await expect.poll(ownersSectionText).toContain('cleanup-job')
    const text = ownersSectionText()
    expect(text).toContain('https://github.com/feldera')
    expect(text.split('ops@acme.test').length - 1).toBe(1)
    expect(text.split('cleanup-job').length - 1).toBe(1)
  })
})
