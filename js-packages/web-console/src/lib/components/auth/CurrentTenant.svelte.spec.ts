// CurrentTenant drives from the session's membership list, plus the acting
// tenant when that lies outside the list: the acting tenant name always
// renders, the dropdown affordance appears only when there is more than one
// tenant to act in, and switching selects by tenant id.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

const felderaState = vi.hoisted(() => ({
  current: undefined as
    | {
        tenantId: string
        tenantName: string
        role: string
        memberships: { tenantId: string; name: string; role: string }[]
      }
    | undefined
}))
const switchTenant = vi.hoisted(() => vi.fn())

vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return felderaState.current
      }
    }
  }
}))
vi.mock('$lib/compositions/switchTenant', () => ({
  switchTenant: (...args: unknown[]) => switchTenant(...args)
}))

// Imported AFTER vi.mock so the mocks take effect.
import CurrentTenant from './CurrentTenant.svelte'

const selectElement = () => document.querySelector<HTMLSelectElement>('select')

describe('CurrentTenant', () => {
  afterEach(() => {
    felderaState.current = undefined
    switchTenant.mockReset()
  })

  it('renders nothing when there is no session data', async () => {
    await render(CurrentTenant)
    expect(selectElement()).toBeNull()
    expect(document.body.textContent).not.toContain('Tenant')
  })

  it('renders nothing when there is no tenant name and no choice to make', async () => {
    felderaState.current = { tenantId: '', tenantName: '', role: 'read', memberships: [] }
    await render(CurrentTenant)
    expect(document.body.textContent).not.toContain('Tenant')
  })

  it('shows the plain tenant name, not a dropdown, for a single membership', async () => {
    felderaState.current = {
      tenantId: 't-acme',
      tenantName: 'acme',
      role: 'admin',
      memberships: [{ tenantId: 't-acme', name: 'acme', role: 'admin' }]
    }
    await render(CurrentTenant)
    expect(selectElement()).toBeNull()
    expect(document.body.textContent).toContain('acme')
  })

  it('shows the plain acting tenant name for an owner with no memberships', async () => {
    felderaState.current = {
      tenantId: 't-x',
      tenantName: 'acted-as',
      role: 'owner',
      memberships: []
    }
    await render(CurrentTenant)
    expect(selectElement()).toBeNull()
    expect(document.body.textContent).toContain('acted-as')
  })

  it('offers every membership and switches by tenant id on change', async () => {
    felderaState.current = {
      tenantId: 't-acme',
      tenantName: 'acme',
      role: 'admin',
      memberships: [
        { tenantId: 't-acme', name: 'acme', role: 'admin' },
        { tenantId: 't-beta', name: 'beta', role: 'read' }
      ]
    }
    await render(CurrentTenant)
    const select = selectElement()!
    expect(select.value).toBe('t-acme')
    expect(Array.from(select.options).map((o) => o.value)).toEqual(['t-acme', 't-beta'])
    select.value = 't-beta'
    select.dispatchEvent(new Event('change', { bubbles: true }))
    await expect.poll(() => switchTenant.mock.calls).toEqual([['t-beta']])
  })

  it('displays an acting tenant outside the membership list (owner act-as)', async () => {
    felderaState.current = {
      tenantId: 't-elsewhere',
      tenantName: 'elsewhere',
      role: 'owner',
      memberships: [
        { tenantId: 't-acme', name: 'acme', role: 'admin' },
        { tenantId: 't-beta', name: 'beta', role: 'read' }
      ]
    }
    await render(CurrentTenant)
    const select = selectElement()!
    // The acting tenant heads the options so the select displays its name even
    // though it is no membership.
    expect(select.value).toBe('t-elsewhere')
    expect(Array.from(select.options).map((o) => o.textContent)).toEqual([
      'elsewhere',
      'acme',
      'beta'
    ])
  })

  it('offers a way back when acting outside a lone membership', async () => {
    felderaState.current = {
      tenantId: 't-elsewhere',
      tenantName: 'elsewhere',
      role: 'owner',
      memberships: [{ tenantId: 't-acme', name: 'acme', role: 'admin' }]
    }
    await render(CurrentTenant)
    const select = selectElement()!
    expect(select.value).toBe('t-elsewhere')
    expect(Array.from(select.options).map((o) => o.textContent)).toEqual(['elsewhere', 'acme'])
    select.value = 't-acme'
    select.dispatchEvent(new Event('change', { bubbles: true }))
    await expect.poll(() => switchTenant.mock.calls).toEqual([['t-acme']])
  })
})
