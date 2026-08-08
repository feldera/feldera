// TenantPicker is the gate shown when a login resolves no acting tenant: with
// memberships it lists them for a one-click selection, without any it explains
// that an administrator must grant access first.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const switchTenant = vi.hoisted(() => vi.fn())
const takeRedirectTarget = vi.hoisted(() => vi.fn())
vi.mock('$lib/compositions/switchTenant', () => ({
  switchTenant: (...args: unknown[]) => switchTenant(...args)
}))
vi.mock('$lib/services/redirectTarget', () => ({
  takeRedirectTarget: () => takeRedirectTarget()
}))

// Imported AFTER vi.mock so the mock takes effect.
import TenantPicker from './TenantPicker.svelte'

const memberships = [
  { tenantId: 't-acme', name: 'acme', role: 'admin' },
  { tenantId: 't-beta', name: 'beta', role: 'read' }
]

describe('TenantPicker', () => {
  afterEach(() => {
    switchTenant.mockReset()
    takeRedirectTarget.mockReset()
  })

  it('lists every membership with its name and role', async () => {
    await render(TenantPicker, { memberships })
    await expect.element(page.getByText('Choose a tenant')).toBeInTheDocument()
    await expect.element(page.getByText('acme', { exact: true })).toBeInTheDocument()
    await expect.element(page.getByText('admin', { exact: true })).toBeInTheDocument()
    await expect.element(page.getByText('beta', { exact: true })).toBeInTheDocument()
    await expect.element(page.getByText('read', { exact: true })).toBeInTheDocument()
  })

  it('selects a tenant by id and restarts on the page the gate interrupted', async () => {
    takeRedirectTarget.mockReturnValue('http://localhost/pipelines/foo/')
    await render(TenantPicker, { memberships })
    await page.getByRole('button', { name: /beta/ }).click()
    expect(switchTenant.mock.calls).toEqual([['t-beta', { to: 'http://localhost/pipelines/foo/' }]])
  })

  it('leaves the restart target unset when nothing was interrupted', async () => {
    takeRedirectTarget.mockReturnValue(undefined)
    await render(TenantPicker, { memberships })
    await page.getByRole('button', { name: /beta/ }).click()
    expect(switchTenant.mock.calls).toEqual([['t-beta', { to: undefined }]])
  })

  it('shows the no-access notice when there are no memberships', async () => {
    await render(TenantPicker, { memberships: [] })
    await expect
      .element(page.getByRole('heading', { name: 'No tenant access' }))
      .toBeInTheDocument()
    await expect.element(page.getByText(/Ask an administrator/)).toBeInTheDocument()
    expect(switchTenant).not.toHaveBeenCalled()
  })
})
