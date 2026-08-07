/**
 * Tenants are renamed in place with the same DoubleClickInput used for pipeline
 * names: the edit affordance opens an input where the name was, Enter commits.
 * These tests drive that path, the DuplicateName recovery ("Take the name")
 * which retries the rename with displace_existing, and deletion through the
 * shared DeleteDialog.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const tenantsState = vi.hoisted(() => ({
  list: [{ id: 't1', name: 'acme', initial_provider: 'oidc' }] as any[]
}))
const renameTenant = vi.hoisted(() => vi.fn())
const deleteTenant = vi.hoisted(() => vi.fn())

vi.mock('$app/state', () => ({ page: { data: { feldera: { tenantId: 't-other' } } } }))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), invalidateAll: vi.fn(async () => {}) }))
vi.mock('$lib/compositions/configCache', () => ({ clearConfigCaches: vi.fn() }))
vi.mock('$lib/services/auth', () => ({ setSelectedTenant: vi.fn() }))
vi.mock('$lib/services/pipelineManager', () => ({
  getTenants: vi.fn(async () => tenantsState.list),
  getAuthConfig: vi.fn(async () => undefined),
  createTenant: vi.fn(),
  deleteTenant: (...args: unknown[]) => deleteTenant(...args),
  renameTenant: (...args: unknown[]) => renameTenant(...args)
}))

// Imported AFTER vi.mock so the mocks take effect.
import { asyncReadable } from '@square/svelte-store'
import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import { getTenants, type Tenant } from '$lib/services/pipelineManager'
import TenantList from './TenantList.svelte'

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

// The tenant store is owned by AdminPage and passed in, so stand one up here.
function mountList() {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  const tenants = asyncReadable<Tenant[]>([], getTenants, { reloadable: true })
  mounted = render(TenantList, { target: mountTarget, props: { tenants } }) as any
}

// The rename input is the DoubleClickInput's, distinct from the create-tenant
// input at the bottom (which starts empty).
const renameInput = () =>
  Array.from(document.querySelectorAll<HTMLInputElement>('input')).find((i) => i.value === 'acme')

async function openRenameAndCommit(newName: string) {
  // Double-click the name to open the editor. The pencil button opens it too,
  // but its icon-font glyph has no box in the headless browser, so a click
  // there never lands.
  await page.getByText('acme', { exact: true }).dblClick()
  await expect.poll(() => renameInput()).toBeTruthy()
  const input = renameInput()!
  input.value = newName
  input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
}

let modal: { unmount: () => Promise<void> } | undefined
let modalTarget: HTMLDivElement | undefined

// Mount the shared modal host with whatever the row's trash button opened, so
// the DeleteDialog and its confirm button render for the test to drive.
function mountOpenDialog() {
  modalTarget = document.createElement('div')
  document.body.appendChild(modalTarget)
  modal = render(GlobalModal, {
    target: modalTarget,
    props: { dialog: useGlobalDialog().dialog }
  }) as any
}

describe('TenantList — in-place rename', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    await modal?.unmount()
    modal = undefined
    modalTarget?.remove()
    modalTarget = undefined
    useGlobalDialog().dialog = null
    renameTenant.mockReset()
    deleteTenant.mockReset()
    vi.clearAllMocks()
  })

  it('commits a rename through the DoubleClickInput', async () => {
    renameTenant.mockResolvedValue({})
    mountList()
    await expect.poll(() => document.body.textContent).toContain('acme')
    await openRenameAndCommit('acme-2')
    // Removing the onvalue wiring (or the component) fails this assertion.
    await expect.poll(() => renameTenant.mock.calls).toContainEqual(['t1', 'acme-2', false])
  })

  it('offers to take the name on a DuplicateName conflict', async () => {
    renameTenant.mockRejectedValueOnce(new Error('a tenant with this name already exists'))
    renameTenant.mockResolvedValueOnce({})
    mountList()
    await expect.poll(() => document.body.textContent).toContain('acme')
    await openRenameAndCommit('taken')
    // The conflict surfaces the recovery affordance...
    await page.getByRole('button', { name: 'Take the name' }).click()
    // ...which retries with displace_existing = true.
    await expect.poll(() => renameTenant.mock.calls).toContainEqual(['t1', 'taken', true])
  })

  it('deletes a tenant through the DeleteDialog', async () => {
    deleteTenant.mockResolvedValue(undefined)
    mountList()
    await expect.poll(() => document.body.textContent).toContain('acme')
    // The trash button is an icon-font button (no box in the headless browser),
    // so dispatch its click directly rather than through the actionability check.
    const trash = document.querySelector<HTMLButtonElement>('[aria-label="Delete tenant acme"]')!
    trash.click()
    // The button only opens the confirmation; nothing is deleted yet.
    expect(deleteTenant).not.toHaveBeenCalled()
    mountOpenDialog()
    await page.getByTestId('button-confirm-delete').click()
    await expect.poll(() => deleteTenant.mock.calls).toContainEqual(['t1'])
  })
})
