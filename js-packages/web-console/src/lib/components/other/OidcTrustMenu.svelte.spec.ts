/**
 * The "Create new trust" form is hidden behind a button by default. Both that
 * button and a row's copy button reveal it; the copy button also prefills every
 * field, suffixing the name with "-copy" so the prefilled form is valid to
 * submit. The user still presses Create.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const trust = {
  id: 't1',
  name: 'ci',
  issuer: 'https://issuer.example',
  subject: 'repo:org/repo',
  audience: 'aud',
  description: 'desc',
  role: 'write'
}

vi.mock('$app/state', () => ({ page: { data: { feldera: { tenantName: 'acme' } } } }))
vi.mock('$lib/services/pipelineManager', () => ({
  getOidcTrustList: vi.fn(async () => [trust]),
  deleteOidcTrust: vi.fn(),
  postOidcTrust: vi.fn(async () => {})
}))

// Imported AFTER vi.mock so the mocks take effect.
import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
import OidcTrustMenu from './OidcTrustMenu.svelte'

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

function mountMenu() {
  mountTarget = document.createElement('div')
  document.body.appendChild(mountTarget)
  mounted = render(OidcTrustMenu, { target: mountTarget }) as any
}

const inputByPlaceholder = (placeholder: string) =>
  document.querySelector<HTMLInputElement>(`input[placeholder="${placeholder}"]`)
const roleSelect = () => document.querySelector<HTMLSelectElement>('select')

describe('OidcTrustMenu — duplicate a trust', () => {
  afterEach(async () => {
    // Clear the dialog first so the menu's onDestroy sees a closed dialog and
    // resets the module-level showForm, isolating the next test.
    useGlobalDialog().dialog = null
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    vi.clearAllMocks()
  })

  it('hides the form until the Create new trust button is pressed', async () => {
    mountMenu()
    await expect.poll(() => document.body.textContent).toContain('ci')
    // Form starts hidden: its fields are absent, the reveal button is shown.
    expect(inputByPlaceholder('github-actions-prod')).toBeNull()
    await page.getByRole('button', { name: 'Create new trust' }).click()
    // The button reveals the form (dropping the {#if showForm} gate fails this,
    // as the field would already be present before the click).
    await expect.poll(() => inputByPlaceholder('github-actions-prod')).toBeTruthy()
  })

  it('scrolls the dialog to the form when it is revealed', async () => {
    const scrollTo = vi.spyOn(Element.prototype, 'scrollTo').mockImplementation(() => {})
    try {
      mountMenu()
      await expect.poll(() => document.body.textContent).toContain('ci')
      await page.getByRole('button', { name: 'Create new trust' }).click()
      // revealForm scrolls the container to its bottom smoothly; dropping that
      // scroll leaves no such call and fails here.
      await expect
        .poll(() => scrollTo.mock.calls.some((c) => (c[0] as any)?.behavior === 'smooth'))
        .toBe(true)
    } finally {
      scrollTo.mockRestore()
    }
  })

  it('keeps a revealed form across the delete confirmation', async () => {
    mountMenu()
    await expect.poll(() => document.body.textContent).toContain('ci')
    await page.getByRole('button', { name: 'Create new trust' }).click()
    await expect.poll(() => inputByPlaceholder('github-actions-prod')).toBeTruthy()
    // Opening the delete confirmation swaps the global dialog, which unmounts
    // then remounts this menu. Icon-font button has no box, so click via the DOM.
    document
      .querySelector<HTMLButtonElement>('[aria-label="Delete ci trust relationship"]')!
      .click()
    // Reproduce that swap: unmount and remount the menu, as GlobalModal does.
    await mounted!.unmount()
    mountMenu()
    // The form stays revealed (dropping the keepForm handling hides it here).
    await expect.poll(() => inputByPlaceholder('github-actions-prod')).toBeTruthy()
  })

  it('restores the scroll offset across the delete confirmation', async () => {
    const scrollTo = vi.spyOn(Element.prototype, 'scrollTo').mockImplementation(() => {})
    try {
      mountMenu()
      await expect.poll(() => document.body.textContent).toContain('ci')
      // The headless dialog body is not tall enough to actually scroll, so force
      // an offset and fire the handler the way a real scroll would.
      const body = document.querySelector<HTMLDivElement>('.overflow-auto')!
      Object.defineProperty(body, 'scrollTop', { configurable: true, value: 120 })
      body.dispatchEvent(new Event('scroll'))
      // Swap to the delete confirmation, then reproduce the remount GlobalModal does.
      document
        .querySelector<HTMLButtonElement>('[aria-label="Delete ci trust relationship"]')!
        .click()
      await mounted!.unmount()
      mountMenu()
      // On remount the saved offset is reapplied (dropping the onscroll tracking
      // or the onMount restore leaves no scrollTo call with top: 120).
      await expect
        .poll(() => scrollTo.mock.calls.some((c) => (c[0] as any)?.top === 120))
        .toBe(true)
    } finally {
      scrollTo.mockRestore()
    }
  })

  it('hides a revealed form once the menu is closed', async () => {
    mountMenu()
    await expect.poll(() => document.body.textContent).toContain('ci')
    await page.getByRole('button', { name: 'Create new trust' }).click()
    await expect.poll(() => inputByPlaceholder('github-actions-prod')).toBeTruthy()
    // Closing the menu clears the global dialog; its onDestroy then resets the
    // module showForm (dropping that guard leaves the form revealed on reopen).
    useGlobalDialog().dialog = null
    await mounted!.unmount()
    mountMenu()
    await expect.poll(() => document.body.textContent).toContain('ci')
    expect(inputByPlaceholder('github-actions-prod')).toBeNull()
  })

  it('prefills the create form from an existing trust', async () => {
    mountMenu()
    await expect.poll(() => document.body.textContent).toContain('ci')
    // Icon-font button has no box in the headless browser, so click via the DOM.
    const copy = document.querySelector<HTMLButtonElement>(
      '[aria-label="Duplicate ci trust relationship"]'
    )!
    copy.click()
    // The name is suffixed so the prefilled form submits without a name clash...
    await expect.poll(() => inputByPlaceholder('github-actions-prod')?.value).toBe('ci-copy')
    // ...every other field is copied verbatim.
    expect(inputByPlaceholder('https://token.actions.githubusercontent.com')?.value).toBe(
      'https://issuer.example'
    )
    expect(inputByPlaceholder('repo:my-org/my-repo:ref:refs/heads/main')?.value).toBe(
      'repo:org/repo'
    )
    expect(inputByPlaceholder('feldera-acme')?.value).toBe('aud')
    expect(inputByPlaceholder('What does this trust grant?')?.value).toBe('desc')
    expect(roleSelect()?.value).toBe('write')
  })
})
