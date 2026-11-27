/**
 * PostHog session replays must not record a new API key. The recorder leaves out
 * every element under the `ph-no-capture` class.
 */
import { describe, expect, it, vi } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

const SECRET = 'apikey:test-secret-0123456789'

vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({
    postApiKey: vi.fn(async (name: string) => ({ name, api_key: SECRET }))
  })
}))

// On submit, superforms updates the SvelteKit page state, which a component test
// does not have.
vi.mock('$app/forms', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$app/forms')>()),
  applyAction: vi.fn()
}))

import NewApiKeyForm from './NewApiKeyForm.svelte'

const generateKey = async () => {
  render(NewApiKeyForm)
  await userEvent.fill(page.getByPlaceholder('Name'), 'ci')
  await userEvent.click(page.getByRole('button', { name: 'Generate' }))
  await expect.element(page.getByText(SECRET)).toBeInTheDocument()
}

describe('NewApiKeyForm', () => {
  it('keeps the generated secret out of session replays', async () => {
    await generateKey()
    expect(page.getByText(SECRET).element().closest('.ph-no-capture')).not.toBeNull()
  })

  it('keeps the copy button out of session replays', async () => {
    await generateKey()
    const copyButton = page.getByRole('button', { name: 'Copy to clipboard' }).element()
    expect(copyButton.closest('.ph-no-capture')).not.toBeNull()
  })
})
