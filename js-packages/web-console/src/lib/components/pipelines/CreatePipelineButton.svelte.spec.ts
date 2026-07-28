// Component test for the create-pipeline affordance gate: hidden unless the
// caller holds write:pipeline. This is the single New Pipeline control reused
// across the app, so this one gate covers every call site.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

const roleState = vi.hoisted(() => ({ current: 'read' as 'read' | 'write' | 'admin' | 'owner' }))
vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return { role: roleState.current, permissions: permissionsOf(roleState.current) }
      }
    }
  }
}))

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ postPipeline: vi.fn() })
}))

vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  useUpdatePipelineList: () => ({ updatePipelines: vi.fn() })
}))

import CreatePipelineButton from './CreatePipelineButton.svelte'

afterEach(() => {
  roleState.current = 'read'
})

describe('CreatePipelineButton.svelte', () => {
  it('shows the New Pipeline button for a write caller', async () => {
    roleState.current = 'write'
    await render(CreatePipelineButton, {})
    await expect.element(page.getByText('New Pipeline')).toBeInTheDocument()
  })

  it('hides the New Pipeline button for a read-only caller', async () => {
    roleState.current = 'read'
    await render(CreatePipelineButton, {})
    // Reverting the gate renders the button for read, failing this.
    await expect.element(page.getByText('New Pipeline')).not.toBeInTheDocument()
  })
})
