import { describe, expect, it, vi } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '#lib/services/rbac.ts'

vi.mock('$app/state', () => ({
  page: {
    data: { feldera: { role: 'write', permissions: permissionsOf('write'), unstableFeatures: [] } }
  }
}))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({ postUpdateRuntime: vi.fn() })
}))
vi.mock('#lib/compositions/pipelines/usePipelineList.svelte.ts', () => ({
  usePipelineList: () => ({ pipelines: [] }),
  useUpdatePipelineList: () => ({ updatePipeline: vi.fn(), discardPendingListRefresh: vi.fn() })
}))

import PipelineVersion from './PipelineVersion.svelte'

const REPOSITION_WAIT_MS = 200

describe('PipelineVersion.svelte', () => {
  it('keeps the open update popover next to the version when the update replaces the chip', async () => {
    const props = {
      pipelineName: 'p1',
      runtimeVersion: '1.0.0',
      baseRuntimeVersion: '1.2.3',
      configuredRuntimeVersion: null
    }
    const { container, rerender } = render(PipelineVersion, props)
    container.style.cssText = 'display: flex; gap: 8px; margin: 300px 0 0 300px'
    await userEvent.hover(page.getByText('Update available'))
    const button = page.getByRole('button', { name: 'Update' })
    await expect.element(button).toBeVisible()
    const popover = button.element().closest<HTMLElement>('[popover]')!

    // In the app, a click on Update sets the new runtime version, which replaces the chip next to
    // the popover. The click also shows the spinner, so the popover repositions to fit its new size.
    // The click and the new version arrive in the same update, as they do in the app.
    ;(button.element() as HTMLButtonElement).click()
    await rerender({ ...props, runtimeVersion: '1.2.3' })
    await expect.element(button.element().parentElement!.querySelector('svg')!).toBeVisible()
    // Gives the popover time to reposition. A poll would pass before it moves.
    await new Promise((resolve) => setTimeout(resolve, REPOSITION_WAIT_MS))

    expect(popover.getBoundingClientRect().top).toBeGreaterThan(300)
  })
})
