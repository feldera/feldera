import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { page, userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '#lib/services/rbac.ts'

vi.mock('$app/state', () => ({
  page: {
    data: { feldera: { role: 'write', permissions: permissionsOf('write'), version: '1.2.3' } }
  }
}))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({ postUpdateRuntime: vi.fn() })
}))
const pipeline = vi.hoisted(() => ({ status: 'Stopped' as unknown }))
vi.mock('#lib/compositions/pipelines/usePipelineList.svelte.ts', () => ({
  usePipelineList: () => ({ pipelines: [{ name: 'p1', ...pipeline }] }),
  useUpdatePipelineList: () => ({ updatePipeline: vi.fn(), discardPendingListRefresh: vi.fn() })
}))

import PipelineVersionTooltip from './PipelineVersionTooltip.svelte'

describe('PipelineVersionTooltip.svelte', () => {
  // The screenshot can take longer while other test files run in the same browser.
  const screenshotOptions = { timeout: 15_000 }

  beforeAll(async () => {
    await page.viewport(800, 600)
    document.documentElement.dataset.theme = 'feldera-modern-theme'
    return () => {
      delete document.documentElement.dataset.theme
    }
  })

  /** Opens the popover by hovering its anchor, the render container. */
  const openPopover = async () => {
    const props = {
      pipelineName: 'p1',
      status: 'update_available' as const,
      baseRuntimeVersion: '1.2.3'
    }
    const { container, rerender } = render(PipelineVersionTooltip, props)
    container.style.cssText = 'width: 200px; height: 24px; margin-left: 400px'
    await userEvent.hover(page.elementLocator(container))
    const button = page.getByRole('button', { name: 'Update' })
    await expect.element(button).toBeVisible()
    return { button, props, rerender }
  }

  afterEach(() => {
    pipeline.status = 'Stopped'
  })

  it('closes the popover 2 seconds after a click on Update', async () => {
    const { button, props, rerender } = await openPopover()
    await button.click()
    // The click sets the new runtime version, so the version status becomes 'latest' at once.
    await rerender({ ...props, status: 'latest' })
    await new Promise((resolve) => setTimeout(resolve, 1_000))
    await expect.element(button).toBeVisible()
    await expect.element(button, { timeout: 3_000 }).not.toBeInTheDocument()
  })

  it('centers the spinner vertically on the busy Update button', async () => {
    // A queued pipeline keeps the Update button busy, so the spinner shows without a click.
    pipeline.status = { Queued: { cause: 'compile' } }
    const { button } = await openPopover()
    const spinner = button.element().parentElement!.querySelector('svg')!
    const centerY = (rect: DOMRect) => rect.top + rect.height / 2
    expect(
      Math.abs(
        centerY(spinner.getBoundingClientRect()) - centerY(button.element().getBoundingClientRect())
      )
    ).toBeLessThan(1)

    const popover = page.elementLocator(button.element().closest('[popover]')!)
    await expect.element(popover).toMatchScreenshot('update-busy', screenshotOptions)
  })
})
