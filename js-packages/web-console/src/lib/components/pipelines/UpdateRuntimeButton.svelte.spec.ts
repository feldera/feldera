import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineThumb } from '#lib/services/pipelineManager.ts'

const mocks = vi.hoisted(() => ({
  status: 'Stopped' as unknown,
  postUpdateRuntime: vi.fn(),
  updatePipeline: vi.fn(),
  discardPendingListRefresh: vi.fn()
}))
vi.mock('$app/state', () => ({ page: { data: { feldera: { version: '1.2.3' } } } }))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({ postUpdateRuntime: mocks.postUpdateRuntime })
}))
vi.mock('#lib/compositions/pipelines/usePipelineList.svelte.ts', () => ({
  usePipelineList: () => ({ pipelines: [{ name: 'p1', status: mocks.status }] }),
  useUpdatePipelineList: () => ({
    updatePipeline: mocks.updatePipeline,
    discardPendingListRefresh: mocks.discardPendingListRefresh
  })
}))

import UpdateRuntimeButton from './UpdateRuntimeButton.svelte'

const renderButton = (onUpdate = vi.fn()) => {
  render(UpdateRuntimeButton, { pipelineName: 'p1', onUpdate })
  return { button: page.getByRole('button', { name: 'Update' }), onUpdate }
}

const spinner = () => document.querySelector('svg.animate-spin')

afterEach(() => {
  mocks.status = 'Stopped'
  vi.clearAllMocks()
})

describe('UpdateRuntimeButton.svelte', () => {
  it('shows a filled button without a spinner while idle', async () => {
    const { button } = renderButton()
    await expect.element(button).toHaveClass('preset-filled-primary-500')
    expect(spinner()).toBeNull()
  })

  it('queues the pipeline for the new version, requests the update and turns busy on click', async () => {
    const { button, onUpdate } = renderButton()
    await button.click()

    expect(mocks.postUpdateRuntime).toHaveBeenCalledWith('p1')
    expect(mocks.discardPendingListRefresh).toHaveBeenCalledOnce()
    expect(onUpdate).toHaveBeenCalledOnce()
    const updater = mocks.updatePipeline.mock.calls[0][1] as (p: PipelineThumb) => PipelineThumb
    expect(
      updater({ name: 'p1', status: 'Stopped', platformVersion: '1.0.0' } as PipelineThumb)
    ).toMatchObject({ platformVersion: '1.2.3', status: { Queued: { cause: 'compile' } } })

    await expect.element(button).toHaveClass('preset-outlined-primary-500')
    await expect.element(button).toHaveAttribute('aria-disabled', 'true')
    expect(spinner()).not.toBeNull()
  })

  it('ignores a second click while busy', async () => {
    const { button } = renderButton()
    await button.click()
    // `pointer-events-none` would block a real click, so dispatch the event directly.
    button.element().dispatchEvent(new MouseEvent('click', { bubbles: true }))
    expect(mocks.postUpdateRuntime).toHaveBeenCalledOnce()
  })

  it('stays busy without a click while the pipeline compiles', async () => {
    mocks.status = { CompilingRust: { cause: 'compile' } }
    const { button } = renderButton()
    await expect.element(button).toHaveClass('preset-outlined-primary-500')
    expect(spinner()).not.toBeNull()
  })
})
