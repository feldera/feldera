/**
 * Renaming a pipeline navigates to its new URL. The page must keep the loaded pipeline in view
 * across that navigation, instead of unmounting the layout and loading the pipeline again.
 */

import type { ComponentProps } from 'svelte'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import type { ExtendedPipeline, PipelineThumb } from '#lib/services/pipelineManager.ts'

const mocks = vi.hoisted(() => ({
  layoutMounts: 0,
  layoutProps: undefined as
    | undefined
    | { pipelineName: string; pipeline: { current: ExtendedPipeline | undefined } },
  setPipeline: undefined as undefined | ((p: ExtendedPipeline) => void)
}))

const list = $state<{ pipelines: PipelineThumb[] }>({ pipelines: [] })

vi.mock('#lib/components/layout/pipelines/PipelineEditLayout.svelte', () => ({
  default: (_anchor: unknown, props: typeof mocks.layoutProps) => {
    mocks.layoutMounts += 1
    mocks.layoutProps = props
  }
}))
vi.mock('#lib/compositions/pipelines/usePipelineList.svelte.ts', () => ({
  usePipelineList: () => list,
  useUpdatePipelineList: () => ({
    updatePipeline: (name: string, updater: (p: PipelineThumb) => PipelineThumb) => {
      const idx = list.pipelines.findIndex((p) => p.name === name)
      if (idx !== -1) {
        list.pipelines[idx] = updater(list.pipelines[idx])
      }
    },
    discardPendingListRefresh: () => {}
  })
}))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({})
}))
vi.mock('#lib/compositions/useWritablePipeline.svelte.ts', () => ({
  writablePipeline: (args: {
    pipeline: { current: ExtendedPipeline | undefined }
    set: (p: ExtendedPipeline) => void
  }) => {
    mocks.setPipeline = args.set
    return args.pipeline
  },
  useRefreshPipeline: vi.fn()
}))

import PipelinePage from './+page.svelte'

const pageProps = (pipelineName: string, pending: Promise<ExtendedPipeline>) =>
  ({
    params: { pipelineName },
    data: { pipelinePreload: { pending, abort: () => {} } }
  }) as unknown as ComponentProps<typeof PipelinePage>

describe('pipeline rename', () => {
  it('keeps the pipeline in view while the page navigates to the new name', async () => {
    const loaded = { name: 'before', refreshVersion: 1 } as unknown as ExtendedPipeline
    list.pipelines = [loaded as PipelineThumb]
    const screen = render(PipelinePage, pageProps('before', Promise.resolve(loaded)))
    await expect.poll(() => mocks.layoutProps?.pipeline.current?.name).toBe('before')
    const mountsBeforeRename = mocks.layoutMounts

    // The rename request returns the pipeline with its new name, then the page navigates.
    mocks.setPipeline?.({ ...loaded, name: 'after' })
    await screen.rerender(pageProps('after', new Promise(() => {})))

    expect(mocks.layoutProps?.pipelineName).toBe('after')
    expect(mocks.layoutProps?.pipeline.current?.name).toBe('after')
    expect(mocks.layoutMounts).toBe(mountsBeforeRename)
  })
})
