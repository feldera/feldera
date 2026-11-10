/**
 * The pipeline page sets the browser tab title to the name of the pipeline in view (issue #5805).
 */

import type { ComponentProps } from 'svelte'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

vi.mock('#lib/components/layout/pipelines/PipelineEditLayout.svelte', () => ({
  default: () => {}
}))
vi.mock('#lib/compositions/pipelines/usePipelineList.svelte.ts', () => ({
  usePipelineList: () => ({ pipelines: undefined }),
  useUpdatePipelineList: () => ({ updatePipeline: vi.fn() })
}))
vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({})
}))
vi.mock('#lib/compositions/useWritablePipeline.svelte.ts', () => ({
  writablePipeline: () => ({}),
  useRefreshPipeline: vi.fn()
}))

import PipelinePage from './+page.svelte'

// Only the props that the page reads. The preload never settles, so no pipeline loads.
const pageProps = (pipelineName: string) =>
  ({
    params: { pipelineName },
    data: { pipelinePreload: { pending: new Promise(() => {}), abort: () => {} } }
  }) as unknown as ComponentProps<typeof PipelinePage>

describe('pipeline page title', () => {
  it('replaces a stale title with the current pipeline name', async () => {
    document.title = 'nb-unihost-2 | Profile Viewer'

    render(PipelinePage, pageProps('nb-blp'))

    await expect.poll(() => document.title).toBe('nb-blp | Feldera')
  })

  it('follows a switch to another pipeline', async () => {
    const screen = render(PipelinePage, pageProps('nb-blp'))
    await expect.poll(() => document.title).toBe('nb-blp | Feldera')

    await screen.rerender(pageProps('nb-other'))

    await expect.poll(() => document.title).toBe('nb-other | Feldera')
  })

  it('shows a URL-encoded name decoded', async () => {
    render(PipelinePage, pageProps('my%20pipeline'))

    await expect.poll(() => document.title).toBe('my pipeline | Feldera')
  })
})
