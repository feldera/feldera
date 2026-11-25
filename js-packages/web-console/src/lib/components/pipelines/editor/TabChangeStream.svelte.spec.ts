import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { ExtendedPipeline } from '#lib/services/pipelineManager.ts'
import { usePipelineActionCallbacks } from '#lib/compositions/pipelines/usePipelineActionCallbacks.svelte.ts'

vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({ relationEgressStream: async () => new Error('no stream in test') })
}))

import TabChangeStream, { hasSelectedRelations } from './TabChangeStream.svelte'

const pipelineProp = (id: string, name: string, status: string) => ({
  current: {
    id,
    name,
    status,
    programInfo: { schema: { inputs: [{ name: 't1', fields: [] }], outputs: [] } }
  } as unknown as ExtendedPipeline
})

describe('TabChangeStream.svelte', () => {
  it('mounts and lists the relations of a stopped pipeline', async () => {
    render(TabChangeStream, {
      pipeline: pipelineProp('p-changestream-mount', 'p-changestream-mount', 'Stopped')
    })

    await expect.element(page.getByText('t1')).toBeInTheDocument()
  })

  it('tracks the selected relations of a pipeline that replaces a deleted one with the same name', async () => {
    const first = render(TabChangeStream, {
      pipeline: pipelineProp('id-old', 'p-recreate', 'Running')
    })
    await page.getByRole('checkbox').first().click()
    expect(hasSelectedRelations('p-recreate')).toBe(true)
    for (const callback of usePipelineActionCallbacks().getAll('', 'delete')) {
      await callback('p-recreate')
    }
    await first.unmount()

    render(TabChangeStream, { pipeline: pipelineProp('id-new', 'p-recreate', 'Running') })
    await page.getByRole('checkbox').first().click()
    expect(hasSelectedRelations('p-recreate')).toBe(true)
  })
})
