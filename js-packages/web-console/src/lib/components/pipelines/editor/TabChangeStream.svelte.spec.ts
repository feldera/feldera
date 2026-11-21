import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { ExtendedPipeline } from '#lib/services/pipelineManager.ts'

vi.mock('#lib/compositions/usePipelineManager.svelte.ts', () => ({
  usePipelineManager: () => ({})
}))

import TabChangeStream from './TabChangeStream.svelte'

describe('TabChangeStream.svelte', () => {
  it('mounts and lists the relations of a stopped pipeline', async () => {
    const pipeline = {
      current: {
        id: 'p-changestream-mount',
        name: 'p-changestream-mount',
        status: 'Stopped',
        programInfo: { schema: { inputs: [{ name: 't1', fields: [] }], outputs: [] } }
      } as unknown as ExtendedPipeline
    }

    render(TabChangeStream, { pipeline })

    await expect.element(page.getByText('t1')).toBeInTheDocument()
  })
})
