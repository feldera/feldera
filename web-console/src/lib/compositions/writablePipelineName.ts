import { type WritableLoadable, asyncWritable } from '@square/svelte-store'
import type { PipelineTab } from './useOpenPipelines'
import type { Pipeline } from '$lib/services/pipelineManager'

export const writablePipelineName = (
  pipeline: WritableLoadable<Pipeline>,
  onRenamePipeline?: (oldTab: PipelineTab, newTab: PipelineTab) => void
) =>
  asyncWritable(
    pipeline,
    (ppl): string => ppl.name,
    async (newPipelineName: string, ppl, oldPipelineName) => {
      if (newPipelineName === '' || !ppl) {
        return oldPipelineName
      }
      await pipeline.set({
        ...ppl,
        name: newPipelineName
      })
      onRenamePipeline?.(oldPipelineName ? { existing: oldPipelineName } : { new: 'new' }, {
        existing: newPipelineName
      })
      return newPipelineName
    }
  )
