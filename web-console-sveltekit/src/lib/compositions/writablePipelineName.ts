import type { NewPipelineRequest } from '$lib/services/manager'
import { type WritableLoadable, asyncWritable } from '@square/svelte-store'
import type { PipelineTab } from './useOpenPipelines'
import type { FullPipeline } from '$lib/services/pipelineManager'

export const writablePipelineName = (
  pipeline: WritableLoadable<FullPipeline | (NewPipelineRequest & { code: string })>,
  renamePipelineTab: (oldTab: PipelineTab, newTab: PipelineTab) => void
) =>
  asyncWritable(
    pipeline,
    (ppl): string => ppl.name,
    // async (newPipelineName, ppl, oldPipelineName) => {
    //   return newPipelineName
    // }
    async (newPipelineName: string, ppl, oldPipelineName) => {
      if (newPipelineName === '' || !ppl) {
        return oldPipelineName
      }
      await pipeline.set({
        ...ppl,
        name: newPipelineName
      })
      renamePipelineTab(oldPipelineName ? { existing: oldPipelineName } : { new: 'new' }, {
        existing: newPipelineName
      })
      window.location.replace('/pipelines/' + newPipelineName)
      return newPipelineName
    }
  )
