import type { NewPipelineRequest } from '$lib/services/manager'
import { type WritableLoadable, asyncWritable } from '@square/svelte-store'
import type { PipelineTab } from './useOpenPipelines'
import type { FullPipeline } from '$lib/services/pipelineManager'
import { base } from '$app/paths'

export const writablePipelineName = (
  pipeline: WritableLoadable<FullPipeline | (NewPipelineRequest & { code: string })>,
  onRenamePipeline?: (oldTab: PipelineTab, newTab: PipelineTab) => void
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
      onRenamePipeline?.(oldPipelineName ? { existing: oldPipelineName } : { new: 'new' }, {
        existing: newPipelineName
      })
      window.location.replace(`${base}/pipelines/` + newPipelineName)
      return newPipelineName
    }
  )
