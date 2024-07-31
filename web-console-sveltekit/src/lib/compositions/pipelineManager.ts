import { getPipeline, putPipeline, type PipelineDescr } from '$lib/services/pipelineManager'
import { asyncWritable, rebounce, type Readable } from '@square/svelte-store'

export const useWritablePipeline = <T extends PipelineDescr>(
  pipelineName: Readable<string>,
  preloaded?: T
) =>
  asyncWritable(
    pipelineName,
    rebounce(getPipeline),
    async (newPipeline, _, oldPipeline) => {
      if (!oldPipeline) {
        throw new Error('useWritablePipeline called on non-existent pipeline')
      }
      await putPipeline(oldPipeline.name, newPipeline)
      return getPipeline(newPipeline.name)
    },
    { initial: preloaded }
  )
