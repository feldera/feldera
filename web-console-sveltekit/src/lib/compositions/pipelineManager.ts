import { handled } from '$lib/functions/request'
import {
  createOrReplaceProgram,
  deleteProgram,
  getPipeline,
  getProgram,
  newProgram,
  updatePipeline as _updatePipeline,
  type UpdatePipelineRequest
} from '$lib/services/manager'
import { getFullPipeline, updatePipeline, type FullPipeline } from '$lib/services/pipelineManager'
import {
  asyncWritable,
  readable,
  rebounce,
  type Loadable,
  type Readable
} from '@square/svelte-store'

export const useWritablePipeline = <T extends FullPipeline>(
  pipelineName: Readable<string>,
  preloaded?: T
) =>
  asyncWritable(
    pipelineName,
    rebounce(getFullPipeline),
    async (newPipeline, _, oldPipeline) => {
      await updatePipeline(oldPipeline, newPipeline)
      return getFullPipeline(newPipeline.name)
    },
    { initial: preloaded }
  )
