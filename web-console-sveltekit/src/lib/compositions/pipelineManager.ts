import { handled } from '$lib/functions/request'
import {
  createOrReplaceProgram,
  deleteProgram,
  getPipeline,
  getProgram,
  newProgram,
  updatePipeline,
  type UpdatePipelineRequest
} from '$lib/services/manager'
import { getFullPipeline, type FullPipeline } from '$lib/services/pipelineManager'
import {
  asyncWritable,
  readable,
  rebounce,
  type Loadable,
  type Readable
} from '@square/svelte-store'

export const useWritablePipeline = <T extends FullPipeline>(pipelineName: Readable<string>, preloaded?: T) =>
  asyncWritable(pipelineName, rebounce(getFullPipeline), async (newPipeline, _, oldPipeline) => {
    const program_name =
      (oldPipeline?.name !== newPipeline.name ? undefined : newPipeline._programName) ??
      newPipeline.name + '_program'

    await createOrReplaceProgram({
      body: { code: newPipeline.code, description: '' },
      path: { program_name }
    })
    await updatePipeline({
      body: ((p) =>
        ({
          name: p.name,
          description: p.description,
          connectors: p._connectors,
          config: p.config,
          program_name
        }) satisfies UpdatePipelineRequest)(newPipeline),
      path: { pipeline_name: oldPipeline!.name }
    })
    if (oldPipeline?._programName && oldPipeline._programName !== program_name) {
      await deleteProgram({ path: { program_name: oldPipeline._programName } })
    }
    return getFullPipeline(newPipeline.name)
  }, {initial: preloaded})
