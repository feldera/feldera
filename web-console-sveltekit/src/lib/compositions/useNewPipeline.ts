import { newPipeline, newProgram, type NewPipelineRequest } from '$lib/services/manager'
import type { FullPipeline } from '$lib/services/pipelineManager'

import { asyncWritable, get, persisted } from '@square/svelte-store'

const emptyPipeline: FullPipeline = { name: '', description: '', config: {}, code: '', schema: {
  inputs: [],
  outputs: []
}, _programName: null, _connectors: [] }

const persistedNewPipeline = persisted<FullPipeline>(
  emptyPipeline,
  'pipelines/new',
  { storageType: 'LOCAL_STORAGE' }
) // localStore<NewPipelineRequest & {code: string}>('pipelines/new', {name: '', description: '', config: {}, code: '' }).value

export const writableNewPipeline = () =>
  asyncWritable(
    persistedNewPipeline,
    (p) => p,
    async (pipeline) => {
      if (!pipeline.name) {
        persistedNewPipeline.set(pipeline)
        return pipeline
      }
      const program_name = pipeline.name + '_program'
      await newProgram({ body: { name: program_name, description: '', code: pipeline.code } })
      await newPipeline({
        body: { name: pipeline.name, description: '', program_name, config: {} }
      })
      persistedNewPipeline.set(emptyPipeline)
      return pipeline
    }, {initial: emptyPipeline}
  )
