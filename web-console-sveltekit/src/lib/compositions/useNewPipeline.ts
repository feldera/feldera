import { newPipeline, newProgram, type NewPipelineRequest } from '$lib/services/manager'

import { asyncWritable, get, persisted } from '@square/svelte-store'

const persistedNewPipeline = persisted<NewPipelineRequest & { code: string }>(
  { name: '', description: '', config: {}, code: '' },
  'pipelines/new',
  { storageType: 'LOCAL_STORAGE' }
) // localStore<NewPipelineRequest & {code: string}>('pipelines/new', {name: '', description: '', config: {}, code: '' }).value

export const writableNewPipeline = () =>
  asyncWritable(
    persistedNewPipeline,
    (p) => p,
    async (pipeline) => {
      if (pipeline.name) {
        const program_name = pipeline.name + '_program'
        await newProgram({ body: { name: program_name, description: '', code: pipeline.code } })
        await newPipeline({
          body: { name: pipeline.name, description: '', program_name, config: {} }
        })
        return pipeline
      }
      persistedNewPipeline.set(pipeline)
      return pipeline
    }
  )
