import type { PipelineDescr } from '$lib/services/pipelineManager'
import { postPipeline, type ExtendedPipelineDescr } from '$lib/services/pipelineManager'

import { asyncWritable, get, persisted } from '@square/svelte-store'

const emptyPipeline: PipelineDescr = {
  name: '',
  description: '',
  runtime_config: {},
  program_config: {},
  program_code: ''
}

const persistedNewPipeline = persisted<PipelineDescr>(emptyPipeline, 'pipelines/new', {
  storageType: 'LOCAL_STORAGE'
}) // localStore<NewPipelineRequest & {code: string}>('pipelines/new', {name: '', description: '', config: {}, code: '' }).value

export const writableNewPipeline = () =>
  asyncWritable(
    persistedNewPipeline,
    (p) => p,
    async (pipeline) => {
      if (!pipeline.name) {
        persistedNewPipeline.set(pipeline)
        return pipeline
      }
      await postPipeline(pipeline)
      persistedNewPipeline.set(emptyPipeline)
      return pipeline
    },
    { initial: emptyPipeline }
  )
