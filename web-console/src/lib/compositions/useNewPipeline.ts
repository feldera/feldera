import type { Pipeline } from '$lib/services/pipelineManager'
import { postPipeline } from '$lib/services/pipelineManager'

import { asyncWritable, get, persisted } from '@square/svelte-store'

const emptyPipeline: Pipeline = {
  name: '',
  description: '',
  runtimeConfig: {},
  programConfig: {},
  programCode: ''
}

const persistedNewPipeline = persisted<Pipeline>(emptyPipeline, 'pipelines/new', {
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
