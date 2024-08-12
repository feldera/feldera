import {
  getExtendedPipeline,
  patchPipeline,
  type ExtendedPipeline,
  type Pipeline
} from '$lib/services/pipelineManager'
import invariant from 'tiny-invariant'

export const useWritablePipeline = (pipelineName: () => string, preloaded: ExtendedPipeline) => {
  if (!pipelineName()) {
    throw new Error('Cannot use pipeline without specifying its name')
  }

  let pipeline = $state(preloaded)

  const reload = async () => {
    pipeline = await getExtendedPipeline(pipelineName())
  }

  $effect(() => {
    let interval = setInterval(reload, 2000)
    reload()
    return () => {
      clearInterval(interval)
    }
  })

  return {
    get pipeline() {
      invariant(pipeline, 'useWritablePipeline: pipeline was not preloaded')
      return pipeline
    },
    async patch(newPipeline: Partial<Pipeline>) {
      return (pipeline = await patchPipeline(pipelineName(), newPipeline))
    },
    async optimisticUpdate(newPipeline: Partial<ExtendedPipeline>) {
      if (!pipeline) {
        return
      }
      pipeline = { ...pipeline, ...newPipeline }
    }
  }
}
