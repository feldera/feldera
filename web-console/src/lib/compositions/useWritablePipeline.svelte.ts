import {
  getExtendedPipeline,
  patchPipeline,
  type ExtendedPipeline,
  type Pipeline
} from '$lib/services/pipelineManager'
import invariant from 'tiny-invariant'

export const useWritablePipeline = (preloaded: () => ExtendedPipeline, onNotFound?: () => void) => {
  let pipeline = $state(preloaded())
  let pipelineName = $derived(pipeline.name)

  if (!pipelineName) {
    throw new Error('Cannot use pipeline without specifying its name')
  }

  const reload = async () => {
    const requestedPipelineName = pipelineName
    let loaded = await getExtendedPipeline(requestedPipelineName, {
      onNotFound: () => {
        if (requestedPipelineName !== pipelineName) {
          return
        }
        onNotFound?.()
      }
    })
    if (requestedPipelineName !== pipelineName) {
      return
    }
    pipeline = loaded
  }

  let interval: NodeJS.Timeout
  $effect(() => {
    restartInterval()
    pipeline = preloaded()
  })
  const restartInterval = () => {
    clearInterval(interval)
    interval = setInterval(reload, 2000)
  }
  $effect(() => {
    queueMicrotask(reload)
    return () => {
      clearInterval(interval)
    }
  })

  return {
    get current() {
      invariant(pipeline, 'useWritablePipeline: pipeline was not preloaded')
      return pipeline
    },
    async patch(newPipeline: Partial<Pipeline>) {
      return (pipeline = await patchPipeline(pipelineName, newPipeline))
    },
    async optimisticUpdate(newPipeline: Partial<ExtendedPipeline>) {
      if (!pipeline) {
        return
      }
      pipeline = { ...pipeline, ...newPipeline }
    }
  }
}
