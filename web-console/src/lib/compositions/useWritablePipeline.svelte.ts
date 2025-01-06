import {
  getExtendedPipeline,
  patchPipeline,
  type ExtendedPipeline,
  type Pipeline
} from '$lib/services/pipelineManager'
import invariant from 'tiny-invariant'

export const writablePipeline = (
  pipeline: { current: ExtendedPipeline },
  set: (pipeline: ExtendedPipeline) => void
) => {
  invariant(pipeline, 'useWritablePipeline: pipeline was not preloaded')
  let pipelineName = pipeline.current.name

  if (!pipelineName) {
    throw new Error('Cannot use pipeline without specifying its name')
  }

  return {
    get current() {
      return pipeline.current
    },
    async patch(newPipeline: Partial<Pipeline>) {
      const res = await patchPipeline(pipelineName, newPipeline)
      set(res)
      return res
    },
    async optimisticUpdate(newPipeline: Partial<ExtendedPipeline>) {
      set({ ...pipeline.current, ...newPipeline })
    }
  }
}

export const useRefreshPipeline = (
  pipeline: () => { current: ExtendedPipeline },
  set: (p: ExtendedPipeline) => void,
  preloaded: () => ExtendedPipeline,
  onNotFound?: () => void
) => {
  const pipelineName = $derived(pipeline().current.name)
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
    set(loaded)
  }

  $effect(() => {
    if (preloaded().name === pipelineName) {
      return
    }
    set(preloaded())
  })

  const restartReload = () => {
    const interval = setInterval(reload, 2000)
    return () => {
      clearInterval(interval)
    }
  }
  $effect.pre(() => {
    pipelineName
    return restartReload()
  })
}
