import {
  getExtendedPipeline,
  patchPipeline,
  type ExtendedPipeline,
  type Pipeline,
  type PipelineThumb
} from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
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

/**
 * Refresh pipeline if the refreshVersion field changed
 */
export const useRefreshPipeline = (
  getPipeline: () => { current: ExtendedPipeline },
  set: (p: ExtendedPipeline) => void,
  getPreloaded: () => ExtendedPipeline,
  getPipelines: () => PipelineThumb[],
  onNotFound?: () => void
) => {
  const pipelineName = $derived(getPipeline().current.name)
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
    if (getPreloaded().name === pipelineName) {
      return
    }
    set(getPreloaded())
  })

  $effect(() => {
    const ps = getPipelines()
    untrack(() => {
      const pipeline = getPipeline().current
      const thumb = ps.find((p) => p.name === pipeline.name)
      if (!thumb) {
        return
      }
      if (thumb.refreshVersion === pipeline.refreshVersion) {
        set({
          ...pipeline,
          ...thumb
        })
      } else {
        reload()
      }
    })
  })
}
