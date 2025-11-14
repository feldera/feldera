import {
  type ExtendedPipeline,
  type Pipeline,
  type PipelineThumb
} from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
import invariant from 'tiny-invariant'
import {
  usePipelineManager,
  type PipelineManagerApi
} from '$lib/compositions/usePipelineManager.svelte'

export const writablePipeline = ({
  api,
  pipeline,
  set,
  update
}: {
  api: PipelineManagerApi
  pipeline: { current: ExtendedPipeline }
  set: (pipeline: ExtendedPipeline) => void
  update: (p: Partial<ExtendedPipeline>) => void
}) => {
  invariant(pipeline, 'useWritablePipeline: pipeline was not preloaded')
  let pipelineName = pipeline.current.name

  if (!pipelineName) {
    throw new Error('Cannot use pipeline without specifying its name')
  }

  return {
    get current() {
      return pipeline.current
    },
    async patch(newPipeline: Partial<Pipeline>, optimistic?: boolean) {
      if (optimistic) {
        update(newPipeline)
      }
      const res = await api.patchPipeline(pipelineName, newPipeline)
      if (!optimistic) {
        set(res)
      }
      return res
    }
  }
}

export type WritablePipeline = ReturnType<typeof writablePipeline>

/**
 * Refresh pipeline if the refreshVersion field changed
 */
export const useRefreshPipeline = ({
  getPipeline,
  set,
  update,
  getPreloaded,
  getPipelines,
  onNotFound
}: {
  getPipeline: () => { current: ExtendedPipeline }
  set: (p: ExtendedPipeline) => void
  update: (p: Partial<ExtendedPipeline>) => void
  getPreloaded: () => ExtendedPipeline
  getPipelines: () => PipelineThumb[]
  onNotFound?: () => void
}) => {
  const pipelineName = $derived(getPipeline().current.name)
  const api = usePipelineManager()
  const reload = async () => {
    const requestedPipelineName = pipelineName
    let loaded = await api.getExtendedPipeline(requestedPipelineName, {
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
        update(thumb)
      } else {
        reload()
      }
    })
  })
}
