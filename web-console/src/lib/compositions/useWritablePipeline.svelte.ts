import {
  type ExtendedPipeline,
  type Pipeline,
  type PipelineThumb
} from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
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
  pipeline: { current: ExtendedPipeline | undefined }
  set: (pipeline: ExtendedPipeline) => void
  update: (p: Partial<ExtendedPipeline>) => void
}) => {
  if (pipeline.current && !pipeline.current.name) {
    throw new Error('Cannot use pipeline with an empty name')
  }

  return {
    get current() {
      return pipeline.current
    },
    async patch(newPipeline: Partial<Pipeline>, optimistic?: boolean) {
      if (!pipeline.current) {
        return
      }
      if (optimistic) {
        update(newPipeline)
      }
      const res = await api.patchPipeline(pipeline.current.name, newPipeline)
      if (!optimistic) {
        set(res)
      }
      return res
    }
  }
}

export type WritablePipeline<T extends boolean = false> = T extends true
  ? {
      current: NonNullable<ReturnType<typeof writablePipeline>['current']>
      patch: ReturnType<typeof writablePipeline>['patch']
    }
  : ReturnType<typeof writablePipeline>

/**
 * Refresh pipeline if the refreshVersion field changed
 */
export const useRefreshPipeline = ({
  getPipeline,
  getPipelines,
  getPreloaded,
  set,
  update,
  onNotFound
}: {
  getPipeline: () => { current: ExtendedPipeline }
  set: (p: ExtendedPipeline) => void
  update: (p: Partial<ExtendedPipeline>) => void
  getPreloaded?: () => ExtendedPipeline
  getPipelines: () => PipelineThumb[] | undefined
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

  if (getPreloaded) {
    $effect(() => {
      if (getPreloaded().name === pipelineName) {
        return
      }
      set(getPreloaded())
    })
  }

  $effect(() => {
    const ps = getPipelines()
    untrack(() => {
      const pipeline = getPipeline().current
      if (!ps) {
        return
      }
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
