import { untrack } from 'svelte'
import {
  type PipelineManagerApi,
  usePipelineManager
} from '$lib/compositions/usePipelineManager.svelte'
import type { ExtendedPipeline, Pipeline, PipelineThumb } from '$lib/services/pipelineManager'

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
 * Refresh pipeline if the refreshVersion field changed.
 *
 * Safe to call before the pipeline cache is populated — the effects no-op
 * until `getPipeline().current` is defined, and re-run on the next
 * pipeline-list tick once it is.
 */
export const useRefreshPipeline = ({
  getPipeline,
  getPipelines,
  getPreloaded,
  getDeleted,
  set,
  update,
  onNotFound
}: {
  getPipeline: () => { current: ExtendedPipeline | undefined }
  set: (p: ExtendedPipeline) => void
  update: (p: Partial<ExtendedPipeline>) => void
  getPreloaded?: () => ExtendedPipeline
  getPipelines: () => PipelineThumb[] | undefined
  getDeleted: () => boolean
  onNotFound?: () => void
}) => {
  const api = usePipelineManager()
  const reload = async (requestedPipelineName: string) => {
    const loaded = await api.getExtendedPipeline(requestedPipelineName, {
      onNotFound: () => {
        if (requestedPipelineName !== getPipeline().current?.name) {
          return
        }
        onNotFound?.()
      }
    })
    if (requestedPipelineName !== getPipeline().current?.name) {
      return
    }
    set(loaded)
  }

  if (getPreloaded) {
    $effect(() => {
      const current = getPipeline().current
      if (!current || getPreloaded().name === current.name) {
        return
      }
      set(getPreloaded())
    })
  }

  $effect(() => {
    const ps = getPipelines()
    untrack(() => {
      if (getDeleted?.()) return
      const pipeline = getPipeline().current
      if (!pipeline || !ps) {
        return
      }
      const thumb = ps.find((p) => p.name === pipeline.name)
      if (!thumb) {
        onNotFound?.()
        return
      }
      if (thumb.refreshVersion === pipeline.refreshVersion) {
        update(thumb)
      } else {
        reload(pipeline.name)
      }
    })
  })
}
