import { onMount } from 'svelte'
import { closedIntervalAction } from '$lib/functions/common/promise'
import type { PipelineThumb } from '$lib/services/pipelineManager'
import { type PipelineManagerApi, usePipelineManager } from '../usePipelineManager.svelte'

let pipelines = $state<PipelineThumb[] | undefined>(undefined)

// Flag to discard the next in-flight polling response that would overwrite an optimistic update.
let discardNextUpdate = false

const reload = async (api: PipelineManagerApi) => {
  discardNextUpdate = false // Make sure to only discard in-flight responses
  const result = await api.getPipelines()
  if (discardNextUpdate) {
    discardNextUpdate = false
    return
  }
  pipelines = result
}

export const useUpdatePipelineList = () => {
  return {
    updatePipelines(updater: (ps: PipelineThumb[]) => PipelineThumb[]) {
      pipelines = updater(pipelines ?? [])
    },
    updatePipeline(pipelineName: string, updater: (ps: PipelineThumb) => PipelineThumb) {
      const idx = pipelines?.findIndex((p) => p.name === pipelineName) ?? -1
      if (!pipelines || idx === -1) {
        return
      }
      pipelines[idx] = updater(pipelines[idx])
    },
    discardPendingListRefresh() {
      discardNextUpdate = true
    }
  }
}

/**
 * Refresh the pipeline list every 2 seconds.
 * A single instance of this hook should be mounted at one time.
 */
export const useRefreshPipelineList = () => {
  const api = usePipelineManager()
  onMount(() => {
    return closedIntervalAction(() => reload(api), 2000)
  })
}

export const usePipelineList = (preloaded?: { pipelines: PipelineThumb[] }) => {
  if (preloaded && !pipelines) {
    pipelines = preloaded.pipelines
  }
  return {
    get pipelines() {
      return [...(pipelines ?? [])]
    },
    set pipelines(ps: PipelineThumb[]) {
      pipelines = ps
    }
  }
}
