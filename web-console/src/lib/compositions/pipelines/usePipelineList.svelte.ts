import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
import { onMount } from 'svelte'
import { useToast } from '$lib/compositions/useToastNotification'
import { closedIntervalAction } from '$lib/functions/common/promise'

let pipelines = $state<PipelineThumb[] | undefined>(undefined)
const reload = async () => {
  pipelines = await getPipelines()
}

export const useUpdatePipelineList = () => {
  return {
    updatePipelines(updater: (ps: PipelineThumb[]) => PipelineThumb[]) {
      pipelines = updater(pipelines ?? [])
    }
  }
}

export const useRefreshPipelineList = () => {
  const { toastError } = useToast()
  onMount(() => {
    return closedIntervalAction(() => reload().catch(toastError), 2000)
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
