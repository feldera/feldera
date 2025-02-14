import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
import { onMount } from 'svelte'

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
  onMount(() => {
    let interval = setInterval(() => reload(), 2000)
    return () => {
      clearInterval(interval)
    }
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
