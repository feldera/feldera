import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
import { onMount } from 'svelte'

let pipelines = $state([] as PipelineThumb[])
const reload = async () => {
  pipelines = await getPipelines()
}

export const useUpdatePipelineList = () => {
  return {
    updatePipelines(updater: (ps: PipelineThumb[]) => PipelineThumb[]) {
      pipelines = updater(pipelines)
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

export const usePipelineList = (preloaded?: { pipelines: typeof pipelines }) => {
  if (preloaded) {
    pipelines = preloaded.pipelines
  }
  return {
    get pipelines() {
      return [...pipelines]
    },
    set pipelines(ps: typeof pipelines) {
      pipelines = ps
    }
  }
}
