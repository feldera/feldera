import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
import { onMount } from 'svelte'

let pipelines = $state([] as PipelineThumb[])
const reload = async () => {
  pipelines = await getPipelines()
}

export const useUpdatePipelineList = () => {
  return {
    updatePipelines(updater: (ps: typeof pipelines) => typeof pipelines) {
      pipelines = updater(pipelines)
    }
  }
}

export const usePipelineList = (preloaded?: { pipelines: typeof pipelines }) => {
  if (preloaded) {
    pipelines = preloaded.pipelines
  }
  onMount(() => {
    let interval = setInterval(() => reload(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
  return {
    get pipelines() {
      const x = [...pipelines]
      return x
    },
    set pipelines(ps: typeof pipelines) {
      pipelines = ps
    }
  }
}
