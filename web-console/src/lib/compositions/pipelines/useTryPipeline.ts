import { goto } from '$app/navigation'
import { base } from '$app/paths'
import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
import type { PipelineDescr } from '$lib/services/manager'
import { postPipeline } from '$lib/services/pipelineManager'

export const useTryPipeline = () => {
  const { updatePipelines } = useUpdatePipelineList()
  return async (pipeline: PipelineDescr) => {
    try {
      const newPipeline = await postPipeline(pipeline)
      updatePipelines((pipelines) => (pipelines.push(newPipeline), pipelines))
    } catch {}
    goto(`${base}/pipelines/${encodeURIComponent(pipeline.name)}/`)
  }
}
