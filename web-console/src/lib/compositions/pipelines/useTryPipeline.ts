import { goto } from '$app/navigation'
import { base } from '$app/paths'
import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
import type { Demo } from '$lib/services/manager'
import { usePipelineManager } from '../usePipelineManager.svelte'

export const useTryPipeline = () => {
  const { updatePipelines } = useUpdatePipelineList()
  const api = usePipelineManager()
  return async (pipeline: Omit<Demo, 'title'>) => {
    try {
      const newPipeline = await api.postPipeline({
        name: pipeline.name,
        description: pipeline.description,
        program_code: pipeline.program_code,
        program_config: {},
        runtime_config: {},
        udf_rust: pipeline.udf_rust,
        udf_toml: pipeline.udf_toml
      })
      updatePipelines((pipelines) => (pipelines.push(newPipeline), pipelines))
    } catch {}
    goto(`${base}/pipelines/${encodeURIComponent(pipeline.name)}/`)
  }
}
