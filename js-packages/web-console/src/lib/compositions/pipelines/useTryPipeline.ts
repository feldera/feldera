import { goto } from '$app/navigation'
import {
  usePipelineList,
  useUpdatePipelineList
} from '$lib/compositions/pipelines/usePipelineList.svelte'
import { resolve } from '$lib/functions/svelte'
import { captureEvent } from '$lib/services/analytics'
import type { Demo } from '$lib/services/manager'
import { usePipelineManager } from '../usePipelineManager.svelte'

export const useTryPipeline = () => {
  const { updatePipelines } = useUpdatePipelineList()
  const pipelineList = usePipelineList()
  const api = usePipelineManager()
  return async (pipeline: Omit<Demo, 'title'>, trigger_location?: string) => {
    const already_created = (pipelineList.pipelines ?? []).some((p) => p.name === pipeline.name)
    captureEvent('demo_opened', {
      demo: pipeline.name,
      already_created,
      trigger_location
    })
    if (!already_created) {
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
    }
    goto(resolve(`/pipelines/${encodeURIComponent(pipeline.name)}/`))
  }
}
