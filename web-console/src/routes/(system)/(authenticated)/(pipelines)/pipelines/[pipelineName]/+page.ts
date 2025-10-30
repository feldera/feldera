import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'
import { goto } from '$app/navigation'
import { base } from '$app/paths'

export const prerender = false

export const load = async ({ parent, params }) => {
  await parent()
  const api = usePipelineManager()
  const pipelineName = decodeURIComponent(params.pipelineName)
  return {
    preloaded: {
      pipeline: api.getExtendedPipeline(pipelineName, {
        onNotFound: () => {
          goto(`${base}/`)
        }
      })
    }
  }
}
