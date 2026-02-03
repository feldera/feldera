import { goto } from '$app/navigation'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'
import { resolve } from '$lib/functions/svelte'

export const prerender = false

export const load = async ({ params, route, url, fetch, parent }) => {
  await parent()
  const api = usePipelineManager()
  const preloadedPipeline = await api.getExtendedPipeline(
    decodeURIComponent(params.pipelineName),
    {
      onNotFound: () => {
        goto(resolve(`/`))
      }
    },
    {
      fetch
    }
  )
  return {
    preloadedPipeline
  }
}
