import { goto } from '$app/navigation'
import { resolve } from '$lib/functions/svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'

export const prerender = false

export const load = async ({ params, route, url, fetch, parent }) => {
  await parent()
  const api = usePipelineManager()
  const preloadedPipeline = await api.getExtendedPipeline(decodeURIComponent(params.pipelineName), {
    fetch,
    onNotFound: () => {
      goto(resolve(`/`))
    }
  })
  return {
    preloadedPipeline
  }
}
