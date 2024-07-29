import { goto } from '$app/navigation'
import { base } from '$app/paths'
import { getPipeline } from '$lib/services/pipelineManager'

export const prerender = false

export const load = async ({ params, route, url, fetch, parent }) => {
  await parent()
  const preloadedPipeline = await getPipeline(params.pipelineName).catch((e) => goto(`${base}/`))
  return {
    preloadedPipeline
  }
}
