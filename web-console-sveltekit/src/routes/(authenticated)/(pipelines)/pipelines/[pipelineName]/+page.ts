import { getFullPipeline } from '$lib/services/pipelineManager'

export const prerender = false

export const load = async ({ params, route, url, fetch }) => {
  const preloadedPipeline = await getFullPipeline(params.pipelineName)
  return {
    preloadedPipeline
  }
}
