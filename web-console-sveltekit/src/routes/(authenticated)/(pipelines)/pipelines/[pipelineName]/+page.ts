import { getPipeline } from '$lib/services/pipelineManager'

export const prerender = false

export const load = async ({ params, route, url, fetch }) => {
  const preloadedPipeline = await getPipeline(params.pipelineName)
  return {
    preloadedPipeline
  }
}
