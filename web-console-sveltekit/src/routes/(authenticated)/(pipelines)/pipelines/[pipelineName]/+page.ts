import { getFullPipeline } from '$lib/services/pipelineManager'

export const prerender = false

export const load = async ({ params, route, url, fetch }) => {
  console.log('a', params)
  const preloadedPipeline = await getFullPipeline(params.pipelineName)
  console.log('b')
  return {
    preloadedPipeline
  }
}
