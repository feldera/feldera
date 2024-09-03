import { goto } from '$app/navigation'
import { base } from '$app/paths'
import { getExtendedPipeline } from '$lib/services/pipelineManager'

export const prerender = false

export const load = async ({ params, route, url, fetch, parent }) => {
  await parent()
  const preloadedPipeline = await getExtendedPipeline(decodeURIComponent(params.pipelineName), {
    fetch
  }).catch((e) => goto(`${base}/`) as never)
  return {
    preloadedPipeline
  }
}
