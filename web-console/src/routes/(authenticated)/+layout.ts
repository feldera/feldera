import { error } from '@sveltejs/kit'
import { getPipelines } from '$lib/services/pipelineManager'

export const load = async ({ parent }) => {
  const data = await parent()
  if (typeof data.auth === 'object' && 'login' in data.auth) {
    await data.auth.login()
    error(401)
  }
  const pipelines = await getPipelines()
  return { ...data, preloaded: { pipelines } }
}
