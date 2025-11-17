import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'

export const load = async ({ parent, fetch }) => {
  const data = await parent()
  const api = usePipelineManager({ fetch })
  if (typeof data.auth === 'object' && 'login' in data.auth) {
    data.auth.login()
    await new Promise(() => {}) // Await indefinitely to avoid loading the page - until redirected to auth page
  }
  const pipelines = await api.getPipelines()
  return { ...data, preloaded: { pipelines } }
}
