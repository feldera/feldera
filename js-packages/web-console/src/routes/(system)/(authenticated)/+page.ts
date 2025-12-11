import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'

export const load = async ({ parent }) => {
  await parent()
  const api = usePipelineManager()
  const demos = await api.getDemos()
  return {
    demos
  }
}
