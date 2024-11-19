import { getDemos } from '$lib/services/pipelineManager'

export const load = async ({ parent }) => {
  await parent()
  return {
    demos: await getDemos()
  }
}
