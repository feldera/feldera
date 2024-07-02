import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export type PipelineTab = { existing: string } | { new: string }

export const useOpenPipelines = () =>
  useLocalStorage<Exclude<PipelineTab, 'pipelines'>[]>('pipelines/open', [])

export const pipelineTabEq = (a: PipelineTab | 'pipelines', b: PipelineTab | 'pipelines') => {
  return typeof a === 'object' && typeof b === 'object'
    ? 'existing' in a && 'existing' in b && a.existing === b.existing
      ? true
      : 'new' in a && 'new' in b && a.new === b.new
    : a === 'pipelines' && b === 'pipelines'
}
