import { localStore } from '$lib/compositions/localStore.svelte'

export type PipelineTab = { existing: string } | { new: string }

export const useOpenPipelines = () =>
  localStore<Exclude<PipelineTab, 'pipelines'>[]>('pipelines/open', [])
