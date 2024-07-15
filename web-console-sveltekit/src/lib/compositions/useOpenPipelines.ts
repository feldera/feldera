import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export type PipelineTab = { existing: string } | { new: string }

// TODO: currently unused

export const useOpenPipelines = () =>
  useLocalStorage<Exclude<PipelineTab, 'pipelines'>[]>('pipelines/open', [])

export const pipelineTabEq = (a: PipelineTab | 'pipelines', b: PipelineTab | 'pipelines') => {
  return typeof a === 'object' && typeof b === 'object'
    ? 'existing' in a && 'existing' in b && a.existing === b.existing
      ? true
      : 'new' in a && 'new' in b && a.new === b.new
    : a === 'pipelines' && b === 'pipelines'
}

// const dropOpenPipeline = (pipelineTab: PipelineTab) => {
//   openPipelines.value.splice(
//     openPipelines.value.findIndex((name) => pipelineTabEq(name, pipelineTab)),
//     1
//   )
// }
// const renamePipelineTab = (oldTab: PipelineTab, newTab: PipelineTab) => {
//   const idx = openPipelines.value.findIndex((name) => pipelineTabEq(name, oldTab))
//   if (idx === -1) {
//     return
//   }
//   openPipelines.value.splice(idx, 1, newTab)
// }
