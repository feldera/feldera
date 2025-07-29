// export const usePipelineAction = (api: PipelineManagerApi, pipelines: () => PipelineThumb[]) => {

import type { NamesInUnion } from '$lib/functions/common/union'
import type { PipelineAction, PipelineStatus, PipelineThumb } from '$lib/services/pipelineManager'
import { usePipelineList } from './pipelines/usePipelineList.svelte'
import { usePipelineManager, type PipelineManagerApi } from './usePipelineManager.svelte'
import { useReactiveWaiter } from './useReactiveWaiter.svelte'
import { unionName } from '$lib/functions/common/union'
import { page } from '$app/state'
import { match } from 'ts-pattern'

export const usePipelineAction = () => {
  const data: { preloaded: { pipelines: PipelineThumb[] } } = page.data as any
  const api = usePipelineManager()
  const pipelineList = usePipelineList(data.preloaded)

  const ignoreStatuses: NamesInUnion<PipelineStatus>[] = [
    'Preparing',
    'Provisioning',
    'Initializing',
    'CompilingRust',
    'SqlCompiled',
    'CompilingSql',
    'Stopping',
    'Pausing',
    'Suspending',
    'Resuming',
    'Queued'
  ]
  const reactiveWaiter = useReactiveWaiter(() => pipelineList.pipelines)
  return {
    postPipelineAction: async (pipeline_name: string, action: PipelineAction) => {
      await api.postPipelineAction(pipeline_name, action)
      const isDesiredState = (
        {
          start: (p) => p.status === 'Running',
          pause: (p) => p.status === 'Paused',
          start_paused: (p) => p.status === 'Paused',
          stop: (p) => p.status === 'Stopped',
          kill: (p) => p.status === 'Stopped',
          clear: (p) => p.storageStatus === 'Cleared'
        } satisfies Record<PipelineAction, (pipeline: PipelineThumb) => boolean>
      )[action]

      const isIntermediateState = match(action)
        .with('clear', () => (pipeline: PipelineThumb) => pipeline.storageStatus === 'Clearing')
        .with(
          'start',
          'pause',
          'start_paused',
          'stop',
          'kill',
          () => (pipeline: PipelineThumb) => ignoreStatuses.includes(unionName(pipeline.status))
        )
        .exhaustive()

      return {
        waitFor: () => {
          const waiter = reactiveWaiter.createWaiter({
            predicate: (ps) => {
              const p = ps.find((p) => p.name === pipeline_name)
              if (!p) {
                throw new Error('Pipeline not found in pipelines list')
              }
              if (isIntermediateState(p)) {
                return false
              }
              if (isDesiredState(p)) {
                return true
              }
              throw new Error(
                `Unexpected status ${JSON.stringify(p.status)}/${JSON.stringify(p.storageStatus)} while waiting for pipeline ${pipeline_name} to complete action ${action}`
              )
            }
          })
          return waiter.waitFor()
        }
      }
    }
  }
}
