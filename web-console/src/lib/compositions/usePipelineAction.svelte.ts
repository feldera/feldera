// export const usePipelineAction = (api: PipelineManagerApi, pipelines: () => PipelineThumb[]) => {

import type { NamesInUnion } from '$lib/functions/common/union'
import type {
  ExtendedPipeline,
  PipelineAction,
  PipelineStatus,
  PipelineThumb
} from '$lib/services/pipelineManager'
import { usePipelineList, useUpdatePipelineList } from './pipelines/usePipelineList.svelte'
import { usePipelineManager, type PipelineManagerApi } from './usePipelineManager.svelte'
import { useReactiveWaiter } from './useReactiveWaiter.svelte'
import { unionName } from '$lib/functions/common/union'
import { page } from '$app/state'
import { match } from 'ts-pattern'

/**
 * Composition for handling pipeline actions with optimistic updates and state management.
 *
 * This composition encapsulates all the complexity of pipeline state transitions, including:
 * - Optimistic UI updates for immediate feedback
 * - Complex action flows (e.g., start action with hidden paused intermediate state)
 * - State synchronization between local pipeline and global pipeline list
 * - Reactive waiting for target states with proper error handling
 * - Automatic determination of intermediate and final states for each action
 *
 * Supported actions:
 * - `start`: Starts pipeline normally or resumes from paused (with optimistic status update)
 * - `start_paused`: Starts pipeline in paused state (with optimistic status update)
 * - `pause`: Pauses running pipeline (with optimistic status update)
 * - `stop`: Gracefully stops pipeline with checkpoint (with optimistic status update)
 * - `kill`: Force stops pipeline immediately (with optimistic status update)
 * - `clear`: Clears pipeline storage and checkpoints (with optimistic storageStatus update)
 *
 * Special handling:
 * - The `start` action can include hidden paused intermediate state logic when callbacks are provided
 * - All actions apply optimistic updates immediately for responsive UI
 * - Returns a waiter that resolves when the action reaches its target state
 *
 * @param pipeline - Pipeline object with current state and optimistic update function
 * @returns Object with postPipelineAction function for executing actions
 */
export const usePipelineAction = () => {
  const data: { preloaded: { pipelines: PipelineThumb[] } } = page.data as any
  const api = usePipelineManager()
  const pipelineList = usePipelineList(data.preloaded)
  const { updatePipeline } = useUpdatePipelineList()

  const ignoreStatuses: NamesInUnion<PipelineStatus>[] = [
    'Preparing',
    'Provisioning',
    'Initializing',
    'CompilingRust',
    'SqlCompiled',
    'CompilingSql',
    'Stopping',
    'Pausing',
    'Resuming',
    'Queued'
  ]
  const reactiveWaiter = useReactiveWaiter(() => pipelineList.pipelines)
  return {
    postPipelineAction: async (
      pipeline_name: string,
      action: PipelineAction | 'resume',
      callbacks?: {
        onPausedReady?: (pipelineName: string) => Promise<void>
      }
    ) => {
      // Optimistic status update based on action
      const optimisticStatus = match(action)
        // .with('start', () => (pipeline.current.status === 'Paused' ? 'Resuming' : 'Preparing'))
        .with('start', () => 'Preparing' as const)
        .with('resume', () => 'Resuming' as const)
        .with('start_paused', () => 'Preparing' as const)
        .with('pause', () => 'Pausing' as const)
        .with('stop', 'kill', () => 'Stopping' as const)
        .with('clear', () => undefined) // clear updates storageStatus, not status
        .with('standby', () => 'Initializing' as const)
        .with('activate', () => 'Running' as const)
        .exhaustive()

      // Apply optimistic updates
      if (optimisticStatus) {
        // pipeline.optimisticUpdate({ status: optimisticStatus })
        updatePipeline(pipeline_name, (p) => ({ ...p, status: optimisticStatus }))
      } else if (action === 'clear') {
        // pipeline.optimisticUpdate({ storageStatus: 'Clearing' })
        updatePipeline(pipeline_name, (p) => ({ ...p, storageStatus: 'Clearing' }))
      }

      // Handle 'start' action with hidden paused intermediate state
      if (action === 'start') {
        // First start in paused state
        await api.postPipelineAction(pipeline_name, 'start_paused')

        // Wait for paused state and run callbacks
        const pausedWaiter = reactiveWaiter.createWaiter({
          predicate: (ps) => {
            const p = ps.find((p) => p.name === pipeline_name)
            if (!p) {
              throw new Error('Pipeline not found in pipelines list')
            }
            if (ignoreStatuses.includes(unionName(p.status))) {
              return false
            }
            if (p.status === 'Paused') {
              return true
            }
            throw new Error(
              `Unexpected status ${JSON.stringify(p.status)} while waiting for pipeline ${pipeline_name} to reach paused state`
            )
          }
        })

        await pausedWaiter.waitFor()
        updatePipeline(pipeline_name, (p) => ({ ...p, status: 'Initializing' }))
        await callbacks?.onPausedReady?.(pipeline_name)

        // Then start normally
        await api.postPipelineAction(pipeline_name, 'resume')
      } else {
        await api.postPipelineAction(pipeline_name, action)
      }

      const isDesiredState = (
        {
          start: (p) => p.status === 'Running',
          resume: (p) => p.status === 'Running',
          pause: (p) => p.status === 'Paused',
          start_paused: (p) => p.status === 'Paused',
          stop: (p) => p.status === 'Stopped',
          kill: (p) => p.status === 'Stopped',
          clear: (p) => p.storageStatus === 'Cleared',
          standby: (p) => p.status === 'Standby',
          activate: (p) => p.status === 'Running'
        } satisfies Record<PipelineAction | 'resume', (pipeline: PipelineThumb) => boolean>
      )[action]

      const isIntermediateState = match(action)
        .with('clear', () => (pipeline: PipelineThumb) => pipeline.storageStatus === 'Clearing')
        .with(
          'start',
          'resume',
          'pause',
          'start_paused',
          'stop',
          'kill',
          'standby',
          'activate',
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
