// export const usePipelineAction = (api: PipelineManagerApi, pipelines: () => PipelineThumb[]) => {

import type { NamesInUnion } from '$lib/functions/common/union'
import type {
  PipelineAction,
  PipelineStatus,
  PipelineThumb
} from '$lib/services/pipelineManager'
import { usePipelineList, useUpdatePipelineList } from './pipelines/usePipelineList.svelte'
import { usePipelineManager } from './usePipelineManager.svelte'
import { useReactiveWaiter } from './useReactiveWaiter.svelte'
import { unionName } from '$lib/functions/common/union'
import { page } from '$app/state'
import { match } from 'ts-pattern'

/**
 * Composition for handling pipeline actions with optimistic updates and state management.
 *
 * This composition encapsulates pipeline state transitions, including:
 * - Optimistic UI updates for immediate feedback
 * - Complex action flows (e.g., start action with hidden paused intermediate state)
 * - State synchronization between local pipeline and global pipeline list
 *
 * Supported actions:
 * - `start`: Starts pipeline normally via hidden start_paused + resume flow (with optimistic status update)
 * - `start_paused`: Starts pipeline in paused state (with optimistic status update)
 * - `pause`: Pauses running pipeline (with optimistic status update)
 * - `stop`: Gracefully stops pipeline with checkpoint (with optimistic status update)
 * - `kill`: Force stops pipeline immediately (with optimistic status update)
 * - `clear`: Clears pipeline storage and checkpoints (with optimistic storageStatus update)
 *
 * Note: To be notified when actions complete, use usePipelineActionCallbacks().add() to register callbacks.
 * The reactive callback system will automatically execute callbacks when desired states are reached.
 *
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
    'Queued',
    'AwaitingApproval',
    'Bootstrapping',
    'Replaying'
  ]
  const reactiveWaiter = useReactiveWaiter(() => pipelineList.pipelines)

  const postPipelineAction = async (
    pipeline_name: string,
    action: PipelineAction | 'resume'
  ): Promise<void> => {
      // Optimistic status update based on action
      const optimisticStatus = match(action)
        .returnType<PipelineThumb['status'] | undefined>()
        .with('start', () => 'Preparing')
        .with('resume', () => 'Resuming')
        .with('start_paused', () => 'Preparing')
        .with('pause', () => 'Pausing')
        .with('stop', 'kill', () => 'Stopping')
        .with('clear', () => undefined) // clear updates storageStatus, not status
        .with('standby', () => 'Initializing')
        .with('activate', () => 'Running')
        .with('approve_changes', () => undefined)
        .exhaustive()

      // Apply optimistic updates
      if (optimisticStatus) {
        updatePipeline(pipeline_name, (p) => ({ ...p, status: optimisticStatus }))
      } else if (action === 'clear') {
        updatePipeline(pipeline_name, (p) => ({ ...p, storageStatus: 'Clearing' }))
      }

      // Handle 'start' action with hidden paused intermediate state
      if (action === 'start') {
        // First start in paused state
        await api.postPipelineAction(pipeline_name, 'start_paused')

        // Wait for paused state
        const pausedWaiter = reactiveWaiter.createWaiter({
          predicate: (ps) => {
            const p = ps.find((p) => p.name === pipeline_name)
            if (!p) {
              throw new Error('Pipeline not found in pipelines list')
            }
            if (ignoreStatuses.includes(unionName(p.status))) {
              return null
            }
            if (
              (['Paused', 'AwaitingApproval'] satisfies PipelineStatus[]).findIndex(
                (status) => status === p.status
              ) !== -1
            ) {
              return { value: true }
            }
            if (p.status === 'Stopped') {
              return { value: false }
            }
            throw new Error(
              `Unexpected status ${JSON.stringify(p.status)} while waiting for pipeline ${pipeline_name} to reach paused state`
            )
          }
        })

        const shouldContinue = await pausedWaiter.waitFor()
        if (!shouldContinue) {
          // Pipeline was stopped before it could be resumed, don't try to resume
          return
        }

        updatePipeline(pipeline_name, (p) => ({ ...p, status: 'Initializing' }))

        // Then start normally (resume action will trigger its own callbacks reactively)
        await api.postPipelineAction(pipeline_name, 'resume')
      } else {
        await api.postPipelineAction(pipeline_name, action)
      }
    }

  return {
    postPipelineAction
  }
}
