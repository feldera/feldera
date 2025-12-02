import type { PipelineAction, PipelineThumb } from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
import { usePipelineList } from './usePipelineList.svelte'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete'

const callbacks: Record<string, Partial<Record<Action, Cb[]>>> = $state({})

// Map of action types to their desired state predicates
const isDesiredState: Record<Action, (pipeline: PipelineThumb) => boolean> = {
  start: (p) => p.status === 'Running',
  resume: (p) => p.status === 'Running',
  pause: (p) => p.status === 'Paused',
  start_paused: (p) => p.status === 'Paused',
  stop: (p) => p.status === 'Stopped',
  kill: (p) => p.status === 'Stopped',
  clear: (p) => p.storageStatus === 'Cleared',
  standby: (p) => p.status === 'Standby',
  activate: (p) => p.status === 'Running',
  approve_changes: (p) => p.status !== 'AwaitingApproval',
  delete: () => false // Never check for desired state on delete - handled separately
}

const pop = (pipelineName: string, action: Action) => {
  callbacks[pipelineName] ??= {}
  callbacks[pipelineName][action] ??= []
  return callbacks[pipelineName][action].pop()
}

export function usePipelineActionCallbacks(preloaded?: { pipelines: PipelineThumb[] }) {
  const pipelineList = usePipelineList(preloaded)

  // Track previous pipeline names to detect deletions
  let previousPipelineNames = $state<Set<string>>(new Set())

  // Reactive effect to monitor pipeline changes and trigger callbacks
  $effect(() => {
    const currentPipelines = pipelineList.pipelines
    untrack(() => {
      const currentPipelineNames = new Set(currentPipelines.map((p) => p.name))

      // Check for deleted pipelines
      const deletedPipelines = Array.from(previousPipelineNames).filter(
        (name) => !currentPipelineNames.has(name)
      )

      // Only process deletions if we had pipelines before (not initial load)
      if (previousPipelineNames.size > 0) {
        for (const deletedPipelineName of deletedPipelines) {
          // Call delete callbacks for specific pipeline
          const specificCbs = callbacks[deletedPipelineName]?.['delete'] ?? []
          // Call global delete callbacks
          const globalCbs = callbacks['']?.['delete'] ?? []
          const allDeleteCbs = [...specificCbs, ...globalCbs]

          // Execute callbacks
          Promise.allSettled(allDeleteCbs.map((cb) => cb(deletedPipelineName)))

          // Clean up callbacks for deleted pipeline
          delete callbacks[deletedPipelineName]
        }
      }

      // Check all registered callbacks to see if their desired states are reached
      for (const pipelineName in callbacks) {
        if (pipelineName === '') continue // Skip global callbacks (only used for delete)

        const pipeline = currentPipelines.find((p) => p.name === pipelineName)
        if (!pipeline) continue // Pipeline not found

        for (const action in callbacks[pipelineName]) {
          const actionKey = action as Action
          if (actionKey === 'delete') continue // Delete handled above

          const cbs = callbacks[pipelineName][actionKey] ?? []
          if (cbs.length === 0) continue

          // Check if desired state is reached
          if (isDesiredState[actionKey](pipeline)) {
            // Execute all callbacks for this action
            // Note: Callbacks are NOT automatically removed - callers must remove them explicitly
            // callbacks[pipelineName][actionKey] = []
            Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
          }
        }
      }

      // Update previous pipeline names
      previousPipelineNames = currentPipelineNames
    })
  })
}

export function getPipelineActionCallbacks() {
  return {
    add(pipelineName: string, action: Action, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      callbacks[pipelineName][action].push(callback)
    },
    remove(pipelineName: string, action: Action, callback: Cb) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      const idx = callbacks[pipelineName][action].findIndex((cb) => cb === callback)
      if (idx === -1) {
        return
      }
      callbacks[pipelineName][action].splice(idx, 1)
    },
    pop,
    popIterator: (pipelineName: string, action: Action) => ({
      [Symbol.iterator](): Iterator<Cb> {
        return {
          next: (): IteratorResult<Cb> => {
            const value = pop(pipelineName, action)
            if (value !== undefined) {
              return { value, done: false }
            } else {
              return { value: undefined as any, done: true }
            }
          }
        }
      }
    }),
    popAll(pipelineName: string, action: Action) {
      callbacks[pipelineName] ??= {}
      callbacks[pipelineName][action] ??= []
      return callbacks[pipelineName][action]?.splice(0)
    },
    getAll(pipelineName: string, action: Action) {
      return callbacks[pipelineName]?.[action] ?? []
    }
  }
}
