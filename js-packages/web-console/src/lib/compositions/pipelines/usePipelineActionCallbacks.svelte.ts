import type { PipelineAction, PipelineThumb } from '$lib/services/pipelineManager'
import { usePipelineList } from './usePipelineList.svelte'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete'

type PendingAction = {
  pipelineName: string
  action: Action
  isDesiredState: (pipeline: PipelineThumb) => boolean
}

const callbacks: Record<string, Partial<Record<Action, Cb[]>>> = $state({})
const pendingActions: PendingAction[] = $state([])

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
    const currentPipelineNames = new Set(currentPipelines.map(p => p.name))

    // Check for deleted pipelines
    const deletedPipelines = Array.from(previousPipelineNames).filter(
      name => !currentPipelineNames.has(name)
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
        Promise.allSettled(allDeleteCbs.map(cb => cb(deletedPipelineName)))

        // Remove pending actions for deleted pipeline
        const toRemove = pendingActions.filter(pa => pa.pipelineName === deletedPipelineName)
        toRemove.forEach(pa => {
          const idx = pendingActions.indexOf(pa)
          if (idx !== -1) {
            pendingActions.splice(idx, 1)
          }
        })
      }
    }

    // Check pending actions for state matches
    const toRemove: PendingAction[] = []
    for (const pendingAction of pendingActions) {
      const pipeline = currentPipelines.find(p => p.name === pendingAction.pipelineName)

      if (!pipeline) {
        // Pipeline not found, skip for now (might be deleted, handled above)
        continue
      }

      // Check if desired state is reached
      if (pendingAction.isDesiredState(pipeline)) {
        // Get callbacks for this action
        const cbs = callbacks[pendingAction.pipelineName]?.[pendingAction.action] ?? []

        // Execute callbacks
        Promise.allSettled(cbs.map(cb => cb(pendingAction.pipelineName)))

        // Mark for removal
        toRemove.push(pendingAction)
      }
    }

    // Remove completed pending actions
    toRemove.forEach(pa => {
      const idx = pendingActions.indexOf(pa)
      if (idx !== -1) {
        pendingActions.splice(idx, 1)
      }
    })

    // Update previous pipeline names
    previousPipelineNames = currentPipelineNames
  })

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
    registerPendingAction(
      pipelineName: string,
      action: Action,
      isDesiredState: (pipeline: PipelineThumb) => boolean
    ) {
      pendingActions.push({
        pipelineName,
        action,
        isDesiredState
      })
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
