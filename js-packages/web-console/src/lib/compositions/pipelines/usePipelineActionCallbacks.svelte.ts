import type {
  PipelineAction,
  PipelineStatus,
  PipelineThumb
} from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
import { usePipelineList } from './usePipelineList.svelte'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete'

const callbacks: Record<string, Partial<Record<Action, Cb[]>>> = $state({})

// Helper to normalize status to string for comparison
const statusToString = (status: PipelineStatus): string => {
  return typeof status === 'string' ? status : Object.keys(status)[0]
}

// Stable states are those where pipelines can remain (not transitional)
const stableStates = [
  'Stopped',
  'Running',
  'Paused',
  'Standby',
  'Suspended',
  'SqlError',
  'RustError',
  'SystemError',
  'AwaitingApproval',
  'Unavailable'
] as const

const isStableState = (status: PipelineStatus): boolean => {
  const statusStr = statusToString(status)
  return stableStates.includes(statusStr as any)
}

/**
 * Track pipeline state transitions
 *
 * This tracker records the "stable" state before a pipeline enters transitional states.
 * For example: Stopped → Preparing → Provisioning → Initializing → Paused
 * We record: lastStableState = "Stopped", and when reaching "Paused", we know the
 * transition is Stopped → Paused (start_paused action), not Running → Paused (pause action).
 *
 * Transitional states (ignored): Preparing, Provisioning, Initializing, Pausing, Resuming, Stopping, etc.
 * Stable states: Stopped, Running, Paused, Standby, Error states, etc.
 */
type PipelineTracker = {
  lastStableState: string | null
  currentState: PipelineStatus
  currentStorageStatus: PipelineThumb['storageStatus']
  lastStorageStatus: PipelineThumb['storageStatus'] | null
}

const trackers = $state<Record<string, PipelineTracker>>({})

// Define which transitions match which actions
type TransitionMatcher = (tracker: PipelineTracker, pipeline: PipelineThumb) => boolean

const isStoppedOrError = (status: string) =>
  ['Stopped', 'SqlError', 'RustError', 'SystemError'].includes(status)

const actionTransitions: Record<Action, TransitionMatcher> = {
  // start_paused: Stopped/Error → Paused (when starting in paused state)
  start_paused: (t, p) =>
    t.lastStableState !== null &&
    isStoppedOrError(t.lastStableState) &&
    statusToString(p.status) === 'Paused',

  // start: Stopped/Error → Running (normal start flow)
  start: (t, p) =>
    t.lastStableState !== null &&
    isStoppedOrError(t.lastStableState) &&
    statusToString(p.status) === 'Running',

  // resume: Paused → Running (resuming from paused state)
  resume: (t, p) =>
    t.lastStableState === 'Paused' && statusToString(p.status) === 'Running',

  // pause: Running → Paused (pausing from running state)
  pause: (t, p) =>
    t.lastStableState === 'Running' && statusToString(p.status) === 'Paused',

  // stop: Running/Paused → Stopped (graceful stop with checkpoint)
  stop: (t, p) =>
    t.lastStableState !== null &&
    ['Running', 'Paused'].includes(t.lastStableState) &&
    statusToString(p.status) === 'Stopped',

  // kill: Any non-Stopped → Stopped (force stop)
  kill: (t, p) =>
    t.lastStableState !== null &&
    t.lastStableState !== 'Stopped' &&
    statusToString(p.status) === 'Stopped',

  // standby: Any → Standby
  standby: (_t, p) => statusToString(p.status) === 'Standby',

  // activate: Standby → Running
  activate: (t, p) =>
    t.lastStableState === 'Standby' && statusToString(p.status) === 'Running',

  // approve_changes: AwaitingApproval → Any other stable state
  approve_changes: (t, p) =>
    t.lastStableState === 'AwaitingApproval' &&
    statusToString(p.status) !== 'AwaitingApproval' &&
    isStableState(p.status),

  // clear: storageStatus transitions to Cleared
  clear: (t, p) =>
    t.lastStorageStatus !== null &&
    t.lastStorageStatus !== 'Cleared' &&
    p.storageStatus === 'Cleared',

  // delete: handled separately (pipeline removed from list)
  delete: () => false
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

          // Clean up callbacks and tracker for deleted pipeline
          delete callbacks[deletedPipelineName]
          delete trackers[deletedPipelineName]
        }
      }

      // Process each current pipeline to detect state transitions
      for (const pipeline of currentPipelines) {
        const pipelineName = pipeline.name

        // Initialize tracker if not exists
        if (!trackers[pipelineName]) {
          trackers[pipelineName] = {
            lastStableState: isStableState(pipeline.status) ? statusToString(pipeline.status) : null,
            currentState: pipeline.status,
            currentStorageStatus: pipeline.storageStatus,
            lastStorageStatus: pipeline.storageStatus
          }
          continue
        }

        const tracker = trackers[pipelineName]
        const statusChanged = JSON.stringify(tracker.currentState) !== JSON.stringify(pipeline.status)
        const storageChanged = tracker.currentStorageStatus !== pipeline.storageStatus

        // Update storage status tracking
        if (storageChanged) {
          tracker.lastStorageStatus = tracker.currentStorageStatus
          tracker.currentStorageStatus = pipeline.storageStatus
        }

        // Update status tracking
        if (statusChanged) {
          const oldStatusStr = statusToString(tracker.currentState)
          const newStatusStr = statusToString(pipeline.status)

          // If transitioning from stable to another state, record the stable origin
          if (isStableState(tracker.currentState) && oldStatusStr !== newStatusStr) {
            tracker.lastStableState = oldStatusStr
          }

          // Update current state
          tracker.currentState = pipeline.status

          // If we reached a new stable state, check for matching action transitions
          if (isStableState(pipeline.status)) {
            // Check all registered callbacks for this pipeline
            for (const action in callbacks[pipelineName]) {
              const actionKey = action as Action
              if (actionKey === 'delete') continue

              const cbs = callbacks[pipelineName][actionKey] ?? []
              if (cbs.length === 0) continue

              // Check if this transition matches the action
              if (actionTransitions[actionKey](tracker, pipeline)) {
                // Execute all callbacks for this action
                // Note: Callbacks are NOT automatically removed - callers must remove them explicitly
                Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
              }
            }
          }
        }

        // Also check storage transitions (clear action)
        if (storageChanged && pipeline.storageStatus === 'Cleared') {
          const cbs = callbacks[pipelineName]?.['clear'] ?? []
          if (cbs.length > 0 && actionTransitions['clear'](tracker, pipeline)) {
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
