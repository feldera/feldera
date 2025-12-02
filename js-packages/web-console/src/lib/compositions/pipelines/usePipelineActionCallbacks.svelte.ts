import type {
  PipelineAction,
  PipelineStatus,
  PipelineThumb
} from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
import { usePipelineList } from './usePipelineList.svelte'
import { fsm } from '@githubnext/tiny-svelte-fsm'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete'

const callbacks: Record<string, Partial<Record<Action, Cb[]>>> = $state({})

// Helper to normalize status to string for comparison
const statusToString = (status: PipelineStatus): string => {
  return typeof status === 'string' ? status : Object.keys(status)[0]
}

// Stable states are those where pipelines can remain (not transitional)
// We ignore: Preparing, Provisioning, Initializing, Pausing, Resuming, Stopping, Bootstrapping, Replaying, etc.
type StableStatus =
  | 'Stopped'
  | 'Running'
  | 'Paused'
  | 'Standby'
  | 'Suspended'
  | 'SqlError'
  | 'RustError'
  | 'SystemError'
  | 'AwaitingApproval'
  | 'Unavailable'

type StorageStatus = PipelineThumb['storageStatus']

// FSM state is a tuple: "PipelineStatus:StorageStatus"
type FSMState = `${StableStatus}:${StorageStatus}`

// Event to signal reaching a new stable state
type FSMEvent = 'transition'

const isStableState = (status: PipelineStatus): boolean => {
  const statusStr = statusToString(status)
  const stableStates: StableStatus[] = [
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
  ]
  return stableStates.includes(statusStr as StableStatus)
}

const makeStateKey = (pipeline: PipelineThumb): FSMState => {
  return `${statusToString(pipeline.status)}:${pipeline.storageStatus}` as FSMState
}

// Parse state key back to components
const parseStateKey = (key: FSMState): { status: string; storageStatus: StorageStatus } => {
  const [status, storageStatus] = key.split(':')
  return { status, storageStatus: storageStatus as StorageStatus }
}

// Helper to check if status is Stopped or Error
const isStoppedOrError = (status: string) =>
  ['Stopped', 'SqlError', 'RustError', 'SystemError'].includes(status)

// Check which action matches a transition
const matchActionForTransition = (from: FSMState, to: FSMState): Action[] => {
  const fromState = parseStateKey(from)
  const toState = parseStateKey(to)

  const matchedActions: Action[] = []

  // Pipeline status transitions
  if (fromState.status !== toState.status) {
    // start_paused: Stopped/Error → Paused
    if (isStoppedOrError(fromState.status) && toState.status === 'Paused') {
      matchedActions.push('start_paused')
    }

    // start: Stopped/Error → Running
    if (isStoppedOrError(fromState.status) && toState.status === 'Running') {
      matchedActions.push('start')
    }

    // resume: Paused → Running
    if (fromState.status === 'Paused' && toState.status === 'Running') {
      matchedActions.push('resume')
    }

    // pause: Running → Paused
    if (fromState.status === 'Running' && toState.status === 'Paused') {
      matchedActions.push('pause')
    }

    // stop: Running/Paused → Stopped
    if (['Running', 'Paused'].includes(fromState.status) && toState.status === 'Stopped') {
      matchedActions.push('stop')
    }

    // kill: Any non-Stopped → Stopped
    if (fromState.status !== 'Stopped' && toState.status === 'Stopped') {
      matchedActions.push('kill')
    }

    // standby: Any → Standby
    if (toState.status === 'Standby') {
      matchedActions.push('standby')
    }

    // activate: Standby → Running
    if (fromState.status === 'Standby' && toState.status === 'Running') {
      matchedActions.push('activate')
    }

    // approve_changes: AwaitingApproval → Any other
    if (fromState.status === 'AwaitingApproval' && toState.status !== 'AwaitingApproval') {
      matchedActions.push('approve_changes')
    }
  }

  // Storage status transitions
  if (fromState.storageStatus !== toState.storageStatus) {
    // clear: Any → Cleared
    if (fromState.storageStatus !== 'Cleared' && toState.storageStatus === 'Cleared') {
      matchedActions.push('clear')
    }
  }

  return matchedActions
}

// Create FSM for tracking pipeline state transitions
const createPipelineFSM = (initialState: FSMState, pipelineName: string) => {
  // Use wildcard pattern to allow any state to transition to any other state
  return fsm<FSMState, FSMEvent>(initialState, {
    '*': {
      transition: (newState: FSMState) => newState,
      _enter: ({ from, to }: { from: FSMState | null; to: FSMState }) => {
        // Skip initial state entry
        if (from === null) return

        // Detect which actions this transition represents
        const actions = matchActionForTransition(from, to)

        // Execute callbacks for matched actions
        for (const action of actions) {
          const cbs = callbacks[pipelineName]?.[action] ?? []
          if (cbs.length > 0) {
            Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
          }
        }
      }
    }
  } as any) // Type assertion needed due to FSM's strict typing with dynamic states
}

// Track FSM instances per pipeline
const pipelineFSMs = $state<Record<string, ReturnType<typeof createPipelineFSM>>>({})

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

          // Clean up callbacks and FSM for deleted pipeline
          delete callbacks[deletedPipelineName]
          delete pipelineFSMs[deletedPipelineName]
        }
      }

      // Process each current pipeline
      for (const pipeline of currentPipelines) {
        const pipelineName = pipeline.name

        // Skip transitional states - only process stable states
        if (!isStableState(pipeline.status)) {
          continue
        }

        const currentStateKey = makeStateKey(pipeline)

        // Initialize FSM if this is the first time we see this pipeline (in a stable state)
        if (!pipelineFSMs[pipelineName]) {
          pipelineFSMs[pipelineName] = createPipelineFSM(currentStateKey, pipelineName)
          continue
        }

        // Send transition event to FSM - it will handle detecting the transition and executing callbacks
        pipelineFSMs[pipelineName].send('transition', currentStateKey)
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
