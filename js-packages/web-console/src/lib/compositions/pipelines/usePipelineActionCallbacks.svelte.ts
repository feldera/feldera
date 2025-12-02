import type { PipelineAction, PipelineStatus, PipelineThumb } from '$lib/services/pipelineManager'
import { untrack } from 'svelte'
import { usePipelineList } from './usePipelineList.svelte'
import { fsm } from '@githubnext/tiny-svelte-fsm'
import { match, P } from 'ts-pattern'

type Cb = (pipelineName: string) => Promise<void>

type Action = PipelineAction | 'delete' | 'rename'

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

const isRunning = (status: string) => ['Running', 'Paused'].includes(status)

/**
 * Check if an action matches a transition
 */
const matchActionForTransition = (from: FSMState, to: FSMState): Action[] => {
  const fromState = parseStateKey(from)
  const toState = parseStateKey(to)

  const matchedActions: Action[] = []

  // Pipeline status transitions
  matchedActions.push(
    ...match([fromState.status, toState.status])
      .returnType<Action[]>()
      .when(
        ([from, to]) => from === to,
        () => []
      )
      .with([P.when(isStoppedOrError), 'Paused'], () => ['start_paused'])
      .with([P.when(isStoppedOrError), 'Running'], () => ['start'])
      .with(['Paused', 'Running'], () => ['resume'])
      .with(['Running', 'Paused'], () => ['pause'])
      .with([P.when(isRunning), 'Stopped'], () => ['stop'])
      .with([P.not('Stopped'), 'Stopped'], () => ['kill'])
      .with([P.any, 'Standby'], () => ['standby'])
      .with(['Standby', 'Running'], () => ['activate'])
      .with(['AwaitingApproval', P.when(isRunning)], () => ['approve_changes'])
      .otherwise(() => [])
  )

  // Storage status transitions
  matchedActions.push(
    ...match([fromState.storageStatus, toState.storageStatus])
      .returnType<Action[]>()
      .when(
        ([from, to]) => from === to,
        () => []
      )
      .with([P.not('Cleared'), 'Cleared'], () => ['clear'])
      .otherwise(() => [])
  )

  return matchedActions
}

/**
 * Create FSM for tracking pipeline state transitions
 */
const createPipelineFSM = (initialState: FSMState, pipelineName: string) => {
  // Use wildcard pattern to allow any state to transition to any other state
  return fsm<FSMState, FSMEvent>(initialState, {
    '*': {
      transition: (newState: FSMState) => newState,
      _enter: ({ from, to }: { from: FSMState | null; to: FSMState }) => {
        // Skip initial state entry
        if (from === null) {
          return
        }

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

/**
 * Reactive composition for monitoring pipeline state transitions and executing callbacks.
 *
 * Tracks stable state transitions (Stopped → Running, Running → Paused, etc.) and automatically
 * executes registered callbacks when actions complete. Transitional states (Preparing, Provisioning,
 * etc.) are ignored.
 *
 * - Registered callbacks are NOT automatically removed - callers control lifecycle
 * - Uses FSM per pipeline to track state transitions and derive actions based on that
 * - Detects pipeline deletions (by ID) and executes 'delete' callbacks
 * - Handles pipeline renames by migrating callbacks and FSM state, and executes 'rename' callbacks
 *
 * @param preloaded - Optional preloaded pipeline data
 */
export function usePipelineActionCallbacks(preloaded?: { pipelines: PipelineThumb[] }) {
  const pipelineList = usePipelineList(preloaded)

  // Track previous pipelines by ID to detect deletions and renames
  let previousPipelinesById = $state<Map<string, PipelineThumb>>(new Map())

  // Reactive effect to monitor pipeline changes and trigger callbacks
  $effect(() => {
    const currentPipelines = pipelineList.pipelines
    untrack(() => {
      const currentPipelinesById = new Map(currentPipelines.map((p) => [p.id, p]))

      // Only process changes if we had pipelines before (not initial load)
      // Check for deleted and renamed pipelines
      for (const [id, oldPipeline] of previousPipelinesById) {
        const currentPipeline = currentPipelinesById.get(id)

        if (!currentPipeline) {
          // Pipeline deleted (ID no longer exists)
          const oldName = oldPipeline.name

          // Call delete callbacks for specific pipeline
          const specificCbs = callbacks[oldName]?.['delete'] ?? []
          // Call global delete callbacks
          const globalCbs = callbacks['']?.['delete'] ?? []
          const allDeleteCbs = [...specificCbs, ...globalCbs]

          // Execute callbacks
          Promise.allSettled(allDeleteCbs.map((cb) => cb(oldName)))

          // Clean up callbacks and FSM for deleted pipeline
          delete callbacks[oldName]
          delete pipelineFSMs[oldName]
        } else if (oldPipeline.name !== currentPipeline.name) {
          // Pipeline renamed (same ID, different name)
          const oldName = oldPipeline.name
          const newName = currentPipeline.name

          // Call rename callbacks for specific pipeline
          const specificCbs = callbacks[oldName]?.['rename'] ?? []
          // Call global rename callbacks
          const globalCbs = callbacks['']?.['rename'] ?? []
          const allRenameCbs = [...specificCbs, ...globalCbs]

          // Execute callbacks (passing the old name)
          Promise.allSettled(allRenameCbs.map((cb) => cb(oldName)))

          // Migrate callbacks from old name to new name (excluding rename callbacks)
          if (callbacks[oldName]) {
            callbacks[newName] = callbacks[oldName]
            delete callbacks[oldName]
          }

          // Migrate FSM from old name to new name
          if (pipelineFSMs[oldName]) {
            pipelineFSMs[newName] = pipelineFSMs[oldName]
            delete pipelineFSMs[oldName]
          }
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

      // Update previous pipelines
      previousPipelinesById = currentPipelinesById
    })
  })
}

/**
 * Get API for managing pipeline action callbacks.
 *
 * Provides methods to register, remove, and query callbacks that execute when pipeline
 * actions complete (detected via state transitions).
 *
 * Use empty string `''` as pipelineName for global callbacks (supported for 'delete' and 'rename' actions).
 *
 * @returns Callback management API with add, remove, pop, popAll, and getAll methods
 *
 * @example
 * ```ts
 * const api = getPipelineActionCallbacks()
 *
 * // Register callback for when pipeline starts
 * const callback = async (name) => console.log(`${name} started`)
 * api.add('my-pipeline', 'start', callback)
 *
 * // Global rename callback (receives old pipeline name)
 * api.add('', 'rename', async (oldName) => console.log(`Pipeline ${oldName} was renamed`))
 *
 * // Remove callback when done
 * api.remove('my-pipeline', 'start', callback)
 * ```
 */
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
