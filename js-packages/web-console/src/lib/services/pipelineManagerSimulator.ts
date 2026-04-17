// Mock data generator for the checkpoint and transaction subsystems exposed
// through `pipelineManager.ts`. See `pipelineManagerSimulator.md` for the
// full behavioral spec.
//
// Off by default. Flip the constants below to enable; flag changes take
// effect on the next call (no rebuild needed beyond the usual dev reload).

import type {
  CheckpointActivity,
  CheckpointFailure,
  CheckpointMetadata,
  CheckpointResponse,
  CheckpointStatus,
  CommitProgressSummary,
  ControllerStatus,
  TemporarySuspendError,
  TransactionInitiators,
  TransactionStatus
} from '$lib/services/manager'

import type { CheckpointSyncResponse, CheckpointSyncStatus } from './pipelineManager'

export const SIMULATE_CHECKPOINTS = true
export const SIMULATE_TRANSACTIONS = true

const TICK_MS = 250

const CP_PERIODIC_MS = 10_000
const CP_DURATION_RANGE_MS: [number, number] = [5_000, 8_000]
const CP_FAILURE_RATE = 0.05

const SYNC_PERIODIC_MS = 120_000
const SYNC_DURATION_RANGE_MS: [number, number] = [1_000, 3_000]
const SYNC_FAILURE_RATE = 0.03

const TX_QUIESCENT_RANGE_MS: [number, number] = [5_000, 15_000]
const TX_IN_PROGRESS_RANGE_MS: [number, number] = [10_000, 15_000]
const TX_COMMIT_RANGE_MS: [number, number] = [15_000, 20_000]
const TX_RECORDS_RANGE: [number, number] = [100_000, 1_000_000]
const TX_TOTAL_OPS_RANGE: [number, number] = [15, 30]
const TX_OPS_IN_FLIGHT_RANGE: [number, number] = [1, 3]
const TX_PER_OP_RECORDS_RANGE: [number, number] = [5_000, 80_000]

const randInt = (lo: number, hi: number) => Math.floor(lo + Math.random() * (hi - lo + 1))
const randIntFromRange = ([lo, hi]: [number, number]) => randInt(lo, hi)
const isoNow = () => new Date().toISOString()

// 64-bit fingerprint sampled per pipeline; deterministic-feeling but random.
const newFingerprint = () => Math.floor(Math.random() * 2 ** 53)

// Cheap UUIDv7-ish: timestamp prefix plus random suffix.  Output passes the
// shape `xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx`.
const uuidv7Like = () => {
  const ts = Date.now()
  const tsHex = ts.toString(16).padStart(12, '0').slice(-12)
  const r = (n: number) =>
    Array.from({ length: n }, () => Math.floor(Math.random() * 16).toString(16)).join('')
  const yDigit = ((Math.floor(Math.random() * 4) + 8) & 0xf).toString(16)
  return `${tsHex.slice(0, 8)}-${tsHex.slice(8, 12)}-7${r(3)}-${yDigit}${r(3)}-${r(12)}`
}

// ---------------------------------------------------------------------------
// State

type CheckpointSimState = {
  fingerprint: number
  state: 'idle' | 'delayed' | 'in_progress'
  nextSequenceNumber: number
  pendingSequenceNumber?: number
  inProgressStartedAt?: string
  inProgressEndsAt?: number
  delayedSince?: string
  delayReasons: TemporarySuspendError[]
  nextAutoTriggerAt: number
  manualPending: boolean
  processedRecords: number
  steps: number
  checkpoints: CheckpointMetadata[]
  lastSuccess?: number
  lastFailure?: CheckpointFailure
  // Sync substate.
  syncState: 'idle' | 'in_progress'
  syncStartedAt?: number
  syncEndsAt?: number
  syncTargetUuid?: string
  syncKind: 'manual' | 'periodic'
  lastManualSuccessUuid?: string
  lastManualFailure?: { uuid: string; error: string }
  lastPeriodicSuccessUuid?: string
  nextPeriodicSyncAt: number
}

type CommitOp = {
  total: number
  done: number
  startedAt: number
  durationMs: number
}

type TransactionSimState = {
  state: TransactionStatus
  transactionId: number
  phaseStartedAt: number
  phaseEndsAt: number
  // TransactionInProgress phase.
  totalRecordsTarget: number
  recordsSoFar: number
  // CommitInProgress phase.
  totalOps: number
  opsCompleted: number
  opsInFlight: CommitOp[]
  opsRemaining: number
  // Final record count, frozen at commit entry.
  finalRecords: number
  // When NoTransaction, the time at which we should kick off a new one.
  nextStartAt: number
}

type PipelineSimState = {
  checkpoint?: CheckpointSimState
  transaction?: TransactionSimState
}

const pipelines = new Map<string, PipelineSimState>()
let tickHandle: ReturnType<typeof setInterval> | undefined

const ensurePipeline = (name: string): PipelineSimState => {
  let s = pipelines.get(name)
  if (!s) {
    s = {}
    pipelines.set(name, s)
  }
  return s
}

const initCheckpointState = (now: number): CheckpointSimState => ({
  fingerprint: newFingerprint(),
  state: 'idle',
  nextSequenceNumber: 1,
  delayReasons: [],
  nextAutoTriggerAt: now + CP_PERIODIC_MS,
  manualPending: false,
  processedRecords: 0,
  steps: 0,
  checkpoints: [],
  syncState: 'idle',
  syncKind: 'periodic',
  nextPeriodicSyncAt: now + SYNC_PERIODIC_MS
})

const initTransactionState = (now: number): TransactionSimState => ({
  state: 'NoTransaction',
  transactionId: 0,
  phaseStartedAt: now,
  phaseEndsAt: now + randIntFromRange(TX_QUIESCENT_RANGE_MS),
  totalRecordsTarget: 0,
  recordsSoFar: 0,
  totalOps: 0,
  opsCompleted: 0,
  opsInFlight: [],
  opsRemaining: 0,
  finalRecords: 0,
  nextStartAt: now + randIntFromRange(TX_QUIESCENT_RANGE_MS)
})

const ensureCheckpoint = (name: string): CheckpointSimState => {
  const s = ensurePipeline(name)
  if (!s.checkpoint) {
    s.checkpoint = initCheckpointState(Date.now())
  }
  return s.checkpoint
}

const ensureTransaction = (name: string): TransactionSimState => {
  const s = ensurePipeline(name)
  if (!s.transaction) {
    s.transaction = initTransactionState(Date.now())
  }
  return s.transaction
}

// ---------------------------------------------------------------------------
// Tick driver

const startTickIfNeeded = () => {
  if (tickHandle !== undefined) return
  if (!SIMULATE_CHECKPOINTS && !SIMULATE_TRANSACTIONS) return
  tickHandle = setInterval(() => tickAll(Date.now()), TICK_MS)
}

const tickAll = (now: number) => {
  for (const [, state] of pipelines) {
    if (SIMULATE_TRANSACTIONS && state.transaction) {
      tickTransaction(state.transaction, now)
    }
    if (SIMULATE_CHECKPOINTS && state.checkpoint) {
      tickCheckpoint(state.checkpoint, state.transaction, now)
    }
  }
}

// ---------------------------------------------------------------------------
// Checkpoint state machine

const startCheckpoint = (cp: CheckpointSimState, now: number) => {
  cp.state = 'in_progress'
  cp.inProgressStartedAt = new Date(now).toISOString()
  cp.inProgressEndsAt = now + randIntFromRange(CP_DURATION_RANGE_MS)
  cp.delayedSince = undefined
  cp.delayReasons = []
  cp.pendingSequenceNumber = cp.nextSequenceNumber
}

const enterDelayed = (cp: CheckpointSimState, now: number, reasons: TemporarySuspendError[]) => {
  cp.state = 'delayed'
  cp.delayedSince = cp.delayedSince ?? new Date(now).toISOString()
  cp.delayReasons = reasons
}

const finishCheckpoint = (cp: CheckpointSimState, now: number) => {
  const seq = cp.pendingSequenceNumber ?? cp.nextSequenceNumber
  if (Math.random() < CP_FAILURE_RATE) {
    cp.lastFailure = {
      sequence_number: seq,
      failed_at: new Date(now).toISOString(),
      error: 'Simulated checkpoint failure: storage backend reported transient I/O error.'
    }
  } else {
    cp.processedRecords += randInt(40_000, 60_000)
    cp.steps += randInt(800, 1_200)
    cp.checkpoints.push({
      uuid: uuidv7Like(),
      fingerprint: cp.fingerprint,
      processed_records: cp.processedRecords,
      steps: cp.steps,
      size: randInt(5, 25) * 1024 * 1024 + randInt(0, 1024 * 1024)
    })
    cp.lastSuccess = seq
  }
  cp.nextSequenceNumber = seq + 1
  cp.pendingSequenceNumber = undefined
  cp.inProgressStartedAt = undefined
  cp.inProgressEndsAt = undefined
  cp.state = 'idle'
  cp.nextAutoTriggerAt = now + CP_PERIODIC_MS
}

const transactionBlocking = (tx: TransactionSimState | undefined): boolean =>
  SIMULATE_TRANSACTIONS && !!tx && tx.state !== 'NoTransaction'

const tickCheckpoint = (
  cp: CheckpointSimState,
  tx: TransactionSimState | undefined,
  now: number
) => {
  // Sync substate is independent of the create state.
  tickSync(cp, now)

  switch (cp.state) {
    case 'idle': {
      const wantsTrigger = cp.manualPending || now >= cp.nextAutoTriggerAt
      if (!wantsTrigger) return
      cp.manualPending = false
      if (transactionBlocking(tx)) {
        enterDelayed(cp, now, ['TransactionInProgress'])
      } else {
        startCheckpoint(cp, now)
      }
      return
    }
    case 'delayed': {
      if (!transactionBlocking(tx)) {
        startCheckpoint(cp, now)
      }
      return
    }
    case 'in_progress': {
      if (cp.inProgressEndsAt !== undefined && now >= cp.inProgressEndsAt) {
        finishCheckpoint(cp, now)
      }
      return
    }
  }
}

// ---------------------------------------------------------------------------
// Sync substate

const tickSync = (cp: CheckpointSimState, now: number) => {
  if (cp.syncState === 'in_progress') {
    if (cp.syncEndsAt !== undefined && now >= cp.syncEndsAt) {
      finishSync(cp, now)
    }
    return
  }
  // idle: maybe kick off a periodic sync.
  if (now < cp.nextPeriodicSyncAt) return
  if (cp.checkpoints.length === 0) {
    cp.nextPeriodicSyncAt = now + SYNC_PERIODIC_MS
    return
  }
  const latest = cp.checkpoints[cp.checkpoints.length - 1]!
  if (cp.lastPeriodicSuccessUuid === latest.uuid) {
    cp.nextPeriodicSyncAt = now + SYNC_PERIODIC_MS
    return
  }
  startSync(cp, now, latest.uuid, 'periodic')
}

const startSync = (
  cp: CheckpointSimState,
  now: number,
  uuid: string,
  kind: 'manual' | 'periodic'
) => {
  cp.syncState = 'in_progress'
  cp.syncStartedAt = now
  cp.syncEndsAt = now + randIntFromRange(SYNC_DURATION_RANGE_MS)
  cp.syncTargetUuid = uuid
  cp.syncKind = kind
}

const finishSync = (cp: CheckpointSimState, now: number) => {
  const uuid = cp.syncTargetUuid!
  const failed = Math.random() < SYNC_FAILURE_RATE
  if (failed) {
    if (cp.syncKind === 'manual') {
      cp.lastManualFailure = {
        uuid,
        error: 'Simulated sync failure: object store returned 5xx (transient).'
      }
    }
    // Periodic failures are not surfaced via /sync_status by design.
  } else {
    if (cp.syncKind === 'manual') {
      cp.lastManualSuccessUuid = uuid
      cp.lastManualFailure = undefined
    } else {
      cp.lastPeriodicSuccessUuid = uuid
    }
  }
  cp.syncState = 'idle'
  cp.syncStartedAt = undefined
  cp.syncEndsAt = undefined
  cp.syncTargetUuid = undefined
  cp.nextPeriodicSyncAt = now + SYNC_PERIODIC_MS
}

// ---------------------------------------------------------------------------
// Transaction state machine

const opDurationMs = (tx: TransactionSimState): number => {
  const phaseLength = Math.max(1, tx.phaseEndsAt - tx.phaseStartedAt)
  const inFlight = Math.max(1, randIntFromRange(TX_OPS_IN_FLIGHT_RANGE))
  // Pace ops so the whole batch finishes inside the commit window.
  return Math.max(500, Math.floor((phaseLength * inFlight) / Math.max(1, tx.totalOps)))
}

const pickInFlightOps = (tx: TransactionSimState, now: number) => {
  const desired = randIntFromRange(TX_OPS_IN_FLIGHT_RANGE)
  while (tx.opsInFlight.length < desired && tx.opsRemaining > 0) {
    tx.opsInFlight.push({
      total: randIntFromRange(TX_PER_OP_RECORDS_RANGE),
      done: 0,
      startedAt: now,
      durationMs: opDurationMs(tx)
    })
    tx.opsRemaining--
  }
}

const enterTransactionInProgress = (tx: TransactionSimState, now: number) => {
  tx.state = 'TransactionInProgress'
  tx.transactionId += 1
  tx.phaseStartedAt = now
  tx.phaseEndsAt = now + randIntFromRange(TX_IN_PROGRESS_RANGE_MS)
  tx.totalRecordsTarget = randIntFromRange(TX_RECORDS_RANGE)
  tx.recordsSoFar = 0
}

const enterCommitInProgress = (tx: TransactionSimState, now: number) => {
  tx.state = 'CommitInProgress'
  tx.phaseStartedAt = now
  tx.phaseEndsAt = now + randIntFromRange(TX_COMMIT_RANGE_MS)
  tx.finalRecords = tx.totalRecordsTarget
  tx.recordsSoFar = tx.totalRecordsTarget
  tx.totalOps = randIntFromRange(TX_TOTAL_OPS_RANGE)
  tx.opsCompleted = 0
  tx.opsInFlight = []
  tx.opsRemaining = tx.totalOps
  pickInFlightOps(tx, now)
}

const enterNoTransaction = (tx: TransactionSimState, now: number) => {
  tx.state = 'NoTransaction'
  tx.transactionId = 0
  tx.phaseStartedAt = now
  tx.totalRecordsTarget = 0
  tx.recordsSoFar = 0
  tx.totalOps = 0
  tx.opsCompleted = 0
  tx.opsInFlight = []
  tx.opsRemaining = 0
  tx.finalRecords = 0
  tx.nextStartAt = now + randIntFromRange(TX_QUIESCENT_RANGE_MS)
  tx.phaseEndsAt = tx.nextStartAt
}

const tickTransaction = (tx: TransactionSimState, now: number) => {
  switch (tx.state) {
    case 'NoTransaction': {
      if (now >= tx.nextStartAt) {
        enterTransactionInProgress(tx, now)
      }
      return
    }
    case 'TransactionInProgress': {
      const length = Math.max(1, tx.phaseEndsAt - tx.phaseStartedAt)
      const p = Math.min(1, (now - tx.phaseStartedAt) / length)
      // ease-out: 1 - (1 - p)^2
      const eased = 1 - (1 - p) * (1 - p)
      tx.recordsSoFar = Math.floor(tx.totalRecordsTarget * eased)
      if (now >= tx.phaseEndsAt) {
        enterCommitInProgress(tx, now)
      }
      return
    }
    case 'CommitInProgress': {
      // Each in-flight op advances linearly from its start time toward its
      // own duration. When an op completes, retire it and pull a fresh one
      // off the remaining queue.
      for (const op of tx.opsInFlight) {
        const p = Math.min(1, (now - op.startedAt) / Math.max(1, op.durationMs))
        op.done = Math.max(op.done, Math.floor(op.total * p))
      }
      tx.opsInFlight = tx.opsInFlight.filter((op) => {
        if (op.done >= op.total) {
          tx.opsCompleted++
          return false
        }
        return true
      })
      pickInFlightOps(tx, now)
      if (now >= tx.phaseEndsAt) {
        // Time's up: snap remaining work to done and end the phase.
        tx.opsCompleted += tx.opsInFlight.length + tx.opsRemaining
        tx.opsInFlight = []
        tx.opsRemaining = 0
        enterNoTransaction(tx, now)
      }
      return
    }
  }
}

// ---------------------------------------------------------------------------
// Public read API

const cloneActivity = (cp: CheckpointSimState): CheckpointActivity => {
  switch (cp.state) {
    case 'idle':
      return { status: 'idle' }
    case 'delayed':
      return {
        status: 'delayed',
        delayed_since: cp.delayedSince ?? isoNow(),
        reasons: cp.delayReasons.slice()
      }
    case 'in_progress':
      return { status: 'in_progress', started_at: cp.inProgressStartedAt ?? isoNow() }
  }
}

export const simulateGetPipelineCheckpoints = (
  pipelineName: string
): Promise<CheckpointMetadata[]> => {
  startTickIfNeeded()
  const cp = ensureCheckpoint(pipelineName)
  return Promise.resolve(cp.checkpoints.map((c) => ({ ...c })))
}

export const simulateCheckpointPipeline = (pipelineName: string): Promise<CheckpointResponse> => {
  startTickIfNeeded()
  const cp = ensureCheckpoint(pipelineName)
  const now = Date.now()
  if (cp.state === 'idle') {
    // Trigger immediately. Defer to the tx-aware path so that an in-progress
    // transaction still pushes us into `delayed`.
    cp.manualPending = true
    cp.nextAutoTriggerAt = now
  } else {
    cp.manualPending = true
  }
  const seq = cp.pendingSequenceNumber ?? cp.nextSequenceNumber
  return Promise.resolve({ checkpoint_sequence_number: seq })
}

export const simulateGetCheckpointStatus = (pipelineName: string): Promise<CheckpointStatus> => {
  startTickIfNeeded()
  const cp = ensureCheckpoint(pipelineName)
  return Promise.resolve({
    success: cp.lastSuccess ?? null,
    failure: cp.lastFailure ? { ...cp.lastFailure } : null
  })
}

export const simulateSyncCheckpoint = (pipelineName: string): Promise<CheckpointSyncResponse> => {
  startTickIfNeeded()
  const cp = ensureCheckpoint(pipelineName)
  const now = Date.now()
  if (cp.syncState === 'in_progress' && cp.syncTargetUuid) {
    return Promise.resolve({ checkpoint_uuid: cp.syncTargetUuid })
  }
  if (cp.checkpoints.length === 0) {
    return Promise.reject(new Error('No checkpoint exists yet to sync.'))
  }
  // Prefer the latest successful checkpoint; fall back to the latest entry.
  const latestSuccess = cp.lastSuccess
    ? cp.checkpoints[cp.checkpoints.length - 1]
    : cp.checkpoints[cp.checkpoints.length - 1]
  startSync(cp, now, latestSuccess!.uuid, 'manual')
  return Promise.resolve({ checkpoint_uuid: latestSuccess!.uuid })
}

export const simulateGetCheckpointSyncStatus = (
  pipelineName: string
): Promise<CheckpointSyncStatus> => {
  startTickIfNeeded()
  const cp = ensureCheckpoint(pipelineName)
  return Promise.resolve({
    success: cp.lastManualSuccessUuid ?? null,
    failure: cp.lastManualFailure ? { ...cp.lastManualFailure } : null,
    periodic: cp.lastPeriodicSuccessUuid ?? null
  })
}

// ---------------------------------------------------------------------------
// Overlay onto a real getPipelineStats result

const buildCommitProgress = (tx: TransactionSimState): CommitProgressSummary => {
  const inProgressTotal = tx.opsInFlight.reduce((acc, op) => acc + op.total, 0)
  const inProgressDone = tx.opsInFlight.reduce((acc, op) => acc + op.done, 0)
  return {
    completed: tx.opsCompleted,
    in_progress: tx.opsInFlight.length,
    in_progress_processed_records: inProgressDone,
    in_progress_total_records: inProgressTotal,
    remaining: tx.opsRemaining
  }
}

const buildInitiators = (tx: TransactionSimState): TransactionInitiators => {
  switch (tx.state) {
    case 'NoTransaction':
      return { initiated_by_connectors: {}, transaction_id: null }
    case 'TransactionInProgress':
      return {
        initiated_by_api: 'Started',
        initiated_by_connectors: {},
        transaction_id: tx.transactionId
      }
    case 'CommitInProgress':
      return {
        initiated_by_api: 'Committed',
        initiated_by_connectors: {},
        transaction_id: tx.transactionId
      }
  }
}

const overlayTransactionFields = (
  status: ControllerStatus,
  tx: TransactionSimState,
  now: number
): ControllerStatus => {
  const records =
    tx.state === 'NoTransaction'
      ? null
      : tx.state === 'CommitInProgress'
        ? tx.finalRecords
        : tx.recordsSoFar
  const msecs = tx.state === 'NoTransaction' ? null : Math.max(0, now - tx.phaseStartedAt)
  return {
    ...status,
    global_metrics: {
      ...status.global_metrics,
      transaction_status: tx.state,
      transaction_id: tx.transactionId,
      transaction_msecs: msecs,
      transaction_records: records,
      transaction_initiators: buildInitiators(tx),
      commit_progress: tx.state === 'CommitInProgress' ? buildCommitProgress(tx) : null
    }
  }
}

const overlayCheckpointFields = (
  status: ControllerStatus,
  cp: CheckpointSimState
): ControllerStatus => ({
  ...status,
  checkpoint_activity: cloneActivity(cp)
})

export const overlayPipelineStats = <
  R extends { pipelineName: string; status: ControllerStatus | null | 'not running' }
>(
  result: R
): R => {
  if (!SIMULATE_CHECKPOINTS && !SIMULATE_TRANSACTIONS) return result
  startTickIfNeeded()
  if (result.status === null || result.status === 'not running') return result
  let next: ControllerStatus = result.status
  const pipelineName = result.pipelineName
  if (SIMULATE_TRANSACTIONS) {
    next = overlayTransactionFields(next, ensureTransaction(pipelineName), Date.now())
  }
  if (SIMULATE_CHECKPOINTS) {
    next = overlayCheckpointFields(next, ensureCheckpoint(pipelineName))
  }
  return { ...result, status: next }
}
