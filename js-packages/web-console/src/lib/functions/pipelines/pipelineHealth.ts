import type {
  PipelineMonitorEventSelectedInfo,
  ProgramStatus,
  ResourcesStatus
} from '$lib/services/manager'
import type { HealthEventBucket } from './health'

const PROGRAM_ERROR_STATUSES: ReadonlyArray<ProgramStatus> = [
  'SystemError',
  'SqlError',
  'RustError'
]
const TRANSITIONING_RESOURCE_STATUSES: ReadonlyArray<ResourcesStatus> = ['Provisioning', 'Stopping']

export type PipelineHealthStatus = 'healthy' | 'unhealthy' | 'major_issue' | 'transitioning'
export type PipelineEventTag = 'pipeline'
export type PipelineHealthBucket = HealthEventBucket<PipelineHealthStatus, PipelineEventTag>

export type IncidentKey = {
  runtimeUnavailable: boolean
  programError: boolean
  deploymentError: boolean
}

const isProgramError = (s: ProgramStatus) => PROGRAM_ERROR_STATUSES.includes(s)

/**
 * Classify a pipeline monitor event into a display status and an incident
 * tuple. Returns `type === null` for the "no data" case (the else branch of
 * the rule); such events are never rendered on the timeline or in the
 * incident list, but they still carry an incident tuple so callers can use
 * them as segment boundaries when grouping.
 */
export function classifyPipelineEvent(e: PipelineMonitorEventSelectedInfo): {
  type: PipelineHealthStatus | null
  incident: IncidentKey
} {
  const deploymentError = !!e.deployment_has_error
  const programError = isProgramError(e.program_status)
  const runtimeUnavailable = e.deployment_runtime_status === 'Unavailable'

  const type: PipelineHealthStatus | null = (() => {
    if (deploymentError) {
      return 'major_issue'
    }
    if (programError) {
      return 'major_issue'
    }
    if (TRANSITIONING_RESOURCE_STATUSES.includes(e.deployment_resources_status)) {
      return 'transitioning'
    }
    if (runtimeUnavailable) {
      return 'unhealthy'
    }
    if (e.deployment_runtime_status === 'Running') {
      return 'healthy'
    }
    if (e.deployment_resources_status === 'Provisioned') {
      return 'transitioning'
    }
    return null
  })()

  return {
    type,
    incident: { runtimeUnavailable, programError, deploymentError }
  }
}

export type ClassifiedPipelineEvent = {
  id: string
  timestamp: Date
  type: PipelineHealthStatus | null
  incident: IncidentKey
  raw: PipelineMonitorEventSelectedInfo
}

export function classifyPipelineEvents(
  events: ReadonlyArray<PipelineMonitorEventSelectedInfo>
): ClassifiedPipelineEvent[] {
  return events.map((e) => {
    const { type, incident } = classifyPipelineEvent(e)
    return {
      id: e.event_id,
      timestamp: new Date(e.recorded_at),
      type,
      incident,
      raw: e
    }
  })
}

const incidentKeyEq = (a: IncidentKey, b: IncidentKey) =>
  a.runtimeUnavailable === b.runtimeUnavailable &&
  a.programError === b.programError &&
  a.deploymentError === b.deploymentError

const isHealthyIncident = (k: IncidentKey) =>
  !k.runtimeUnavailable && !k.programError && !k.deploymentError

function describeIncident(k: IncidentKey): { description: string; title: string } {
  const parts: string[] = []
  if (k.deploymentError) {
    parts.push('deployment error')
  }
  if (k.programError) {
    parts.push('compilation error')
  }
  if (k.runtimeUnavailable) {
    parts.push('runtime unavailable')
  }
  const description = parts.length ? `Pipeline incident: ${parts.join(', ')}` : 'Pipeline incident'
  const title = parts.length ? `Pipeline ${parts.join(' + ')}` : 'Pipeline incident'
  return { description, title }
}

const SEVERITY: Record<PipelineHealthStatus, number> = {
  healthy: 0,
  transitioning: 1,
  unhealthy: 2,
  major_issue: 3
}

/**
 * Group classified pipeline events into incident buckets keyed by the
 * `(runtimeUnavailable, programError, deploymentError)` tuple.
 *
 * Sequential events with the same tuple fold into one bucket. A healthy
 * tuple closes the current incident. Only non-healthy incidents are
 * returned. The most recent incident whose tuple is still non-healthy is
 * marked `active: true`. Events with `type === null` ("no data") only
 * influence segmentation via their incident tuple — they never appear in
 * the resulting buckets.
 */
export function groupPipelineEvents(
  events: ReadonlyArray<ClassifiedPipelineEvent>
): PipelineHealthBucket[] {
  if (!events.length) {
    return []
  }

  const sorted = [...events].sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())

  const segments: ClassifiedPipelineEvent[][] = []
  let current: ClassifiedPipelineEvent[] = []

  for (const e of sorted) {
    if (isHealthyIncident(e.incident)) {
      if (current.length) {
        segments.push(current)
        current = []
      }
      continue
    }
    if (e.type === null) {
      continue
    }
    current.push(e)
  }
  if (current.length) {
    segments.push(current)
  }

  if (!segments.length) {
    return []
  }

  const lastIncident = sorted[sorted.length - 1].incident
  const lastEventIsHealthy = isHealthyIncident(lastIncident)
  const activeSegmentIndex = lastEventIsHealthy ? -1 : segments.length - 1

  const buckets: PipelineHealthBucket[] = []
  for (let i = 0; i < segments.length; i++) {
    buckets.push(...foldByIncident(segments[i], i === activeSegmentIndex))
  }
  return buckets.sort((a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime())
}

function foldByIncident(
  events: ReadonlyArray<ClassifiedPipelineEvent>,
  active: boolean
): PipelineHealthBucket[] {
  const result: PipelineHealthBucket[] = []
  let bucket: PipelineHealthBucket | null = null
  let bucketIncident: IncidentKey | null = null

  for (const e of events) {
    // type==null events are pre-filtered, so cast is safe inside this fold.
    const t = e.type as PipelineHealthStatus
    if (bucket && bucketIncident && incidentKeyEq(bucketIncident, e.incident)) {
      bucket.timestampTo = e.timestamp
      bucket.events.push({ id: e.id, timestamp: e.timestamp, status: t })
      if (SEVERITY[t] > SEVERITY[bucket.type]) {
        bucket.type = t
      }
    } else {
      const labels = describeIncident(e.incident)
      bucket = {
        timestampFrom: e.timestamp,
        timestampTo: e.timestamp,
        type: t,
        description: labels.description,
        tag: 'pipeline',
        active,
        title: labels.title,
        events: [{ id: e.id, timestamp: e.timestamp, status: t }]
      }
      bucketIncident = e.incident
      result.push(bucket)
    }
  }
  return result
}

const NONE = '(none)'

const formatScalar = (v: unknown) => (v == null || v === '' ? NONE : String(v))
const formatJson = (v: unknown) =>
  v == null || v === '' ? NONE : '\n' + JSON.stringify(v, null, 2)

export function formatPipelineEventDescription(e: PipelineMonitorEventSelectedInfo): string {
  return [
    `recorded_at: ${formatScalar(e.recorded_at)}`,
    `program_status: ${formatScalar(e.program_status)}`,
    `deployment_resources_status: ${formatScalar(e.deployment_resources_status)}`,
    `deployment_runtime_status: ${formatScalar(e.deployment_runtime_status)}`,
    `deployment_has_error: ${formatScalar(e.deployment_has_error)}`,
    `deployment_error.message: ${formatScalar(e.deployment_error?.message)}`,
    `deployment_error.error_code: ${formatScalar(e.deployment_error?.error_code)}`,
    `deployment_resources_status_details: ${formatJson(e.deployment_resources_status_details)}`,
    `deployment_runtime_status_details: ${formatJson(e.deployment_runtime_status_details)}`
  ].join('\n')
}
