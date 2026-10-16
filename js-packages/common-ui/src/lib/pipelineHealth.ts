// Classifies pipeline monitor events and groups them into incidents. The event type is a
// structural subset of the pipeline-manager API type, so both a live `/events` response and the
// `pipeline_events.json` of a support bundle match it.

import { type HealthEventBucket, type HealthStatus, healthSeverity } from './healthEvents'

/** One pipeline monitor event (`PipelineMonitorEventSelectedInfo` in the OpenAPI spec). The
 *  `*_details` fields are present only in the extended form of the event. */
export type PipelineMonitorEvent = {
  event_id: string
  recorded_at: string
  program_status: string
  storage_status: string
  storage_status_details?: unknown
  deployment_has_error: boolean
  deployment_error?: { message?: string; error_code?: string; details?: unknown } | null
  deployment_resources_status: string
  deployment_resources_status_details?: unknown
  deployment_resources_desired_status: string
  deployment_runtime_status?: string | null
  deployment_runtime_status_details?: unknown
  deployment_runtime_desired_status?: string | null
}

const PROGRAM_ERROR_STATUSES: ReadonlyArray<string> = ['SystemError', 'SqlError', 'RustError']
const TRANSITIONING_RESOURCE_STATUSES: ReadonlyArray<string> = ['Provisioning', 'Stopping']

export type PipelineHealthStatus = HealthStatus
export type PipelineEventTag = 'pipeline'
export type PipelineHealthBucket = HealthEventBucket<PipelineHealthStatus, PipelineEventTag>

export type IncidentKey = {
  runtimeUnavailable: boolean
  programError: boolean
  deploymentError: boolean
}

const isProgramError = (s: string) => PROGRAM_ERROR_STATUSES.includes(s)

/**
 * Classify a pipeline monitor event into a display status and an incident
 * tuple. Returns `type === null` for the "no data" case (the else branch of
 * the rule); such events are never rendered on the timeline or in the
 * incident list, but they still carry an incident tuple so callers can use
 * them as segment boundaries when grouping.
 */
export function categorizePipelineEvent(e: PipelineMonitorEvent): {
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
    if (e.deployment_resources_status === 'Stopped') {
      return 'idle'
    }
    return null
  })()

  return {
    type,
    incident: { runtimeUnavailable, programError, deploymentError }
  }
}

export type CategorizedPipelineEvent = {
  id: string
  timestamp: Date
  type: PipelineHealthStatus | null
  incident: IncidentKey
  raw: PipelineMonitorEvent
}

export function categorizePipelineEvents(
  events: ReadonlyArray<PipelineMonitorEvent>
): CategorizedPipelineEvent[] {
  return events.map((e) => {
    const { type, incident } = categorizePipelineEvent(e)
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

/**
 * Group categorized pipeline events into incident buckets keyed by the
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
  events: ReadonlyArray<CategorizedPipelineEvent>
): PipelineHealthBucket[] {
  if (!events.length) {
    return []
  }

  // Events are assumed to arrive in descending time order (latest first), as
  // returned by the API. Iterating from the end gives oldest-first order without
  // a sort. If that ordering is ever unreliable, a sort on timestamp (with
  // event_id as a tiebreaker for same-millisecond events) would be needed.
  const sorted = events

  const segments: CategorizedPipelineEvent[][] = []
  let current: CategorizedPipelineEvent[] = []

  for (let i = sorted.length - 1; i >= 0; i--) {
    const e = sorted[i]
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

  const lastIncident = sorted[0].incident
  const lastEventIsHealthy = isHealthyIncident(lastIncident)
  const activeSegmentIndex = lastEventIsHealthy ? -1 : segments.length - 1

  const buckets: PipelineHealthBucket[] = []
  for (let i = 0; i < segments.length; i++) {
    buckets.push(...foldByIncident(segments[i], i === activeSegmentIndex))
  }
  return buckets.sort((a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime())
}

function foldByIncident(
  events: ReadonlyArray<CategorizedPipelineEvent>,
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
      if (healthSeverity[t] > healthSeverity[bucket.type]) {
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

// Render a status, always appending the desired value:"Running (desired is Stopped)".
const formatStatusWithDesired = (current: unknown, desired: unknown) => {
  const base = formatScalar(current)
  return `${base}, desired is ${desired ?? `(none)`}`
}

export function formatPipelineEventDescription(e: PipelineMonitorEvent): string {
  return [
    `recorded_at: ${formatScalar(e.recorded_at)}`,
    `program_status: ${formatScalar(e.program_status)}`,
    `storage_status: ${formatScalar(e.storage_status)}`,
    `deployment_resources_status: ${formatStatusWithDesired(e.deployment_resources_status, e.deployment_resources_desired_status)}`,
    `deployment_runtime_status: ${formatStatusWithDesired(e.deployment_runtime_status, e.deployment_runtime_desired_status)}`,
    `deployment_has_error: ${formatScalar(e.deployment_has_error)}`,
    `deployment_error.message: ${formatScalar(e.deployment_error?.message)}`,
    `deployment_error.error_code: ${formatScalar(e.deployment_error?.error_code)}`,
    `storage_status_details: ${formatJson(e.storage_status_details)}`,
    `deployment_resources_status_details: ${formatJson(e.deployment_resources_status_details)}`,
    `deployment_runtime_status_details: ${formatJson(e.deployment_runtime_status_details)}`
  ].join('\n')
}
