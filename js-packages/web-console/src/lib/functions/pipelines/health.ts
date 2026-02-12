import { match, P } from 'ts-pattern'
import { pushSortedOn } from '$lib/functions/common/array'
import { ceilToHour } from '$lib/functions/common/date'
import type {
  ClusterMonitorEventSelectedInfo,
  MonitorStatus,
  PipelineMonitorEventSelectedInfo
} from '$lib/services/manager'

export type HealthEventType = 'healthy' | 'unhealthy' | 'major_issue'

export type RawHealthEvent<Tag extends string = string> = {
  timestamp: Date
  type: HealthEventType
  description: string
  tag: Tag
  id: string
  thumb: string
}

export type HealthEventBucket<Tag extends string = string> = {
  timestampFrom: Date
  timestampTo: Date
  type: string
  description: string
  tag: Tag
  active: boolean
  title: string
  events: {
    id: string
    timestamp: Date
    status: HealthEventType
    thumb: string
  }[]
}

// --- Cluster event helpers ---

export type ClusterEventTag = 'api' | 'compiler' | 'runner'

export const clusterTagTitles: Record<ClusterEventTag, string> = {
  api: 'API server incident',
  compiler: 'Compiler server incident',
  runner: 'Kubernetes runner incident'
}

export const toEventType = (status: MonitorStatus) =>
  match(status)
    .returnType<HealthEventType>()
    .with('Healthy', () => 'healthy' as const)
    .with('InitialUnhealthy', () => 'unhealthy')
    .with('Unhealthy', () => 'major_issue')
    .exhaustive()

export function unpackCombinedEvent(
  e: ClusterMonitorEventSelectedInfo
): RawHealthEvent<ClusterEventTag>[] {
  const timestamp = new Date(e.recorded_at)
  return [
    {
      timestamp,
      type: toEventType(e.api_status),
      description:
        e.api_status === 'Healthy'
          ? 'The API server is healthy.'
          : 'There was an issue with the API server.',
      tag: 'api',
      id: e.id,
      thumb: ''
    },
    {
      timestamp,
      type: toEventType(e.compiler_status),
      description:
        e.compiler_status === 'Healthy'
          ? 'The compiler server is healthy.'
          : 'There was an issue with the compiler server.',
      tag: 'compiler',
      id: e.id,
      thumb: ''
    },
    {
      timestamp,
      type: toEventType(e.runner_status),
      description:
        e.runner_status === 'Healthy'
          ? 'The runner is healthy.'
          : 'There was an issue with the runner.',
      tag: 'runner',
      id: e.id,
      thumb: ''
    }
  ]
}

export function formatClusterEventDetail(
  e: ClusterMonitorEventSelectedInfo,
  tag: ClusterEventTag
): { timestamp: Date; description: string } {
  return {
    timestamp: new Date(e.recorded_at),
    description: match(tag)
      .with('api', () => (e.api_self_info || '') + '\n' + (e.api_resources_info || ''))
      .with(
        'compiler',
        () => (e.compiler_self_info || '') + '\n' + (e.compiler_resources_info || '')
      )
      .with('runner', () => (e.runner_self_info || '') + '\n' + (e.runner_resources_info || ''))
      .exhaustive()
  }
}

// --- Pipeline event helpers ---

export type PipelineEventTag = 'pipeline'

export const pipelineTagTitles: Record<PipelineEventTag, string> = {
  pipeline: 'Pipeline incident'
}

export function unpackPipelineEvent(
  e: PipelineMonitorEventSelectedInfo
): RawHealthEvent<PipelineEventTag> {
  const { type, description } = pipelineEventClassification(e)
  return {
    timestamp: new Date(e.recorded_at),
    type,
    description,
    tag: 'pipeline',
    id: e.id,
    thumb: description
  }
}

function pipelineEventClassification(e: PipelineMonitorEventSelectedInfo): {
  type: HealthEventType
  description: string
} {
  return match(e)
    .returnType<{ type: HealthEventType; description: string }>()
    .with(
      { program_status: P.union('SqlError', 'RustError', 'SystemError') },
      ({ program_status }) => ({
        type: 'major_issue',
        description: `Pipeline has a compilation error (${program_status}).`
      })
    )
    .with({ resources_desired_status: 'Provisioned', resources_status: 'Stopped' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline provisioning requested.'
    }))
    .with({ resources_desired_status: 'Provisioned', resources_status: 'Stopping' }, () => ({
      type: 'major_issue',
      description: 'Pipeline is shutting down.'
    }))
    .with({ resources_status: 'Provisioning' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline resources are being provisioned.'
    }))
    .with({ resources_status: 'Stopping' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is shutting down.'
    }))
    .with(
      {
        resources_status: 'Provisioned',
        runtime_status: 'Unavailable',
        runtime_desired_status: P.when((d) => d != null && d !== 'Unavailable')
      },
      () => ({
        type: 'major_issue',
        description: 'Pipeline runtime is unavailable.'
      })
    )
    .with({ resources_status: 'Provisioned', runtime_status: 'Running' }, () => ({
      type: 'healthy',
      description: 'Pipeline is running.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Paused' }, () => ({
      type: 'healthy',
      description: 'Pipeline is paused.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Initializing' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is initializing.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Bootstrapping' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is bootstrapping.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Replaying' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is replaying.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Standby' }, () => ({
      type: 'healthy',
      description: 'Pipeline is on standby.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Suspended' }, () => ({
      type: 'healthy',
      description: 'Pipeline is suspended.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Unavailable' }, () => ({
      type: 'healthy',
      description: 'Pipeline runtime is unavailable.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'Coordination' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is coordinating.'
    }))
    .with({ resources_status: 'Provisioned', runtime_status: 'AwaitingApproval' }, () => ({
      type: 'unhealthy',
      description: 'Pipeline is awaiting approval.'
    }))
    .with({ resources_status: 'Stopped' }, () => ({
      type: 'healthy',
      description: 'Pipeline is stopped.'
    }))
    .otherwise(() => ({
      type: 'healthy',
      description: 'Pipeline status recorded.'
    }))
}

export function formatPipelineEventDetail(e: PipelineMonitorEventSelectedInfo): {
  timestamp: Date
  description: string
} {
  const lines: string[] = []
  lines.push(`Program: ${e.program_status}`)
  lines.push(`Resources: ${e.resources_status} (desired: ${e.resources_desired_status})`)
  if (e.resources_status_details) {
    lines.push(`Resources details: ${JSON.stringify(e.resources_status_details, null, 2)}`)
  }
  if (e.runtime_status != null) {
    lines.push(`Runtime: ${e.runtime_status} (desired: ${e.runtime_desired_status ?? 'N/A'})`)
  }
  if (e.runtime_status_details) {
    lines.push(`Runtime details: ${JSON.stringify(e.runtime_status_details, null, 2)}`)
  }
  lines.push(`Storage: ${e.storage_status}`)
  return {
    timestamp: new Date(e.recorded_at),
    description: lines.join('\n')
  }
}

// --- Shared utilities ---

/**
 * Compute the end of the health time window (ceiled to the nearest hour).
 */
export function healthTimeWindowEnd(events: { recorded_at: string }[] | null): Date {
  return ceilToHour(events?.length ? new Date(events[0].recorded_at) : new Date())
}

/**
 * Compute the start of the health time window.
 */
export function healthTimeWindowStart(endDate: Date, windowHours: number): Date {
  return new Date(endDate.getTime() - windowHours * 60 * 60 * 1000)
}

/**
 * Create a HealthEventBucket from a clicked timeline bar's time range.
 * Expects the raw events to already be filtered by tag and sorted by timestamp in descending order.
 */
export function createBucketFromTimelineGroup<Tag extends string>(
  rawEvents: RawHealthEvent<Tag>[],
  tag: Tag,
  group: { startTime: number; endTime: number; status: HealthEventType },
  label: string
): HealthEventBucket<Tag> | null {
  const eventsInRange = rawEvents.filter((e) => {
    const eventTime = e.timestamp.getTime()
    return e.tag === tag && eventTime >= group.startTime && eventTime < group.endTime
  })
  if (eventsInRange.length === 0) return null
  return {
    timestampFrom: new Date(group.startTime),
    timestampTo: new Date(group.endTime),
    type: group.status,
    description: `${label} events`,
    tag,
    active: false,
    title: `${label} status history`,
    events: eventsInRange.map((e) => ({
      id: e.id,
      timestamp: e.timestamp,
      status: e.type,
      thumb: e.thumb
    }))
  }
}

// --- Event grouping ---

/**
 * Groups sequential events with the same tag and health status into time-bounded buckets.
 * Creates "islands" of consecutive healthy and non-healthy events, split by time boundaries.
 *
 * @param events - The health events to group
 * @param bucketMs - Time boundary in milliseconds (default: 1 hour). Events are split into separate buckets if they cross this boundary
 * @param tagTitles - Mapping from tag to incident title for display
 */
export function groupHealthEvents<Tag extends string>(
  events: readonly RawHealthEvent<Tag>[],
  bucketMs: number = 3600000,
  tagTitles: Record<Tag, string> = clusterTagTitles as Record<Tag, string>
): HealthEventBucket<Tag>[] {
  const splitByTheHour = false

  // 1) Group by tag
  const byTag = new Map<Tag, RawHealthEvent<Tag>[]>()
  for (const e of events) {
    const arr = byTag.get(e.tag)
    if (arr) arr.push(e)
    else byTag.set(e.tag, [e])
  }

  const allGroups: HealthEventBucket<Tag>[] = []

  for (const [tag, tagEvents] of byTag.entries()) {
    // 2) Sort by timestamp asc (deterministic tie-breakers optional)
    const sorted = [...tagEvents].sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime())

    // 3) Split into buckets of incidents separated by "healthy" events
    // Each bucket contains only NON-healthy events.
    const segments: RawHealthEvent<Tag>[][] = []
    let current: RawHealthEvent<Tag>[] = []

    for (const e of sorted) {
      if (e.type === 'healthy') {
        // healthy closes the current incident (if any)
        if (current.length) {
          segments.push(current)
          current = []
        }
        continue
      }

      current.push(e)
    }
    if (current.length) {
      segments.push(current)
    }

    if (!segments.length) {
      continue
    }

    // 4) Mark "active" ONLY for the first segment if the most recent event is non-healthy
    // Since sorted is in descending order (newest first), sorted[0] is the most recent event
    // and segments[0] contains the most recent non-healthy events
    const mostRecentEventIsHealthy = sorted[0]?.type === 'healthy'
    const activeSegmentIndex = mostRecentEventIsHealthy ? -1 : 0

    // 5) For each segment, split by time boundaries, then fold consecutive events
    // Insert each segment's events in sorted order to avoid a final sort
    for (let i = 0; i < segments.length; i++) {
      const isActive = i === activeSegmentIndex

      if (splitByTheHour) {
        // Split segment by time boundaries
        const timeSegments = splitByTimeBoundary(segments[i], bucketMs)

        // Process each time-bounded sub-segment
        for (const timeSegment of timeSegments) {
          const bucket = foldSegment(timeSegment, tag, isActive, tagTitles)
          if (bucket) {
            pushSortedOn(
              allGroups,
              [bucket],
              (a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime()
            )
          }
        }
      } else {
        const bucket = foldSegment(segments[i], tag, isActive, tagTitles)
        if (bucket) {
          pushSortedOn(
            allGroups,
            [bucket],
            (a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime()
          )
        }
      }
    }
  }

  return allGroups
}

/**
 * Splits events into sub-arrays based on time boundaries.
 * Events that cross a time boundary (e.g., hour boundary) are placed in separate arrays.
 *
 * @param events - Events sorted by timestamp
 * @param boundaryMs - Time boundary in milliseconds (e.g., 3600000 for 1 hour)
 */
function splitByTimeBoundary<Tag extends string>(
  events: readonly RawHealthEvent<Tag>[],
  boundaryMs: number
): RawHealthEvent<Tag>[][] {
  if (events.length === 0) return []

  const result: RawHealthEvent<Tag>[][] = []
  let current: RawHealthEvent<Tag>[] = [events[0]]
  let currentBoundary = Math.floor(events[0].timestamp.getTime() / boundaryMs)

  for (let i = 1; i < events.length; i++) {
    const boundary = Math.floor(events[i].timestamp.getTime() / boundaryMs)
    if (boundary === currentBoundary) {
      current.push(events[i])
    } else {
      result.push(current)
      current = [events[i]]
      currentBoundary = boundary
    }
  }
  result.push(current)
  return result
}

const severityOrder: Record<HealthEventType, number> = {
  healthy: 0,
  unhealthy: 1,
  major_issue: 2
}

/**
 * Folds all non-healthy events in a segment into a single HealthEventBucket.
 * The bucket's type is the worst severity seen, and its description comes
 * from the first event with that worst severity.
 * Expects eventsSortedByTime to already be filtered by tag and sorted by timestamp in descending order.
 */
function foldSegment<Tag extends string>(
  eventsSortedByTime: readonly RawHealthEvent<Tag>[],
  tag: Tag,
  active: boolean,
  tagTitles: Record<Tag, string>
): HealthEventBucket<Tag> | null {
  if (eventsSortedByTime.length === 0) {
    return null
  }

  const earliest = eventsSortedByTime.length - 1
  let worstType: HealthEventType = eventsSortedByTime[earliest].type
  let worstDescription = eventsSortedByTime[earliest].description
  for (let i = earliest - 1; i >= 0; --i) {
    const event = eventsSortedByTime[i]
    if (severityOrder[event.type] > severityOrder[worstType]) {
      worstType = event.type
      worstDescription = event.description
    }
  }

  return {
    timestampFrom: eventsSortedByTime[earliest].timestamp,
    timestampTo: eventsSortedByTime[0].timestamp,
    type: worstType,
    description: worstDescription,
    tag,
    active,
    title: tagTitles[tag] ?? `${tag} incident`,
    events: eventsSortedByTime.map((e) => ({
      id: e.id,
      timestamp: e.timestamp,
      status: e.type,
      thumb: e.thumb
    }))
  }
}
