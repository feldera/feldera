import { match } from 'ts-pattern'
import { pushSortedOn } from '$lib/functions/common/array'
import type { ClusterMonitorEventSelectedInfo, MonitorStatus } from '$lib/services/manager'

export type HealthEventType = 'healthy' | 'unhealthy' | 'major_issue'

type EventTag = 'api' | 'runner' | 'compiler'

export type RawHealthEvent = {
  timestamp: Date
  type: HealthEventType
  description: string
  tag: EventTag
  id: string
}

export type HealthEventBucket = {
  timestampFrom: Date
  timestampTo: Date
  type: string
  description: string
  tag: EventTag
  active: boolean
  title: string
  events: {
    id: string
    timestamp: Date
    status: HealthEventType
  }[]
}

export const toEventType = (status: MonitorStatus) =>
  match(status)
    .returnType<HealthEventType>()
    .with('Healthy', () => 'healthy' as const)
    .with('InitialUnhealthy', () => 'unhealthy')
    .with('Unhealthy', () => 'major_issue')
    .exhaustive()

export function unpackCombinedEvent(e: ClusterMonitorEventSelectedInfo): RawHealthEvent[] {
  const timestamp = new Date(e.recorded_at)
  return [
    {
      timestamp,
      type: toEventType(e.api_status),
      description:
        e.api_status === 'Healthy'
          ? 'The API server is healthy.'
          : 'There was an issue with the API server.',
      tag: 'api' as const,
      id: e.id
    },
    {
      timestamp,
      type: toEventType(e.compiler_status),
      description:
        e.compiler_status === 'Healthy'
          ? 'The compiler server is healthy.'
          : 'There was an issue with the compiler server.',
      tag: 'compiler' as const,
      id: e.id
    },
    {
      timestamp,
      type: toEventType(e.runner_status),
      description:
        e.runner_status === 'Healthy'
          ? 'The runner is healthy.'
          : 'There was an issue with the runner.',
      tag: 'runner' as const,
      id: e.id
    }
  ]
}

/**
 * Groups sequential events with the same tag and health status into time-bounded buckets.
 * Creates "islands" of consecutive healthy and non-healthy events, split by time boundaries.
 *
 * @param events - The health events to group
 * @param bucketMs - Time boundary in milliseconds (default: 1 hour). Events are split into separate buckets if they cross this boundary
 */
export function groupHealthEvents(
  events: readonly RawHealthEvent[],
  bucketMs: number = 3600000
): HealthEventBucket[] {
  const splitByTheHour = false

  // 1) Group by tag
  const byTag = new Map<EventTag, RawHealthEvent[]>()
  for (const e of events) {
    const arr = byTag.get(e.tag)
    if (arr) arr.push(e)
    else byTag.set(e.tag, [e])
  }

  const allGroups: HealthEventBucket[] = []

  for (const [tag, tagEvents] of byTag.entries()) {
    // 2) Sort by timestamp asc (deterministic tie-breakers optional)
    const sorted = [...tagEvents].sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())

    // 3) Split into segments - alternating "islands" of healthy vs non-healthy events
    // Each segment contains either all healthy or all non-healthy events
    const segments: RawHealthEvent[][] = []
    let current: RawHealthEvent[] = []

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
      // ================================
      // if (current.length === 0) {
      //   // First event
      //   current.push(e)
      // } else {
      //   const prevIsHealthy = current[0].type === 'healthy'
      //   const currIsHealthy = e.type === 'healthy'
      //   if (prevIsHealthy === currIsHealthy) {
      //     // Same island type (both healthy or both non-healthy), continue
      //     current.push(e)
      //   } else {
      //     // Switch to different island type
      //     segments.push(current)
      //     current = [e]
      //   }
      // }
    }
    if (current.length) {
      segments.push(current)
    }

    if (!segments.length) {
      continue
    }

    // 4) Mark "active" ONLY for the last segment if the most recent event is non-healthy
    // Check the most recent event in the sorted timeline (not the last segment, which never contains healthy events)
    const lastEventIsHealthy = sorted[sorted.length - 1]?.type === 'healthy'
    const activeSegmentIndex = lastEventIsHealthy ? -1 : segments.length - 1

    // 5) For each segment, split by time boundaries, then fold consecutive events
    // Insert each segment's events in sorted order to avoid a final sort
    for (let i = 0; i < segments.length; i++) {
      const isActive = i === activeSegmentIndex

      if (splitByTheHour) {
        // Split segment by time boundaries
        const timeSegments = splitByTimeBoundary(segments[i], bucketMs)

        // Process each time-bounded sub-segment
        for (const timeSegment of timeSegments) {
          pushSortedOn(
            allGroups,
            foldConsecutive(timeSegment, tag, isActive),
            (a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime()
          )
        }
      } else {
        pushSortedOn(
          allGroups,
          foldConsecutive(segments[i], tag, isActive),
          (a, b) => b.timestampFrom.getTime() - a.timestampFrom.getTime()
        )
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
function splitByTimeBoundary(
  events: readonly RawHealthEvent[],
  boundaryMs: number
): RawHealthEvent[][] {
  if (events.length === 0) return []

  const result: RawHealthEvent[][] = []
  let current: RawHealthEvent[] = [events[0]]
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

function foldConsecutive(
  eventsSortedByTime: readonly RawHealthEvent[],
  tag: EventTag,
  active: boolean
): HealthEventBucket[] {
  const tagTitles: Record<EventTag, string> = {
    api: 'API server incident',
    compiler: 'Compiler server incident',
    runner: 'Kubernetes runner incident'
  }

  const result: HealthEventBucket[] = []
  let current: HealthEventBucket | null = null

  for (const e of eventsSortedByTime) {
    if (current && current.type === e.type && current.description === e.description) {
      // extend range
      current.timestampTo = e.timestamp
      current.events.push({
        id: e.id,
        timestamp: e.timestamp,
        status: e.type
      })
      // active stays the same for the whole incident
    } else {
      current = {
        timestampFrom: e.timestamp,
        timestampTo: e.timestamp,
        type: e.type,
        description: e.description,
        tag,
        active,
        title: tagTitles[tag],
        events: [
          {
            id: e.id,
            timestamp: e.timestamp,
            status: e.type
          }
        ]
      }
      result.push(current)
    }
  }

  return result
}
