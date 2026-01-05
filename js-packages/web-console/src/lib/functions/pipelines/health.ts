import { match } from 'ts-pattern'
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

export type HealthEventParts = {
  timestampFrom: Date
  timestampTo: Date
  type: string
  description: string
  tag: EventTag
  active: boolean
  events: {
    id: string
    timestamp: Date
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
          ? 'The program compiler is healthy.'
          : 'There was an issue with the program compiler.',
      tag: 'compiler' as const,
      id: e.id
    },
    {
      timestamp,
      type: toEventType(e.runner_status),
      description:
        e.runner_status === 'Healthy'
          ? 'The Kubernetes runner is healthy.'
          : 'There was an issue with the Kubernetes runner.',
      tag: 'runner' as const,
      id: e.id
    }
  ]
}

/**
 * Groups sequential unhealthy events with the same tag as a single incident based on the abscence of healthy events between them
 */
export function groupHealthEvents(events: readonly RawHealthEvent[]): HealthEventParts[] {
  // 1) Group by tag
  const byTag = new Map<EventTag, RawHealthEvent[]>()
  for (const e of events) {
    const arr = byTag.get(e.tag)
    if (arr) arr.push(e)
    else byTag.set(e.tag, [e])
  }

  const allGroups: HealthEventParts[] = []

  for (const [tag, tagEvents] of byTag.entries()) {
    // 2) Sort by timestamp asc (deterministic tie-breakers optional)
    const sorted = [...tagEvents].sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())

    // 3) Split into incident segments by "healthy" separators
    // Each segment contains only NON-healthy events.
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
    }
    if (current.length) segments.push(current)

    if (!segments.length) continue

    // 4) Mark "active" ONLY for the last segment, but only if it is not closed by a later healthy.
    // Because we flush segments upon encountering healthy, the "current" leftover at the end is the only
    // segment that can be "open". So: active = (we ended with a non-empty current).
    //
    // Implementation detail: we already pushed current if non-empty. To know if last segment is open,
    // we can re-check whether the last event in sorted is non-healthy.
    const lastIsHealthy = sorted[sorted.length - 1]?.type === 'healthy'
    const activeSegmentIndex = lastIsHealthy ? -1 : segments.length - 1

    // 5) Fold each segment by consecutive (type, description) and set active on parts if segment is active
    for (let i = 0; i < segments.length; i++) {
      const isActive = i === activeSegmentIndex
      allGroups.push(...foldConsecutive(segments[i], tag, isActive))
    }
  }

  // 6) Global sort by timestampFrom
  allGroups.sort((a, b) => a.timestampFrom.getTime() - b.timestampFrom.getTime())

  return allGroups
}

function foldConsecutive(
  eventsSortedByTime: readonly RawHealthEvent[],
  tag: EventTag,
  active: boolean
): HealthEventParts[] {
  const result: HealthEventParts[] = []
  let current: HealthEventParts | null = null

  for (const e of eventsSortedByTime) {
    if (current && current.type === e.type && current.description === e.description) {
      // extend range
      current.timestampTo = e.timestamp
      current.events.push({
        id: e.id,
        timestamp: e.timestamp
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
        events: [
          {
            id: e.id,
            timestamp: e.timestamp
          }
        ]
      }
      result.push(current)
    }
  }

  return result
}
