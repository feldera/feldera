/**
 * Unit tests for the pipeline health classifier and incident grouper that
 * back the Health tab. These are pure functions over
 * `PipelineMonitorEventSelectedInfo`, so the tests construct minimal events
 * inline rather than mocking the API.
 */
import { describe, expect, it } from 'vitest'
import { partition } from '$lib/functions/common/array'
import type {
  PipelineMonitorEventSelectedInfo,
  ProgramStatus,
  ResourcesStatus
} from '$lib/services/manager'
import {
  type CategorizedPipelineEvent,
  categorizePipelineEvent,
  categorizePipelineEvents,
  formatPipelineEventDescription,
  groupPipelineEvents,
  type IncidentKey
} from './pipelineHealth'

// --- Factories ---

let nextId = 1
const freshId = () => `e${nextId++}`

function makeEvent(
  overrides: Partial<PipelineMonitorEventSelectedInfo> = {}
): PipelineMonitorEventSelectedInfo {
  // Spread overrides last so explicit `null`/`false` overrides are honored.
  return {
    event_id: freshId(),
    recorded_at: '2026-05-01T12:00:00Z',
    program_status: 'Success',
    deployment_resources_desired_status: 'Provisioned',
    deployment_resources_status: 'Provisioned',
    deployment_runtime_desired_status: 'Running',
    deployment_runtime_status: 'Running',
    deployment_has_error: false,
    storage_status: 'InUse',
    ...overrides
  }
}

const HEALTHY_INCIDENT: IncidentKey = {
  runtimeUnavailable: false,
  programError: false,
  deploymentError: false
}

// --- categorizePipelineEvent ---

describe('categorizePipelineEvent', () => {
  describe('rule precedence', () => {
    it('returns major_issue when deployment_has_error is true (highest precedence)', () => {
      // deployment_has_error wins even when other conditions also indicate trouble
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_has_error: true,
          program_status: 'SqlError',
          deployment_resources_status: 'Provisioning',
          deployment_runtime_status: 'Unavailable'
        })
      )
      expect(r.type).toBe('major_issue')
      expect(r.incident).toEqual({
        deploymentError: true,
        programError: true,
        runtimeUnavailable: true
      })
    })

    it.each<ProgramStatus>([
      'SystemError',
      'SqlError',
      'RustError'
    ])('returns major_issue for program_status=%s when no deployment error', (status) => {
      const r = categorizePipelineEvent(makeEvent({ program_status: status }))
      expect(r.type).toBe('major_issue')
      expect(r.incident.programError).toBe(true)
    })

    it.each<ResourcesStatus>([
      'Provisioning',
      'Stopping'
    ])('returns transitioning for resources_status=%s (overrides runtime status)', (status) => {
      // Resource transition wins over Unavailable runtime: blue, not yellow
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: status,
          deployment_runtime_status: 'Unavailable'
        })
      )
      expect(r.type).toBe('transitioning')
    })

    it('returns unhealthy when runtime is Unavailable and resources are not transitioning', () => {
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: 'Provisioned',
          deployment_runtime_status: 'Unavailable'
        })
      )
      expect(r.type).toBe('unhealthy')
      expect(r.incident.runtimeUnavailable).toBe(true)
    })

    it('returns healthy when runtime is Running', () => {
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: 'Provisioned',
          deployment_runtime_status: 'Running'
        })
      )
      expect(r.type).toBe('healthy')
      expect(r.incident).toEqual(HEALTHY_INCIDENT)
    })

    it('returns transitioning when resources are Provisioned but runtime is not Running/Unavailable', () => {
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: 'Provisioned',
          deployment_runtime_status: 'Initializing'
        })
      )
      expect(r.type).toBe('transitioning')
    })

    it('returns "idle" when pipeline is stopped', () => {
      // resources Stopped + runtime null falls through every rule
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: 'Stopped',
          deployment_runtime_status: null
        })
      )
      expect(r.type).toBe('idle')
      expect(r.incident).toEqual(HEALTHY_INCIDENT)
    })
  })

  describe('incident tuple', () => {
    it('all-false tuple is reported as healthy by isHealthyIncident usage', () => {
      // null-categorized events still carry a healthy tuple, so they close
      // incidents during grouping (verified in groupPipelineEvents tests).
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_resources_status: 'Stopped',
          deployment_runtime_status: null
        })
      )
      expect(r.incident).toEqual(HEALTHY_INCIDENT)
    })

    it('captures all three flags when concurrently true', () => {
      const r = categorizePipelineEvent(
        makeEvent({
          deployment_has_error: true,
          program_status: 'RustError',
          deployment_runtime_status: 'Unavailable'
        })
      )
      expect(r.incident).toEqual({
        deploymentError: true,
        programError: true,
        runtimeUnavailable: true
      })
    })
  })
})

// --- categorizePipelineEvents ---

describe('categorizePipelineEvents', () => {
  it('preserves order, attaches timestamps, ids, and raw event reference', () => {
    const e1 = makeEvent({ event_id: 'a', recorded_at: '2026-05-01T10:00:00Z' })
    const e2 = makeEvent({
      event_id: 'b',
      recorded_at: '2026-05-01T11:00:00Z',
      program_status: 'SqlError'
    })
    const out = categorizePipelineEvents([e1, e2])

    expect(out).toHaveLength(2)
    expect(out[0].id).toBe('a')
    expect(out[0].timestamp.toISOString()).toBe('2026-05-01T10:00:00.000Z')
    expect(out[0].type).toBe('healthy')
    expect(out[0].raw).toBe(e1)

    expect(out[1].id).toBe('b')
    expect(out[1].type).toBe('major_issue')
    expect(out[1].incident.programError).toBe(true)
  })
})

// --- groupPipelineEvents ---

function categorized(
  ts: string,
  type: CategorizedPipelineEvent['type'],
  incident: Partial<IncidentKey> = {},
  id = freshId()
): CategorizedPipelineEvent {
  return {
    id,
    timestamp: new Date(ts),
    type,
    incident: { ...HEALTHY_INCIDENT, ...incident },
    raw: makeEvent({ event_id: id, recorded_at: ts })
  }
}

describe('groupPipelineEvents', () => {
  it('returns no buckets when input is empty', () => {
    expect(groupPipelineEvents([])).toEqual([])
  })

  it('returns no buckets when all events are healthy-tuple', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T11:00:00Z', 'healthy'),
      categorized('2026-05-01T10:00:00Z', 'healthy')
    ])
    expect(out).toEqual([])
  })

  it('folds consecutive same-tuple events into one bucket', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:05:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(1)
    expect(out[0].events).toHaveLength(3)
    expect(out[0].timestampFrom.toISOString()).toBe('2026-05-01T10:00:00.000Z')
    expect(out[0].timestampTo.toISOString()).toBe('2026-05-01T10:10:00.000Z')
    expect(out[0].tag).toBe('pipeline')
  })

  it('splits into separate buckets when the tuple changes within an incident', () => {
    // Same segment (no healthy event between), but different tuples
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:05:00Z', 'major_issue', {
        runtimeUnavailable: true,
        deploymentError: true
      }),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(2)
    // Output is sorted descending — newer bucket first.
    expect(out[0].timestampFrom.toISOString()).toBe('2026-05-01T10:05:00.000Z')
    expect(out[1].timestampFrom.toISOString()).toBe('2026-05-01T10:00:00.000Z')
  })

  it('healthy-tuple events close the current incident', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:05:00Z', 'healthy'),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(2)
    // Newest first
    expect(out[0].timestampFrom.toISOString()).toBe('2026-05-01T10:10:00.000Z')
    expect(out[1].timestampFrom.toISOString()).toBe('2026-05-01T10:00:00.000Z')
  })

  it('null-typed events with healthy tuple act as segment boundaries', () => {
    // Without the null event between them, the two unhealthy events would
    // fold into one bucket. The null carries a healthy tuple so it closes
    // the segment, and the second unhealthy starts a new one.
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:05:00Z', null),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(2)
  })

  it('marks only the last bucket as active when the most recent event is non-healthy', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'major_issue', { deploymentError: true }),
      categorized('2026-05-01T10:05:00Z', 'healthy'),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(2)
    // Newest (active) bucket first
    expect(out[0].active).toBe(true)
    expect(out[1].active).toBe(false)
  })

  it('marks no bucket active when the most recent event is healthy', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'healthy'),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(1)
    expect(out[0].active).toBe(false)
  })

  it('escalates bucket type to the worst severity within the incident', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:05:00Z', 'major_issue', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    // Both share the runtimeUnavailable=true tuple → one bucket with the
    // worst observed type.
    expect(out).toHaveLength(1)
    expect(out[0].type).toBe('major_issue')
    expect(out[0].events).toHaveLength(2)
  })

  it('processes events in API descending order (latest first) correctly', () => {
    const out = groupPipelineEvents([
      categorized('2026-05-01T10:10:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:05:00Z', 'unhealthy', { runtimeUnavailable: true }),
      categorized('2026-05-01T10:00:00Z', 'unhealthy', { runtimeUnavailable: true })
    ])
    expect(out).toHaveLength(1)
    expect(out[0].timestampFrom.toISOString()).toBe('2026-05-01T10:00:00.000Z')
    expect(out[0].timestampTo.toISOString()).toBe('2026-05-01T10:10:00.000Z')
  })
})

// --- formatPipelineEventDescription ---

describe('formatPipelineEventDescription', () => {
  it('renders all top-level fields and substitutes (none) for nullish optionals', () => {
    const out = formatPipelineEventDescription(
      makeEvent({
        recorded_at: '2026-05-01T12:00:00Z',
        program_status: 'Success',
        storage_status: 'InUse',
        deployment_resources_status: 'Provisioned',
        deployment_resources_desired_status: 'Provisioned',
        deployment_runtime_status: null,
        deployment_runtime_desired_status: 'Running',
        deployment_has_error: false
      })
    )
    expect(out).toContain('recorded_at: 2026-05-01T12:00:00Z')
    expect(out).toContain('program_status: Success')
    expect(out).toContain('storage_status: InUse')
    expect(out).toContain('deployment_resources_status: Provisioned')
    expect(out).toContain('deployment_resources_desired_status: Provisioned')
    expect(out).toContain('deployment_runtime_status: (none)')
    expect(out).toContain('deployment_runtime_desired_status: Running')
    expect(out).toContain('deployment_has_error: false')
    expect(out).toContain('deployment_error.message: (none)')
    expect(out).toContain('deployment_error.error_code: (none)')
    expect(out).toContain('storage_status_details: (none)')
    expect(out).toContain('deployment_resources_status_details: (none)')
    expect(out).toContain('deployment_runtime_status_details: (none)')
  })

  it('substitutes (none) for nullish desired-status and storage fields', () => {
    const out = formatPipelineEventDescription(
      makeEvent({
        storage_status: undefined,
        deployment_resources_desired_status: undefined,
        deployment_runtime_desired_status: null
      })
    )
    expect(out).toContain('storage_status: (none)')
    expect(out).toContain('deployment_resources_desired_status: (none)')
    expect(out).toContain('deployment_runtime_desired_status: (none)')
  })

  it('renders deployment_error fields when present', () => {
    const out = formatPipelineEventDescription(
      makeEvent({
        deployment_has_error: true,
        deployment_error: {
          message: 'pod crashed',
          error_code: 'PodCrashLoop',
          details: {}
        }
      })
    )
    expect(out).toContain('deployment_error.message: pod crashed')
    expect(out).toContain('deployment_error.error_code: PodCrashLoop')
  })

  it('renders status detail blocks as pretty JSON when provided', () => {
    const out = formatPipelineEventDescription(
      makeEvent({
        storage_status_details: { backend: 'file' },
        deployment_resources_status_details: { phase: 'Pending' },
        deployment_runtime_status_details: { reason: 'Initializing' }
      })
    )
    expect(out).toContain('"backend": "file"')
    expect(out).toContain('"phase": "Pending"')
    expect(out).toContain('"reason": "Initializing"')
  })
})

// --- Full pipeline: raw events → categorize → group → partition ---

describe('TabHealth split: same-timestamp events fold into one active incident', () => {
  // Paste the output of pipeline/events?selector=status here.
  const rawEvents: PipelineMonitorEventSelectedInfo[] = [
    {
      event_id: '019df87b-e784-7af0-865e-fd7918e895e3',
      recorded_at: '2026-05-05T14:12:43.009099Z',
      deployment_resources_status: 'Stopped',
      deployment_resources_desired_status: 'Stopped',
      deployment_runtime_status: null,
      deployment_runtime_desired_status: null,
      deployment_has_error: true,
      program_status: 'Success',
      storage_status: 'InUse'
    },
    {
      event_id: '019df87b-e776-76a3-a1ff-772d65f81d93',
      recorded_at: '2026-05-05T14:12:42.992525Z',
      deployment_resources_status: 'Stopping',
      deployment_resources_desired_status: 'Stopped',
      deployment_runtime_status: null,
      deployment_runtime_desired_status: null,
      deployment_has_error: true,
      program_status: 'Success',
      storage_status: 'InUse'
    },
    {
      event_id: '019df87b-e773-7873-9209-8484a7c415ea',
      recorded_at: '2026-05-05T14:12:42.992525Z',
      deployment_resources_status: 'Provisioned',
      deployment_resources_desired_status: 'Stopped',
      deployment_runtime_status: 'Running',
      deployment_runtime_desired_status: 'Running',
      deployment_has_error: false,
      program_status: 'Success',
      storage_status: 'InUse'
    },
    {
      event_id: '019df87b-23df-7d22-aa43-f979eb2c4b63',
      recorded_at: '2026-05-05T14:11:52.926307Z',
      deployment_resources_status: 'Provisioned',
      deployment_resources_desired_status: 'Provisioned',
      deployment_runtime_status: 'Running',
      deployment_runtime_desired_status: 'Running',
      deployment_has_error: false,
      program_status: 'Success',
      storage_status: 'InUse'
    }
  ]

  it('produces exactly 1 unresolved and 0 previous incidents', () => {
    const categorized = categorizePipelineEvents(rawEvents)
    const grouped = groupPipelineEvents(categorized)
    const [unresolved, previous] = partition(grouped, (e) => e.active)

    expect(unresolved).toHaveLength(1)
    expect(previous).toHaveLength(0)
    expect(unresolved[0].active).toBe(true)
  })
})
