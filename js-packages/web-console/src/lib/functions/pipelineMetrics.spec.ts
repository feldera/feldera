import { describe, expect, it } from 'vitest'
import type { ControllerStatus, InputEndpointStatus } from '$lib/services/manager'
import type { TimeSeriesEntry } from '$lib/types/pipelineManager'
import { formatDuration } from './format'
import {
  accumulatePipelineMetrics,
  multihostMemoryLimitMb,
  timeSeriesAxisMax
} from './pipelineMetrics'

const sampleAt = (timeMs: number): TimeSeriesEntry => ({
  t: timeMs,
  r: 0,
  m: 0,
  s: 0
})

describe('timeSeriesAxisMax', () => {
  it('anchors to the newest sample regardless of the client clock', () => {
    const metrics = [sampleAt(1000), sampleAt(2000), sampleAt(3000)]
    // A client clock that disagrees with the server must not influence the result.
    expect(timeSeriesAxisMax(metrics, () => 9999)).toBe(3000)
  })

  it('falls back to the supplied time source when there are no samples', () => {
    expect(timeSeriesAxisMax([], () => 4242)).toBe(4242)
  })
})

describe('multihostMemoryLimitMb', () => {
  it('returns the per-host limit unchanged for a single host', () => {
    expect(multihostMemoryLimitMb(2048, 1)).toBe(2048)
  })

  it('scales the per-host limit by the number of hosts in a multihost deployment', () => {
    // The reported memory metric sums RSS across all hosts, so the limit line
    // must reflect the aggregate ceiling, not the per-host limit.
    expect(multihostMemoryLimitMb(2048, 3)).toBe(3 * 2048)
  })

  it('treats a missing or non-positive host count as a single host', () => {
    expect(multihostMemoryLimitMb(2048, undefined)).toBe(2048)
    expect(multihostMemoryLimitMb(2048, null)).toBe(2048)
    expect(multihostMemoryLimitMb(2048, 0)).toBe(2048)
  })

  it('returns undefined when no memory limit is configured', () => {
    expect(multihostMemoryLimitMb(undefined, 3)).toBeUndefined()
    expect(multihostMemoryLimitMb(null, 3)).toBeUndefined()
    expect(multihostMemoryLimitMb(0, 3)).toBeUndefined()
  })
})

describe('accumulatePipelineMetrics latency aggregate', () => {
  const inputStatus = (
    stream: string,
    endpointName: string,
    latency?: number
  ): InputEndpointStatus =>
    ({
      config: { stream },
      endpoint_name: endpointName,
      metrics: {
        total_bytes: 0,
        total_records: 0,
        buffered_bytes: 0,
        buffered_records: 0,
        num_transport_errors: 0,
        num_parse_errors: 0,
        end_of_input: false,
        processing_latency_p99_micros: latency
      },
      paused: false,
      barrier: false,
      health: null,
      fatal_error: null
    }) as InputEndpointStatus

  const aggregateLatency = (inputs: InputEndpointStatus[], relation = 'orders') =>
    accumulatePipelineMetrics(0)(undefined, {
      status: {
        global_metrics: {
          transaction_initiators: { initiated_by_connectors: {} }
        },
        inputs,
        outputs: []
      } as unknown as ControllerStatus
    })!.tables.get(relation)!.aggregate.metrics.processing_latency_p99_micros

  it('reports the slowest connector rather than the sum of percentiles', () => {
    const latency = aggregateLatency([
      inputStatus('orders', 'c1', 2_000),
      inputStatus('orders', 'c2', 8_000)
    ])
    expect(latency).toBe(8_000)
  })

  it('ignores connectors without samples', () => {
    expect(
      aggregateLatency([inputStatus('orders', 'c1'), inputStatus('orders', 'c2', 3_000)])
    ).toBe(3_000)
  })

  it('is undefined when no connector has samples', () => {
    expect(aggregateLatency([inputStatus('orders', 'c1')])).toBeUndefined()
  })

  it('passes the latency of a single connector through unchanged', () => {
    expect(aggregateLatency([inputStatus('orders', 'c1', 4_200)])).toBe(4_200)
  })

  it('brands the aggregate as a duration', () => {
    // Typechecks only while the aggregate carries its unit: `formatDuration`
    // takes `Microseconds`, not a bare number.
    expect(formatDuration(aggregateLatency([inputStatus('orders', 'c1', 2_000)]))).toBe('2 ms')
  })
})
