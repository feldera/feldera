import { describe, expect, it } from 'vitest'
import type { ControllerStatus, InputEndpointStatus } from '$lib/services/manager'
import type { TimeSeriesEntry } from '$lib/types/pipelineManager'
import { formatDuration } from './format'
import {
  accumulatePipelineMetrics,
  calcPipelineThroughput,
  multihostMemoryLimitMb,
  staleSampleCount,
  timeSeriesAxisMax
} from './pipelineMetrics'

const sampleAt = (timeMs: number, records = 0): TimeSeriesEntry => ({
  t: timeMs,
  r: records,
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

describe('staleSampleCount', () => {
  /** `hosts` samples per second, as a multihost coordinator reports them. */
  const seriesAt = (hosts: number, seconds: number, firstMs = 0) =>
    Array.from({ length: hosts * seconds }, (_, i) =>
      // Hosts report at a fixed phase offset within each second.
      sampleAt(firstMs + Math.floor(i / hosts) * 1000 + (i % hosts) * 50)
    )

  it('keeps a series that fits the window', () => {
    expect(staleSampleCount([sampleAt(1000), sampleAt(2000), sampleAt(3000)], 5000)).toBe(0)
  })

  it('drops samples older than the window', () => {
    const samples = [sampleAt(1000), sampleAt(2000), sampleAt(3000), sampleAt(4000)]
    // Measured from the newest sample: 1000 is 3000ms old, 2000 is exactly at the edge.
    expect(staleSampleCount(samples, 2000)).toBe(1)
  })

  it('keeps a sample sitting exactly on the window edge', () => {
    expect(staleSampleCount([sampleAt(1000), sampleAt(3000)], 2000)).toBe(0)
  })

  it('retains the same time span whatever the sample rate', () => {
    // A multihost pipeline reports one sample per host per tick, so a count-based
    // buffer would shrink the retained span in proportion to the host count.
    const spanOf = (hosts: number) => {
      const samples = seriesAt(hosts, 90)
      const kept = samples.slice(staleSampleCount(samples, 60_000))
      return kept.at(-1)!.t - kept[0]!.t
    }
    expect(spanOf(1)).toBe(60_000)
    expect(spanOf(2)).toBe(60_000)
    expect(spanOf(4)).toBe(60_000)
  })

  it('drops a stale sample that arrived behind fresher ones', () => {
    // The scan walks back from the newest sample, so an out-of-order straggler
    // takes everything older than itself with it.
    const samples = [sampleAt(9000), sampleAt(1000), sampleAt(9500), sampleAt(10_000)]
    expect(staleSampleCount(samples, 2000)).toBe(2)
  })

  it('has nothing to drop in an empty series', () => {
    expect(staleSampleCount([], 60_000)).toBe(0)
  })
})

describe('calcPipelineThroughput', () => {
  it('reports the records added since the preceding sample', () => {
    const { series } = calcPipelineThroughput([
      sampleAt(1000, 0),
      sampleAt(2000, 30),
      sampleAt(3000, 100)
    ])
    expect(series.map((p) => p.value)).toEqual([
      [2000, 30],
      [3000, 70]
    ])
  })

  it('covers the plotted window once one extra sample interval is retained', () => {
    // Why `RETAIN_MS` exceeds the plotted window: the oldest sample yields no
    // point, so retaining exactly the window would leave the line short of it.
    const samples = Array.from({ length: 62 }, (_, i) => sampleAt(i * 1000, i))
    const { series } = calcPipelineThroughput(samples)
    expect(series.length).toBe(samples.length - 1)
    expect(series.at(-1)!.value[0] - series[0]!.value[0]).toBe(60_000)
  })

  it('reports the newest rate as the current one', () => {
    expect(calcPipelineThroughput([sampleAt(1000, 0), sampleAt(2000, 5)]).current).toBe(5)
  })

  it('has no rate to report for fewer than two samples', () => {
    expect(calcPipelineThroughput([]).series).toEqual([])
    expect(calcPipelineThroughput([sampleAt(1000, 7)]).series).toEqual([])
    expect(calcPipelineThroughput([sampleAt(1000, 7)]).current).toBe(0)
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
