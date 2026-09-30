import {
  type AggregatedInputEndpointMetrics,
  type AggregatedOutputEndpointMetrics,
  aggregateConnectorMetrics
} from 'common-lib/connectorMetrics'
import { discreteDerivative } from 'common-lib/math'
import { tuple } from 'common-lib/tuple'
import { ServerDate } from '$lib/compositions/serverTime'
import type {
  CheckpointActivity,
  ControllerStatus,
  InputEndpointStatus,
  OutputEndpointStatus,
  PermanentSuspendError
} from '$lib/services/manager'
import type { GlobalMetricsTimestamp, TimeSeriesEntry } from '$lib/types/pipelineManager'

export const emptyPipelineMetrics = {
  tables: new Map<string, AggregatedInputEndpointMetrics>(),
  views: new Map<string, AggregatedOutputEndpointMetrics>(),
  inputs: [] as InputEndpointStatus[],
  outputs: [] as OutputEndpointStatus[],
  global: {
    transaction_status: 'NoTransaction',
    transaction_initiators: { initiated_by_connectors: {} }
  } as GlobalMetricsTimestamp,
  checkpoint_activity: { status: 'idle' } as CheckpointActivity,
  permanent_checkpoint_errors: undefined as Array<PermanentSuspendError> | null | undefined
}

export type PipelineMetrics = typeof emptyPipelineMetrics & { lastTimestamp?: number }

const addZeroMetrics = (previous: PipelineMetrics) => ({
  ...previous,
  tables: new Map(),
  views: new Map()
})

export const accumulatePipelineMetrics =
  (newTimestamp: number) =>
  (
    oldData: PipelineMetrics | undefined,
    { status: newData }: { status: ControllerStatus | null }
  ): PipelineMetrics | undefined => {
    if (!newData) {
      return oldData ? addZeroMetrics(oldData) : oldData
    }
    const globalWithTimestamp = {
      ...newData.global_metrics,
      timeMs: newTimestamp
    }
    return {
      lastTimestamp: oldData?.lastTimestamp,
      inputs: newData.inputs,
      outputs: newData.outputs,
      ...aggregateConnectorMetrics(newData, oldData),
      global: globalWithTimestamp,
      checkpoint_activity: newData.checkpoint_activity ?? { status: 'idle' as const },
      permanent_checkpoint_errors: newData.permanent_checkpoint_errors
    }
  }

/**
 * Right edge (newest time) of a performance graph's time axis.
 *
 * Samples carry server-side timestamps, so the axis must be anchored to the
 * newest sample rather than to the client clock: any client/server clock skew
 * would otherwise shift the plotted line relative to the axis and leave the
 * graph under-filling its width. Before any sample has arrived, fall back to
 * the server-time estimate so the empty window is still in the right time base.
 *
 * @param now - Source of the fallback time; injectable for testing.
 */
export const timeSeriesAxisMax = (metrics: TimeSeriesEntry[], now: () => number = ServerDate.now) =>
  metrics.at(-1)?.t ?? now()

/**
 * Number of oldest samples to drop to keep the series within its bounds.
 *
 * Every sample within `windowMs` of the newest one is kept, and so is the
 * newest sample that falls outside it. That one extra sample is what lets a
 * rate series, which needs a pair of samples per point, cover the whole window:
 * keeping its age rather than a fixed cushion holds at any sample interval.
 *
 * `maxSamples` bounds the series should the timestamps stop advancing, which
 * leaves every sample inside the window; set it well above the sample rate any
 * deployment reports.
 *
 * @param samples - Series ordered oldest first.
 * @param windowMs - How far back from the newest sample to retain.
 * @param maxSamples - Ceiling on the retained sample count.
 */
export const staleSampleCount = (
  samples: TimeSeriesEntry[],
  windowMs: number,
  maxSamples: number
): number => {
  const newest = samples.at(-1)?.t
  if (newest === undefined) {
    return 0
  }
  const oldestKept = newest - windowMs
  let stale = 0
  for (let i = samples.length - 1; i >= 0; i--) {
    if (samples[i].t < oldestKept) {
      // Sample `i` is the newest one outside the window: it anchors the first
      // rate that the window can plot, so only samples older than it are stale.
      stale = i
      break
    }
  }
  return Math.max(stale, samples.length - maxSamples)
}

/**
 * Memory limit on a multi-host deployment, in MB.
 *
 * `memory_mb_max` is the individual host's limit, but the reported memory metric
 * is the sum of resident memory across all hosts in a multihost deployment.
 * To keep the limit line meaningful, multiply the per-host limit by the number of hosts.
 *
 * @param perHostMemoryMb - Per-host limit `runtimeConfig.resources.memory_mb_max`, in MB.
 * @param hosts - Number of hosts `runtimeConfig.hosts`; treated as at least 1.
 * @returns The aggregate limit in MB, or undefined when no limit is configured.
 */
export const multihostMemoryLimitMb = (
  perHostMemoryMb: number | null | undefined,
  hosts: number | null | undefined
): number | undefined => (perHostMemoryMb ? perHostMemoryMb * Math.max(hosts ?? 1, 1) : undefined)

/**
 * Throughput time series: records added between consecutive samples, stamped
 * with the newer sample of each pair.
 *
 * A rate needs two samples, so the series holds one point fewer than `metrics`
 * and starts at the second-oldest sample. Retaining one sample older than the
 * plotted window is therefore what makes the rate cover that window; see
 * `staleSampleCount`.
 *
 * @returns Series of `[timestamp, records]`, plus the latest and mean rate and
 * the y-axis bounds.
 */
export const calcPipelineThroughput = (metrics: TimeSeriesEntry[]) => {
  const series = discreteDerivative(metrics, (n1, n0) => ({
    value: tuple(n1.t, n1.r - n0.r)
  }))

  const avgN = Math.min(Math.ceil(series.length / 5), 4)
  const valueMax = series.length
    ? series
        .slice()
        .sort((a, b) => a.value[1] - b.value[1])
        .slice(-avgN)
        .reduce((acc, cur) => acc + cur.value[1], 0) / avgN
    : 0
  const yMaxStep = 10 ** Math.ceil(Math.log10(valueMax)) / 5
  const yMax = valueMax !== 0 ? Math.ceil((valueMax * 1.25) / yMaxStep) * yMaxStep : 100
  const yMin = 0
  const current = series.at(-1)?.value?.[1] ?? 0
  const average = series.length
    ? series.reduce((acc, cur) => cur.value[1] + acc, 0) / series.length
    : 0
  return { series, current, average, yMin, yMax }
}
