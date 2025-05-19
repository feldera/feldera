import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { normalizeCaseIndependentName } from '$lib/functions/felderaRelation'
import { groupBy } from '$lib/functions/common/array'
import type {
  ControllerStatus,
  GlobalMetricsTimestamp,
  InputEndpointMetrics,
  InputEndpointStatus,
  OutputEndpointMetrics,
  OutputEndpointStatus
} from '$lib/types/pipelineManager'
import invariant from 'tiny-invariant'
import { discreteDerivative } from './common/math'

export const emptyPipelineMetrics = {
  tables: new Map<string, InputEndpointMetrics>(),
  views: new Map<string, OutputEndpointMetrics>(),
  inputs: [] as InputEndpointStatus[],
  outputs: [] as OutputEndpointStatus[],
  global: [] as GlobalMetricsTimestamp[]
}

export type PipelineMetrics = typeof emptyPipelineMetrics & { lastTimestamp?: number }

/**
 * Accumulate metrics history, accounting for the desired time window, dropping the previous data if the new data overwrites it
 * @param keepMs Time window of metrics age to be kept
 */
const reconcileHistoricData = (
  oldData: GlobalMetricsTimestamp[],
  newData: GlobalMetricsTimestamp,
  refetchMs: number,
  keepMs?: number
) => {
  const sliceAt = (() => {
    if (!nonNull(keepMs)) {
      return -oldData.length
    }
    const isOverwritingTimestamp = !!oldData.find((m) => m.timeMs >= newData.timeMs)
    if (isOverwritingTimestamp) {
      // clear metrics history if we get a timestamp that overwrites existing data point
      return oldData.length
    }
    // return one more element than needed to satisfy keepMs when applying discreteDerivative() to result data series
    return -Math.ceil(keepMs / refetchMs)
  })()
  return [...oldData.slice(sliceAt), newData]
}

const addZeroMetrics = (previous: PipelineMetrics) => ({
  ...previous,
  tables: new Map(),
  views: new Map(),
  global: previous.global.length
    ? ((m) => [
        ...previous.global,
        {
          ...m,
          rss_bytes: 0,
          timeMs: Date.now(),
          start_time: 0
        }
      ])(previous.global.at(-1))
    : []
})

export const accumulatePipelineMetrics =
  (newTimestamp: number, refetchMs: number, keepMs?: number) => (oldData: any, x: any) => {
    const { status: newData } = x
    invariant(((v: any): v is PipelineMetrics | undefined => true)(oldData))
    invariant(((v: any): v is ControllerStatus | null => true)(newData))
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
      tables: new Map(
        groupBy(newData.inputs, (i) => normalizeCaseIndependentName({ name: i.config.stream })).map(
          ([relationName, metrics]) =>
            tuple(
              relationName,
              metrics.reduce(
                (acc: InputEndpointMetrics, cur) => {
                  const metrics = cur.metrics
                  return {
                    total_bytes: acc.total_bytes + metrics.total_bytes,
                    total_records: acc.total_records + metrics.total_records,
                    buffered_records: acc.buffered_records + metrics.buffered_records,
                    num_transport_errors: acc.num_transport_errors + metrics.num_transport_errors,
                    num_parse_errors: acc.num_parse_errors + metrics.num_parse_errors,
                    end_of_input: acc.end_of_input || metrics.end_of_input
                  }
                },
                {
                  total_bytes: 0,
                  total_records: 0,
                  buffered_records: 0,
                  num_transport_errors: 0,
                  num_parse_errors: 0,
                  end_of_input: false
                }
              )
            )
        )
      ),
      views: new Map(
        groupBy(newData.outputs, (i) =>
          normalizeCaseIndependentName({ name: i.config.stream })
        ).map(([relationName, metrics]) =>
          tuple(
            relationName,
            metrics.reduce(
              (acc: OutputEndpointMetrics, cur) => {
                const metrics = cur.metrics
                return {
                  buffered_batches: acc.buffered_batches + metrics.buffered_batches,
                  buffered_records: acc.buffered_records + metrics.buffered_records,
                  num_encode_errors: acc.num_encode_errors + metrics.num_encode_errors,
                  num_transport_errors: acc.num_transport_errors + metrics.num_transport_errors,
                  total_processed_input_records:
                    acc.total_processed_input_records + metrics.total_processed_input_records,
                  transmitted_bytes: acc.transmitted_bytes + metrics.transmitted_bytes,
                  transmitted_records: acc.transmitted_records + metrics.transmitted_records
                }
              },
              {
                buffered_batches: 0,
                buffered_records: 0,
                num_encode_errors: 0,
                num_transport_errors: 0,
                total_processed_input_records: 0,
                transmitted_bytes: 0,
                transmitted_records: 0
              }
            )
          )
        )
      ),
      global: reconcileHistoricData(oldData?.global ?? [], globalWithTimestamp, refetchMs, keepMs)
    } as any
  }

/**
 * @returns Time series of throughput with smoothing window over 3 data intervals
 */
export const calcPipelineThroughput = (metrics: { global: GlobalMetricsTimestamp[] }) => {
  const totalProcessed = metrics.global.map((m) => tuple(m.timeMs, m.total_processed_records))
  const series = discreteDerivative(totalProcessed, ({}, {}, i, arr) => {
    const n3 = arr[i]
    const n2 = arr[i - 1]
    const n1 = arr[i - 2] ?? n2
    const n0 = arr[i - 3] ?? n1
    return {
      name: n3[0].toString(),
      value: tuple(n3[0], ((n3[1] - n0[1]) * 1000) / (n3[0] - n0[0]))
    }
  })

  const avgN = Math.min(Math.ceil(series.length / 5), 4)
  const valueMax = series.length
    ? series
        .slice()
        .sort((a, b) => a.value[1] - b.value[1])
        .slice(-avgN)
        .reduce((acc, cur) => acc + cur.value[1], 0) / avgN
    : 0
  const yMaxStep = Math.pow(10, Math.ceil(Math.log10(valueMax))) / 5
  const yMax = valueMax !== 0 ? Math.ceil((valueMax * 1.25) / yMaxStep) * yMaxStep : 100
  const yMin = 0
  const current = series.at(-1)?.value?.[1] ?? 0
  const average = series.length
    ? series.reduce((acc, cur) => cur.value[1] + acc, 0) / series.length
    : 0
  return { series, current, average, yMin, yMax }
}
