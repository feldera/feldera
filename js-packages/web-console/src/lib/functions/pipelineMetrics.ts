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
  OutputEndpointStatus,
  TimeSeriesEntry
} from '$lib/types/pipelineManager'
import invariant from 'tiny-invariant'
import { discreteDerivative } from './common/math'

export const emptyPipelineMetrics = {
  tables: new Map<string, InputEndpointMetrics>(),
  views: new Map<string, OutputEndpointMetrics>(),
  inputs: [] as InputEndpointStatus[],
  outputs: [] as OutputEndpointStatus[],
  global: {} as GlobalMetricsTimestamp
}

export type PipelineMetrics = typeof emptyPipelineMetrics & { lastTimestamp?: number }

const addZeroMetrics = (previous: PipelineMetrics) => ({
  ...previous,
  tables: new Map(),
  views: new Map()
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
      global: globalWithTimestamp
    } as any
  }

/**
 * @returns Time series of throughput with smoothing window over 3 data intervals
 */
export const calcPipelineThroughput = (metrics: TimeSeriesEntry[]) => {
  const series = discreteDerivative(metrics, ({}, {}, i, arr) => {
    const n1 = arr[i]
    const n0 = arr[i - 1]
    return {
      name: n1.t.toFixed(),
      value: tuple(n1.t.toNumber(), n1.r.minus(n0.r).toNumber())
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
