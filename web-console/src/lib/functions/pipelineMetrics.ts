import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { normalizeCaseIndependentName } from '$lib/functions/felderaRelation'
import { groupBy } from '$lib/functions/common/array'
import {
  ControllerStatus,
  GlobalMetricsTimestamp,
  InputEndpointMetrics,
  OutputEndpointMetrics
} from '$lib/types/pipeline'
import invariant from 'tiny-invariant'

const emptyPipelineMetrics = {
  input: new Map<string, InputEndpointMetrics>(),
  output: new Map<string, OutputEndpointMetrics>(),
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
    const isOverwritingTimestamp = !!oldData.find(m => m.timeMs >= newData.timeMs)
    if (isOverwritingTimestamp) {
      // clear metrics history if we get a timestamp that overwrites existing data point
      return oldData.length
    }
    // return one more element than needed to satisfy keepMs when applying discreteDerivative() to result data series
    return -Math.ceil(keepMs / refetchMs)
  })()
  return [...oldData.slice(sliceAt), newData]
}

export const accumulatePipelineMetrics = (refetchMs: number, keepMs?: number) => (oldData: any, x: any) => {
  const { status: newData } = x
  invariant(((v: any): v is PipelineMetrics | undefined => true)(oldData))
  invariant(((v: any): v is ControllerStatus | null => true)(newData))
  if (!newData) {
    return {
      ...emptyPipelineMetrics,
      lastTimestamp: undefined
    }
  }
  const newTimestamp = Date.now()
  const globalWithTimestamp = {
    ...newData.global_metrics,
    timeMs: newTimestamp
  }
  return {
    lastTimestamp: oldData?.lastTimestamp,
    input: new Map(
      groupBy(newData.inputs, i => normalizeCaseIndependentName({ name: i.config.stream })).map(
        ([relationName, metrics]) =>
          tuple(
            relationName,
            metrics.reduce(
              (acc: InputEndpointMetrics, cur) => {
                const metrics = cur.metrics
                return {
                  total_bytes: acc.total_bytes + metrics.total_bytes,
                  total_records: acc.total_records + metrics.total_records,
                  buffered_bytes: acc.buffered_bytes + metrics.buffered_bytes,
                  buffered_records: acc.buffered_records + metrics.buffered_records,
                  num_transport_errors: acc.num_transport_errors + metrics.num_transport_errors,
                  num_parse_errors: acc.num_parse_errors + metrics.num_parse_errors,
                  end_of_input: acc.end_of_input || metrics.end_of_input
                }
              },
              {
                total_bytes: 0,
                total_records: 0,
                buffered_bytes: 0,
                buffered_records: 0,
                num_transport_errors: 0,
                num_parse_errors: 0,
                end_of_input: false
              }
            )
          )
      )
    ),
    output: new Map(
      groupBy(newData.outputs, i => normalizeCaseIndependentName({ name: i.config.stream })).map(
        ([relationName, metrics]) =>
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
