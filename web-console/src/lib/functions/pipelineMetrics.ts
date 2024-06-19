import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { normalizeCaseIndependentName } from '$lib/functions/felderaRelation'
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
      newData.inputs.map(cs => tuple(normalizeCaseIndependentName({ name: cs.config.stream }), cs.metrics))
    ),
    output: new Map(
      newData.outputs.map(cs => tuple(normalizeCaseIndependentName({ name: cs.config.stream }), cs.metrics))
    ),
    global: reconcileHistoricData(oldData?.global ?? [], globalWithTimestamp, refetchMs, keepMs)
  } as any
}
