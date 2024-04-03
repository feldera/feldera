import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { normalizeCaseIndependentName } from '$lib/functions/felderaRelation'
import {
  ControllerStatus,
  GlobalMetrics,
  GlobalMetricsTimestamp,
  InputEndpointMetrics,
  OutputEndpointMetrics,
  PipelineStatus
} from '$lib/types/pipeline'
import { useState } from 'react'
import invariant from 'tiny-invariant'

import { useQuery } from '@tanstack/react-query'

const emptyData = {
  input: new Map<string, InputEndpointMetrics>(),
  output: new Map<string, OutputEndpointMetrics>(),
  global: [] as GlobalMetricsTimestamp[]
}

type PipelineMetrics = typeof emptyData

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

export function usePipelineMetrics(props: {
  pipelineName: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const pipelineManagerQuery = usePipelineManagerQuery()
  const [lastTimestamp, setLastTimestamp] = useState<number>()
  const pipelineStatsQuery = useQuery({
    ...pipelineManagerQuery.pipelineStats(props.pipelineName),
    enabled: props.status === PipelineStatus.RUNNING,
    refetchInterval: props.refetchMs,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: false,
    structuralSharing: (oldData: any, newData: any) => {
      invariant(((v: any): v is PipelineMetrics | undefined => true)(oldData))
      invariant(((v: any): v is ControllerStatus | null => true)(newData))
      if (!newData) {
        setLastTimestamp(undefined)
        return emptyData
      }
      const now = Date.now()
      if (!lastTimestamp) {
        setLastTimestamp(now)
      }
      const globalWithTimestamp = { ...newData.global_metrics, timeMs: lastTimestamp ? now - lastTimestamp : 0 }

      return {
        input: new Map(
          newData.inputs.map(cs => tuple(normalizeCaseIndependentName({ name: cs.config.stream }), cs.metrics))
        ),
        output: new Map(
          newData.outputs.map(cs => tuple(normalizeCaseIndependentName({ name: cs.config.stream }), cs.metrics))
        ),
        global: reconcileHistoricData(oldData?.global ?? [], globalWithTimestamp, props.refetchMs, props.keepMs)
      } as any
    }
  })

  if (!pipelineStatsQuery.data) {
    return emptyData
  }
  return pipelineStatsQuery.data as unknown as PipelineMetrics
}

export const calcPipelineThroughput = (metrics: { global: (GlobalMetrics & { timeMs: number })[] }) => {
  const totalProcessed = metrics.global.map(m => tuple(m.timeMs, m.total_processed_records))
  const throughput = discreteDerivative(totalProcessed, (n1, n0) =>
    tuple(n1[0], ((n1[1] - n0[1]) * 1000) / (n1[0] - n0[0]))
  )
  const smoothTput = ((n1, n0) => n1[1] * 0.5 + n0[1] * 0.5)(
    throughput.at(-2) ?? throughput.at(-1) ?? tuple(0, 0),
    throughput.at(-1) ?? tuple(0, 0)
  )

  const valueMax = throughput.length ? Math.max(...throughput.map(v => v[1])) : 0
  const yMaxStep = Math.pow(10, Math.ceil(Math.log10(valueMax))) / 5
  const yMax = valueMax !== 0 ? Math.ceil((valueMax * 1.25) / yMaxStep) * yMaxStep : 100
  const yMin = 0
  return { throughput, smoothTput, yMin, yMax }
}
