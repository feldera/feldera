import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import {
  ControllerStatus,
  GlobalMetrics,
  InputEndpointMetrics,
  OutputEndpointMetrics,
  PipelineStatus
} from '$lib/types/pipeline'
import { useState } from 'react'

import { useQuery } from '@tanstack/react-query'

const toMetrics = (data: ControllerStatus, { timeMs }: { timeMs: number }) => ({
  input: new Map(data.inputs.map(cs => tuple(cs.config.stream, cs.metrics))),
  output: new Map(data.outputs.map(cs => tuple(cs.config.stream, cs.metrics))),
  global: [{ ...data.global_metrics, timeMs }]
})

const emptyData = {
  input: new Map<string, InputEndpointMetrics>(),
  output: new Map<string, OutputEndpointMetrics>(),
  global: []
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
    structuralSharing: (oldData: any, _newData: any) => {
      if (!_newData) {
        setLastTimestamp(undefined)
        return emptyData
      }
      const now = Date.now()
      if (!lastTimestamp) {
        setLastTimestamp(now)
      }
      const newData = toMetrics(_newData, { timeMs: lastTimestamp ? now - lastTimestamp : 0 })
      const oldGlobal: (GlobalMetrics & { timeMs: number })[] = oldData?.global ?? []

      // clear metrics history if we get a timestamp that overwrites existing data point
      const isOverwritingTimestamp = !!oldGlobal.find(m => m.timeMs >= newData.global[0].timeMs)

      const sliceAt = isOverwritingTimestamp
        ? oldGlobal.length
        : nonNull(props.keepMs)
          ? -Math.ceil(props.keepMs / props.refetchMs)
          : -oldGlobal.length
      return {
        input: newData.input,
        output: newData.output,
        // global includes one more element than needed to satisfy keepMs when applying dicreteDerivative to data series
        global: [...oldGlobal.slice(sliceAt), newData.global[0]]
      } as any
    }
  })

  if (!pipelineStatsQuery.data) {
    return emptyData
  }
  return pipelineStatsQuery.data as unknown as ReturnType<typeof toMetrics>
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
