import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { pipelineManagerAggregateQuery } from '$lib/services/pipelineManagerAggregateQuery'
import { GlobalMetrics, PipelineStatus } from '$lib/types/pipeline'

import { useQuery } from '@tanstack/react-query'

export function usePipelineMetrics(props: {
  pipelineName: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const pipelineStatsQuery = useQuery({
    ...pipelineManagerAggregateQuery.pipelineMetrics(props.pipelineName, props.refetchMs, props.keepMs),
    enabled: props.status === PipelineStatus.RUNNING,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: false
  })
  return pipelineStatsQuery.data
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
