import type { PipelineStatus } from '$lib/services/pipelineManager'
import { getPipelineStats } from '$lib/services/pipelineManager'
import { accumulatePipelineMetrics, emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

export const useAggregatePipelineStats = (pipelineName: string,
  status: PipelineStatus,
  refetchMs: number,
  keepMs?: number) => {
  let metrics = $state(emptyPipelineMetrics)

  const doFetch = (pipelineName: string) => getPipelineStats(pipelineName).then(stats => {
    metrics = accumulatePipelineMetrics(refetchMs, keepMs)(metrics, stats.status === 'not running' ? { status: null} : stats)
  })

  $effect(() => {
    metrics = emptyPipelineMetrics
    const timeout = setInterval(() => doFetch(pipelineName), refetchMs)
    return () => clearInterval(timeout)
  })
  return { get metrics () { return metrics } }
}