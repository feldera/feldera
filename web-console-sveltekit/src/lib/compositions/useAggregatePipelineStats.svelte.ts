import type { PipelineStatus } from '$lib/services/pipelineManager'
import { getPipelineStats } from '$lib/services/pipelineManager'
import { accumulatePipelineMetrics, emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

export const useAggregatePipelineStats = (pipelineName: string,
  refetchMs: number,
  keepMs?: number) => {
  let metrics = $state(emptyPipelineMetrics)

  const doFetch = (pipelineName: string) => getPipelineStats(pipelineName).then(stats => {
    console.log('doFetch')
    metrics = accumulatePipelineMetrics(refetchMs, keepMs)(metrics, stats.status === 'not running' ? { status: null} : stats)
  })

  $effect(() => {
    console.log('pipelineName updated!', pipelineName)
    // if (metrics.global.length) {
      metrics = emptyPipelineMetrics
    // }
    const timeout = setInterval(() => doFetch(pipelineName), refetchMs)
    // setTimeout(() => doFetch(pipelineName), 10)
    // setTimeout(() => doFetch(pipelineName), 10)
    doFetch(pipelineName)
    return () => {
      clearInterval(timeout)
    }
  })
  return { get metrics () { return metrics } }
}