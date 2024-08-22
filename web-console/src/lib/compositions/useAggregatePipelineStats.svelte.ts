import { clearInterval, setInterval } from 'worker-timers'

import { getPipelineStats } from '$lib/services/pipelineManager'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
} from '$lib/functions/pipelineMetrics'

export const useAggregatePipelineStats = (
  pipelineName: string,
  refetchMs: number,
  keepMs?: number
) => {
  let metrics = $state(emptyPipelineMetrics)

  const doFetch = (pipelineName: string) => {
    getPipelineStats(pipelineName).then((stats) => {
      metrics = accumulatePipelineMetrics(refetchMs, keepMs)(
        metrics,
        stats.status === 'not running' ? { status: null } : stats
      )
    })

  }

  $effect(() => {
    metrics = emptyPipelineMetrics
    const interval = setInterval(() => doFetch(pipelineName), refetchMs)
    doFetch(pipelineName)
    return () => {
      clearInterval(interval)
    }
  })
  return {
    get metrics() {
      return metrics
    }
  }
}
