import { clearInterval, setInterval } from 'worker-timers'

import { getPipelineStats, type ExtendedPipeline } from '$lib/services/pipelineManager'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
} from '$lib/functions/pipelineMetrics'
import { isMetricsAvailable } from '$lib/functions/pipelines/status'

export const useAggregatePipelineStats = (
  pipeline: {current: ExtendedPipeline},
  refetchMs: number,
  keepMs?: number
) => {
  let metrics = $state(emptyPipelineMetrics)

  let pipelineStatus = $derived(pipeline.current.status)
  const doFetch = (pipelineName: string) => {
    if (!isMetricsAvailable(pipelineStatus)) {
      metrics = emptyPipelineMetrics
      return
    }
    getPipelineStats(pipelineName).then((stats) => {
      metrics = accumulatePipelineMetrics(refetchMs, keepMs)(
        metrics,
        stats.status === 'not running' ? { status: null } : stats
      )
    })

  }

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    metrics = emptyPipelineMetrics
    const interval = setInterval(() => doFetch(pipelineName), refetchMs)
    doFetch(pipelineName)
    return () => {
      clearInterval(interval)
    }
  })
  return {
    get current() {
      return metrics
    }
  }
}
