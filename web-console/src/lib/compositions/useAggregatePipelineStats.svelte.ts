import { clearInterval, setInterval } from 'worker-timers'

import { getPipelineStats, type ExtendedPipeline } from '$lib/services/pipelineManager'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
  type PipelineMetrics
} from '$lib/functions/pipelineMetrics'
import { isMetricsAvailable } from '$lib/functions/pipelines/status'

let metrics: Record<string, PipelineMetrics> = {} // Disable reactivity for metrics data for better performance
let getMetrics = $state<() => typeof metrics>(() => metrics)

export const useAggregatePipelineStats = (
  pipeline: { current: ExtendedPipeline },
  refetchMs: number,
  keepMs?: number
) => {
  let pipelineStatus = $derived(pipeline.current.status)
  const doFetch = (pipelineName: string) => {
    if (!isMetricsAvailable(pipelineStatus)) {
      metrics[pipelineName] = emptyPipelineMetrics
      getMetrics = () => metrics
      return
    }
    getPipelineStats(pipelineName).then((stats) => {
      metrics[pipelineName] = accumulatePipelineMetrics(refetchMs, keepMs)(
        metrics[pipelineName],
        stats.status === 'not running' ? { status: null } : stats
      )
      getMetrics = () => metrics
    })
  }

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    const interval = setInterval(() => doFetch(pipelineName), refetchMs)
    doFetch(pipelineName)
    return () => {
      clearInterval(interval)
    }
  })
  return {
    get current() {
      return getMetrics()[pipelineName] ?? emptyPipelineMetrics
    }
  }
}
