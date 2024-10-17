import { clearInterval, setInterval } from 'worker-timers'

import { getPipelineStats, type ExtendedPipeline } from '$lib/services/pipelineManager'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
  type PipelineMetrics
} from '$lib/functions/pipelineMetrics'
import { isMetricsAvailable } from '$lib/functions/pipelines/status'
import { untrack } from 'svelte'

let metrics: Record<string, PipelineMetrics> = {} // Disable reactivity for metrics data for better performance
let getMetrics = $state<() => typeof metrics>(() => metrics)

export const useAggregatePipelineStats = (
  pipeline: { current: ExtendedPipeline },
  refetchMs: number,
  keepMs?: number
) => {
  let timeout = $state<NodeJS.Timeout>()
  let pipelineStatus = $derived(pipeline.current.status)

  let metricsAvailable = $derived(isMetricsAvailable(pipelineStatus))
  const doFetch = (pipelineName: string) => {
    if (metricsAvailable === 'no') {
      metrics[pipelineName] = emptyPipelineMetrics
      getMetrics = () => metrics
      return
    }
    if (metricsAvailable === 'soon') {
      timeout = setTimeout(() => doFetch(pipelineName), Math.max(0, refetchMs))
      getMetrics = () => metrics
      return
    }
    let requestTimestamp = Date.now()
    getPipelineStats(pipelineName).then((stats) => {
      let responseTimestamp = Date.now()
      metrics[pipelineName] = accumulatePipelineMetrics(
        (requestTimestamp + responseTimestamp) / 2,
        refetchMs,
        keepMs
      )(metrics[pipelineName], stats.status === 'not running' ? { status: null } : stats)
      getMetrics = () => metrics
      timeout = setTimeout(
        () => doFetch(pipelineName),
        Math.max(0, requestTimestamp + refetchMs - responseTimestamp)
      )
    })
  }

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    metricsAvailable
    untrack(() => doFetch(pipelineName))
    return () => {
      clearTimeout(timeout)
    }
  })
  return {
    get current() {
      return getMetrics()[pipelineName] ?? emptyPipelineMetrics
    }
  }
}
