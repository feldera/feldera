import { getPipelineStats, type ExtendedPipeline } from '$lib/services/pipelineManager'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
  type PipelineMetrics
} from '$lib/functions/pipelineMetrics'
import { isMetricsAvailable } from '$lib/functions/pipelines/status'
import { untrack } from 'svelte'
import { useToast } from './useToastNotification'
import { closedIntervalAction } from '$lib/functions/common/promise'

let metrics: Record<string, PipelineMetrics> = {} // Disable reactivity for metrics data for better performance
let getMetrics = $state<() => typeof metrics>(() => metrics)

export const useAggregatePipelineStats = (
  pipeline: { current: ExtendedPipeline },
  refetchMs: number,
  keepMs?: number
) => {
  let pipelineStatus = $derived(pipeline.current.status)

  let metricsAvailable = $derived(isMetricsAvailable(pipelineStatus))
  const { toastError } = useToast()
  const doFetch = (pipelineName: string) => {
    if (metricsAvailable === 'no') {
      metrics[pipelineName] = emptyPipelineMetrics
      getMetrics = () => metrics
      return Promise.resolve()
    }
    if (metricsAvailable === 'soon') {
      getMetrics = () => metrics
      return Promise.resolve()
    }
    let requestTimestamp = Date.now()
    return getPipelineStats(pipelineName).then((stats) => {
      let responseTimestamp = Date.now()
      metrics[pipelineName] = accumulatePipelineMetrics(
        (requestTimestamp + responseTimestamp) / 2,
        refetchMs,
        keepMs
      )(metrics[pipelineName], stats.status === 'not running' ? { status: null } : stats)
      getMetrics = () => metrics
    }, toastError)
  }

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    metricsAvailable
    const cancel = untrack(() => closedIntervalAction(() => doFetch(pipelineName), refetchMs))
    return () => {
      cancel()
    }
  })
  return {
    get current() {
      return getMetrics()[pipelineName] ?? emptyPipelineMetrics
    }
  }
}
