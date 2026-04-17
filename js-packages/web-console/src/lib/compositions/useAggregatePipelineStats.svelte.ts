import { untrack } from 'svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { closedIntervalAction } from '$lib/functions/common/promise'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
  type PipelineMetrics
} from '$lib/functions/pipelineMetrics'
import { isMetricsAvailable } from '$lib/functions/pipelines/status'
import type { ExtendedPipeline } from '$lib/services/pipelineManager'

const metrics: Record<string, PipelineMetrics> = {} // Disable reactivity for metrics data for better performance
let getMetrics = $state<() => typeof metrics>(() => metrics)

export const useAggregatePipelineStats = (
  pipeline: { current: ExtendedPipeline },
  refetchMs: number,
  keepMs?: number,
  options?: { getDeleted?: () => boolean }
) => {
  const pipelineStatus = $derived(pipeline.current.status)

  const metricsAvailable = $derived(isMetricsAvailable(pipelineStatus))
  const api = usePipelineManager()

  const doFetch = async (pipelineName: string) => {
    if (metricsAvailable === 'no') {
      metrics[pipelineName] = emptyPipelineMetrics
      getMetrics = () => metrics
      return
    }
    if (metricsAvailable === 'soon') {
      const data = accumulatePipelineMetrics(Date.now())(metrics[pipelineName], { status: null })
      if (!data) {
        return
      }
      metrics[pipelineName] = data
      getMetrics = () => metrics
      return
    }
    if (metricsAvailable === 'missing') {
      getMetrics = () => metrics
      return
    }
    const requestTimestamp = Date.now()
    const stats = await api.getPipelineStats(pipelineName)
    const responseTimestamp = Date.now()
    const data = accumulatePipelineMetrics((requestTimestamp + responseTimestamp) / 2)(
      metrics[pipelineName],
      stats.status === 'not running' ? { status: null } : { status: stats.status }
    )
    if (!data) {
      return
    }
    metrics[pipelineName] = data
    getMetrics = () => metrics
  }

  const pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    metricsAvailable
    if (options?.getDeleted?.()) return
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
