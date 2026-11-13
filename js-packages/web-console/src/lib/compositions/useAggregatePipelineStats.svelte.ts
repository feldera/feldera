import { closedIntervalAction } from 'common-lib/promise'
import { untrack } from 'svelte'
import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
import {
  accumulatePipelineMetrics,
  emptyPipelineMetrics,
  type PipelineMetrics
} from '#lib/functions/pipelineMetrics.ts'
import { isMetricsAvailable } from '#lib/functions/pipelines/status.ts'
import type { ExtendedPipeline } from '#lib/services/pipelineManager.ts'

/** The time between two polls of the pipeline statistics. The views that animate the statistics use it too. */
export const PIPELINE_STATS_POLL_MS = 2000

// Keyed by pipeline id, so a rename keeps the history
const metrics: Record<string, PipelineMetrics> = {} // Disable reactivity for metrics data for better performance
let getMetrics = $state<() => typeof metrics>(() => metrics)

export const useAggregatePipelineStats = (
  pipeline: { current: ExtendedPipeline | undefined },
  refetchMs: number,
  keepMs?: number,
  options?: { getDeleted?: () => boolean }
) => {
  const pipelineStatus = $derived(pipeline.current?.status)

  const metricsAvailable = $derived(pipelineStatus ? isMetricsAvailable(pipelineStatus) : 'no')
  const api = usePipelineManager()

  const doFetch = async ({ id, name }: { id: string; name: string }) => {
    if (metricsAvailable === 'no') {
      metrics[id] = emptyPipelineMetrics
      getMetrics = () => metrics
      return
    }
    if (metricsAvailable === 'soon') {
      const data = accumulatePipelineMetrics(Date.now())(metrics[id], { status: null })
      if (!data) {
        return
      }
      metrics[id] = data
      getMetrics = () => metrics
      return
    }
    if (metricsAvailable === 'missing') {
      getMetrics = () => metrics
      return
    }
    const requestTimestamp = Date.now()
    const stats = await api.getPipelineStats(name)
    const responseTimestamp = Date.now()
    const data = accumulatePipelineMetrics((requestTimestamp + responseTimestamp) / 2)(
      metrics[id],
      stats.status === 'not running' ? { status: null } : { status: stats.status }
    )
    if (!data) {
      return
    }
    metrics[id] = data
    getMetrics = () => metrics
  }

  const pipelineId = $derived(pipeline.current?.id)
  const pipelineName = $derived(pipeline.current?.name)
  $effect(() => {
    if (!pipelineId || !pipelineName) {
      return
    }
    const target = { id: pipelineId, name: pipelineName }
    metricsAvailable
    if (options?.getDeleted?.()) {
      return
    }
    const cancel = untrack(() => closedIntervalAction(() => doFetch(target), refetchMs))
    return () => {
      cancel()
    }
  })
  return {
    get current() {
      return pipelineId ? (getMetrics()[pipelineId] ?? emptyPipelineMetrics) : undefined
    }
  }
}
