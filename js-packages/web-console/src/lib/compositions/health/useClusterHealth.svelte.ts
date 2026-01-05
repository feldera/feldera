import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type HealthEventType, toEventType } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = typeof status

let status = $state({
  api: 'healthy' as HealthEventType,
  compiler: 'healthy' as HealthEventType,
  runner: 'healthy' as HealthEventType
})

export const useClusterHealth = () => {
  const api = usePipelineManager()
  useInterval(async () => {
    const event = await api.getClusterEvent('latest')
    status = {
      api: toEventType(event.api_status),
      compiler: toEventType(event.compiler_status),
      runner: toEventType(event.runner_status)
    }
  }, 10000)
  return {
    get current() {
      return status
    }
  }
}
