import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type HealthEventType, toEventType } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = typeof status

let status = $state({
  api: 'healthy' as HealthEventType,
  compiler: 'healthy' as HealthEventType,
  runner: 'healthy' as HealthEventType
})

/**
 * Poll cluster health every 10 seconds (with an immediate first call) and
 * publish the result to the module-level `status` store. A single instance of
 * this hook should be mounted at one time (the authenticated layout owns it);
 * consumers read the state via {@link useClusterHealth}.
 */
export const useRefreshClusterHealth = () => {
  const api = usePipelineManager()
  useInterval(async () => {
    const event = await api.getClusterEvent('latest')
    status = {
      api: toEventType(event.api_status),
      compiler: toEventType(event.compiler_status),
      runner: toEventType(event.runner_status)
    }
  }, 10000)
}

export const useClusterHealth = () => ({
  get current() {
    return status
  }
})
