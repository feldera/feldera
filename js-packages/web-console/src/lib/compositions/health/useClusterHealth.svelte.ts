import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type ClusterEventType, toEventType } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = typeof status

let status = $state({
  api: 'healthy' as ClusterEventType,
  compiler: 'healthy' as ClusterEventType,
  runner: 'healthy' as ClusterEventType
})

/**
 * Poll cluster health every 10 seconds (with an immediate first call) and
 * publish the result to the module-level `status` store. A single instance of
 * this hook should be mounted at one time (the authenticated layout owns it);
 * consumers read the state via {@link useClusterHealth}.
 *
 * `shouldPoll` is consulted on every tick, because the owning layout persists
 * across states in which polling must stop (e.g. no acting tenant resolved).
 */
export const useRefreshClusterHealth = (shouldPoll?: () => boolean) => {
  const api = usePipelineManager()
  useInterval(async () => {
    if (shouldPoll?.() === false) {
      return
    }
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
