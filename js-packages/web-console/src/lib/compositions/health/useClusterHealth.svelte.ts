import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type ClusterEventType, toEventType } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = typeof status

let status = $state({
  api: 'healthy' as ClusterEventType,
  compiler: 'healthy' as ClusterEventType,
  runner: 'healthy' as ClusterEventType,
  /**
   * The cluster monitor stopped writing events, so the statuses above are the last
   * recorded ones rather than current ones. The monitor runs within the Kubernetes runner.
   */
  stale: false,
  /** When the newest cluster monitor event was recorded, or null before the first poll. */
  recordedAt: null as Date | null
})

/**
 * Poll cluster health every 10 seconds (with an immediate first call) and
 * publish the result to the module-level `status` store. A single instance of
 * this hook should be mounted at one time (the `(authorized)` layout owns it);
 * consumers read the state via {@link useClusterHealth}.
 */
export const useRefreshClusterHealth = () => {
  const api = usePipelineManager()
  useInterval(async () => {
    const event = await api.getClusterEvent('latest')
    status = {
      api: toEventType(event.api_status),
      compiler: toEventType(event.compiler_status),
      runner: toEventType(event.runner_status),
      // Absent on anything but the latest event, which is what this polls.
      stale: event.stale ?? false,
      recordedAt: new Date(event.recorded_at)
    }
  }, 10000)
}

export const useClusterHealth = () => ({
  get current() {
    return status
  }
})
