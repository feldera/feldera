import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type ClusterEventType, toClusterStatus } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = typeof status

/**
 * The newest cluster monitor event the console has seen, and whether the API server still
 * vouched for it when it answered.
 */
let status = $state({
  api: 'healthy' as ClusterEventType,
  compiler: 'healthy' as ClusterEventType,
  runner: 'healthy' as ClusterEventType,
  /** When the event was recorded, or null before the first poll. */
  recordedAt: null as Date | null,
  /**
   * The cluster monitor stopped writing events, so the statuses above are the last
   * recorded ones rather than current ones. The monitor runs within the Kubernetes runner.
   */
  stale: false
})

const POLL_INTERVAL_MS = 10_000

/**
 * Poll cluster health every 10 seconds, with an immediate first call.
 *
 * The verdict comes from the server, which owns both the clock and the threshold, and it
 * describes the moment the response was sent. That holds for as long as the responses keep
 * arriving; an API server that stops answering leaves this state untouched and the console
 * reports it through `isNetworkHealthy` instead.
 *
 * A single instance of this hook should be mounted at one time (the `(authorized)` layout
 * owns it); consumers read the state via {@link useClusterHealth}.
 */
export const useRefreshClusterHealth = () => {
  const api = usePipelineManager()
  useInterval(async () => {
    status = toClusterStatus(await api.getClusterEvent('latest'))
  }, POLL_INTERVAL_MS)
}

export const useClusterHealth = () => ({
  get current() {
    return status
  }
})
