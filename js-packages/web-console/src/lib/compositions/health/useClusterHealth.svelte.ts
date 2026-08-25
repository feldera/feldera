import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type ClusterEventType, toClusterStatus } from '$lib/functions/pipelines/health'

/**
 * The newest cluster monitor event the console has seen, and whether the API server still
 * vouched for it when it answered.
 */
export type ClusterHealthStatus = {
  api: ClusterEventType
  compiler: ClusterEventType
  runner: ClusterEventType
  /** When the event was recorded. */
  recordedAt: Date
  /**
   * The cluster monitor stopped writing events, so the statuses above are the last
   * recorded ones rather than current ones. The monitor runs within the Kubernetes runner.
   */
  stale: boolean
}

// Unknown until the first poll answers, and unknown again once the poller unmounts: a page
// that never polls, such as the profile viewer, must not present the cluster as healthy.
let status = $state<ClusterHealthStatus | undefined>(undefined)

const POLL_INTERVAL_MS = 10_000

/**
 * Poll cluster health every 10 seconds, with an immediate first call.
 *
 * The verdict comes from the server, which owns both the clock and the threshold, and it
 * describes the moment the response was sent. That holds for as long as the responses keep
 * arriving; an API server that stops answering leaves this state untouched and the console
 * reports it through `isNetworkHealthy` instead.
 *
 * A single instance of this hook should be mounted at one time (the `(shell)` layout owns
 * it); consumers read the state via {@link useClusterHealth}.
 *
 * The verdict lasts as long as the polling does. Unmounting means the user left the app
 * shell, for the profile viewer or the tenant picker, and no poller is left to correct what
 * the last one read, while the profile menu goes on showing it on every page.
 */
export const useRefreshClusterHealth = () => {
  const api = usePipelineManager()
  // This hook's own lifetime. The request in flight at unmount still resolves, and publishing
  // that answer would write the verdict straight back over the reset below.
  let isPolling = true
  $effect(() => () => {
    isPolling = false
    status = undefined
  })
  useInterval(async () => {
    const event = await api.getClusterEvent('latest')
    if (!isPolling) {
      return
    }
    status = toClusterStatus(event)
  }, POLL_INTERVAL_MS)
}

export const useClusterHealth = () => ({
  get current() {
    return status
  }
})
