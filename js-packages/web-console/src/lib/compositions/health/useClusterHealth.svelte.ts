import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
import { type ClusterEventType, toEventType } from '$lib/functions/pipelines/health'

export type ClusterHealthStatus = {
  api: ClusterEventType
  compiler: ClusterEventType
  runner: ClusterEventType
}

// Unknown until the first poll answers, and unknown again once the poller unmounts: a page
// that never polls, such as the profile viewer, must not present the cluster as healthy.
let status = $state<ClusterHealthStatus | undefined>(undefined)

/**
 * Poll cluster health every 10 seconds (with an immediate first call) and
 * publish the result to the module-level `status` store. A single instance of
 * this hook should be mounted at one time (the `(shell)` layout owns it);
 * consumers read the state via {@link useClusterHealth}.
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
