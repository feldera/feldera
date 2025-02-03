import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
import { onMount } from 'svelte'
import { useToast } from '$lib/compositions/useToastNotification'

let pipelines = $state<PipelineThumb[] | undefined>(undefined)
const reload = async () => {
  pipelines = await getPipelines()
}

export const useUpdatePipelineList = () => {
  return {
    updatePipelines(updater: (ps: PipelineThumb[]) => PipelineThumb[]) {
      pipelines = updater(pipelines ?? [])
    }
  }
}

/**
 * Start calling an action in a loop with an interval while waiting for the previous invocation to resolve
 * Does not perform the action on the initial call
 * Does not handle rejection
 * @returns A function to cancel the action loop
 */
const closedIntervalAction = (action: () => Promise<void>, periodMs: number) => {
  let timeout: NodeJS.Timeout | undefined
  let onTimeoutReject: () => void
  let t1 = Date.now()
  setTimeout(async () => {
    do {
      await new Promise((resolve, reject) => {
        const t2 = Date.now()
        timeout = setTimeout(resolve, Math.max(Math.min(periodMs - t2 + t1, periodMs), 0))
        onTimeoutReject = reject
      })
      t1 = Date.now()
      await action()
    } while (timeout)
  })
  return () => {
    clearTimeout(timeout)
    timeout = undefined
    onTimeoutReject()
  }
}

export const useRefreshPipelineList = () => {
  const { toastError } = useToast()
  onMount(() => {
    return closedIntervalAction(() => reload().catch(toastError), 2000)
  })
}

export const usePipelineList = (preloaded?: { pipelines: PipelineThumb[] }) => {
  if (preloaded && !pipelines) {
    pipelines = preloaded.pipelines
  }
  return {
    get pipelines() {
      return [...(pipelines ?? [])]
    },
    set pipelines(ps: PipelineThumb[]) {
      pipelines = ps
    }
  }
}
