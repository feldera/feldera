import { getPipelineStatus } from '$lib/services/pipelineManager'
import { asyncDerived, asyncReadable, type Readable } from '@square/svelte-store'
import { Store } from 'runed'

export const usePipelineStatus = (pipelineName: Readable<string>) => {
  const status = asyncDerived(pipelineName, (pipelineName) => getPipelineStatus(pipelineName), {
    reloadable: true,
    initial: { status: 'Initializing' as const }
  })
  $effect(() => {
    let interval = setInterval(() => status.reload?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
  $effect(() => {
    pipelineName
    status.reload?.()
  })
  const store = new Store(status)
  const reload = status.reload!
  Object.assign(store, { reload })
  return store as typeof store & { reload: typeof reload }
}
