import { getPipelineStatus, type PipelineStatus } from '$lib/services/pipelineManager'

export const usePipelineStatus = (pipelineName: { current: string }) => {
  let status = $state({ status: 'Initializing' as PipelineStatus })
  const reload = async () => {
    status.status = (await getPipelineStatus(pipelineName.current)).status
  }
  $effect(() => {
    let interval = setInterval(() => reload(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
  reload()
  $effect(() => {
    pipelineName
    reload()
  })
  return {
    get status() {
      return status.status
    },
    set status(s: PipelineStatus) {
      status.status = s
    }
  }
}
