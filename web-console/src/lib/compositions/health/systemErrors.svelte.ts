import {
  getPipelineStats,
  type ExtendedPipeline,
  type Pipeline
} from '$lib/services/pipelineManager'
import type { ControllerStatus } from '$lib/types/pipelineManager'
import {
  extractPipelineErrors,
  extractPipelineXgressErrors,
  extractProgramError,
  type SystemError
} from './systemErrors'

export const useSystemErrors = (pipeline: { current: ExtendedPipeline }) => {
  let stats = $state<{ pipelineName: string; status: ControllerStatus | null | 'not running' }>({
    pipelineName: pipeline.current.name,
    status: null
  })
  const reloadStats = async (pipelineName: string) => {
    if (pipeline.current.status !== 'Running' && pipeline.current.status !== 'Paused') {
      return
    }
    stats = await getPipelineStats(pipelineName)
  }
  let programErrors = $derived(
    extractProgramError(() => pipeline.current)({
      name: pipeline.current.name,
      status: pipeline.current.programStatus
    })
  )
  let pipelineErrors = $derived(extractPipelineErrors(pipeline.current))
  let xgressErrors = $derived(extractPipelineXgressErrors(stats))
  {
    let pipelineName = $derived(pipeline.current.name)
    $effect(() => {
      let interval = setInterval(() => {
        reloadStats(pipelineName)
      }, 2000)
      reloadStats(pipelineName)
      return () => {
        clearInterval(interval)
      }
    })
  }
  let systemErrors = $derived(
    ([] as SystemError<any, any>[]).concat(programErrors, pipelineErrors, xgressErrors)
  )

  return {
    get current() {
      return systemErrors
    }
  }
}
