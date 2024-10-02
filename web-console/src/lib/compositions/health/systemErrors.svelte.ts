import {
  getPipelineStats,
  type ExtendedPipeline,
  type Pipeline
} from '$lib/services/pipelineManager'
import type { ControllerStatus } from '$lib/types/pipelineManager'
import {
  extractPipelineErrors,
  extractPipelineXgressErrors,
  extractProgramErrors,
  type SystemError
} from './systemErrors'

export const listPipelineErrors = (pipeline: ExtendedPipeline) => {
  let programErrors = Object.values(
    extractProgramErrors(() => pipeline)({
      name: pipeline.name,
      status: pipeline.programStatus
    })
  ).flat(1)
  let pipelineErrors = extractPipelineErrors(pipeline)
  return {
    programErrors,
    pipelineErrors
  }
}

export const useSystemErrors = (_pipeline: { current: ExtendedPipeline }) => {
  let pipeline = $derived(_pipeline)
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
  let { programErrors, pipelineErrors } = $derived(listPipelineErrors(pipeline.current))
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
