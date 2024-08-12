import { getPipelineStats, type ExtendedPipeline, type Pipeline } from "$lib/services/pipelineManager"
import type { ControllerStatus } from "$lib/types/pipelineManager"
import { extractPipelineErrors, extractPipelineXgressErrors, extractProgramError, type SystemError } from "./systemErrors"

export const useSystemErrors = (pipeline: ExtendedPipeline) => {
    let stats = $state<{pipelineName: string, status: ControllerStatus | null | 'not running'}>({pipelineName: pipeline.name, status: null})
    const reloadStats = async (pipelineName: string) => {
        stats = await getPipelineStats(pipelineName)
    }
    let programErrors = $derived(extractProgramError(() => pipeline)({name: pipeline.name, status: pipeline.programStatus}))
    let pipelineErrors = $derived(extractPipelineErrors(pipeline))
    let xgressErrors = $derived(extractPipelineXgressErrors(stats))
    $effect(() => {
        let interval = setInterval(() => {
            reloadStats(pipeline.name)
        }, 2000)
        reloadStats(pipeline.name)
        return () => {
            clearInterval(interval)
        }
    })
    let systemErrors = $derived(([] as SystemError<any, any>[]).concat(programErrors, pipelineErrors, xgressErrors))
    return systemErrors
  }