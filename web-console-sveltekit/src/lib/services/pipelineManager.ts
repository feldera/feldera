import { handled } from '$lib/functions/request'
import {
  createOrReplaceProgram,
  deleteProgram,
  pipelineDelete,
  getPipeline,
  getProgram,
  listPipelines,
  newProgram,
  updatePipeline,
  type UpdatePipelineRequest,
  type PipelineStatus,
  type ProgramStatus,
  type Pipeline,
  type ProgramDescr,
  pipelineAction as _pipelineAction
} from '$lib/services/manager'
import { P, match } from 'ts-pattern'

const toFullPipeline = (pipeline: Pipeline, program?: ProgramDescr) => ({
  name: pipeline.descriptor.name,
  description: pipeline.descriptor.description,
  config: pipeline.descriptor.config,
  code: program?.code ?? '',
  schema: program?.schema ?? {
    inputs: [],
    outputs: []
  },
  _programName: pipeline.descriptor.program_name,
  _connectors: pipeline.descriptor.attached_connectors
})

export const getFullPipeline = async (pipeline_name: string) => {
  const pipeline = await handled(getPipeline)({ path: { pipeline_name } })
  const program = pipeline.descriptor.program_name
    ? await handled(getProgram)({
        path: { program_name: pipeline.descriptor.program_name },
        query: { with_code: true }
      })
    : undefined
  return toFullPipeline(pipeline, program)
}

export const getPipelines = async () => {
  const pipelines = await handled(listPipelines)()
  return pipelines.map((p) => toFullPipeline(p))
}

export const getPipelineStatus = async (pipeline_name: string) => {
  const pipeline = await handled(getPipeline)({ path: { pipeline_name } })
  const program = pipeline.descriptor.program_name
    ? await handled(getProgram)({
        path: { program_name: pipeline.descriptor.program_name },
        query: { with_code: true }
      })
    : undefined
  return {
    status: consolidatePipelineStatus(pipeline.state.current_status, program?.status ?? 'NoProgram')
  }
}

export type PipelineStatus = ReturnType<typeof consolidatePipelineStatus>

const consolidatePipelineStatus = (
  pipelineStatus: PipelineStatus,
  programStatus: ProgramStatus | 'NoProgram'
) => {
  const isError = P.union({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any })
  return match([pipelineStatus, programStatus])
    .with(['Shutdown', 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Shutdown', 'Pending'], () => 'Queued' as const)
    .with(['Shutdown', 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Shutdown', isError], () => 'Program err' as const)
    .with(['Shutdown', 'NoProgram'], () => 'No program' as const)
    .with(['Shutdown', 'Success'], () => 'Shutdown' as const)
    .with(['Provisioning', P._], () => 'Starting up' as const)
    .with(['Initializing', P._], () => 'Initializing' as const)
    .with(['Paused', 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Paused', 'Pending'], () => 'Queued' as const)
    .with(['Paused', 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Paused', isError], () => 'Program err' as const)
    .with(['Paused', P._], () => 'Paused' as const)
    .with(['Running', P._], () => 'Running' as const)
    .with(['ShuttingDown', P._], () => 'ShuttingDown' as const)
    .with(['Failed', P._], () => 'Failed' as const)
    .exhaustive()
}

export const deletePipeline = async (pipeline_name: string) => {
  const pipeline = await handled(getPipeline)({ path: { pipeline_name } })
  if (pipeline.descriptor.program_name) {
    await deleteProgram({ path: { program_name: pipeline.descriptor.program_name } })
  }
  await pipelineDelete({ path: { pipeline_name } })
}

export type FullPipeline = Awaited<ReturnType<typeof getFullPipeline>>

export const pipelineAction = (pipeline_name: string, action: 'start' | 'pause' | 'shutdown') =>
  handled(_pipelineAction)({ path: { pipeline_name, action } })
