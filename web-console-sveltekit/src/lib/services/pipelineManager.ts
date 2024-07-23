import { handled } from '$lib/functions/request'
import {
  createOrReplaceProgram,
  deleteProgram,
  pipelineDelete,
  getPipeline,
  getProgram,
  listPipelines,
  newProgram,
  updatePipeline as _updatePipeline,
  type UpdatePipelineRequest,
  type PipelineStatus as _PipelineStatus,
  type ProgramStatus,
  type Pipeline,
  type ProgramDescr,
  pipelineAction as _pipelineAction,
  pipelineStats,
  type ErrorResponse,
  getPrograms,
  updateProgram,
  getAuthenticationConfig,
  type NewPipelineRequest,
  newPipeline
} from '$lib/services/manager'
import { P, match } from 'ts-pattern'
import { leftJoin } from 'array-join'
import type { ControllerStatus } from '$lib/types/pipelineManager'

const emptyProgramDescr: ProgramDescr = {
  code: '',
  config: {},
  description: '',
  name: '',
  program_id: '',
  schema: { inputs: [], outputs: [] },
  status: 'Success',
  version: -1
}

const toPipelineThumb = (pipeline: Pipeline, program: ProgramDescr) => ({
  name: pipeline.descriptor.name,
  description: pipeline.descriptor.description,
  config: pipeline.descriptor.config,
  status: consolidatePipelineStatus(
    pipeline.state.current_status,
    pipeline.state.error,
    program.status
  )
})

export type PipelineThumb = ReturnType<typeof toPipelineThumb>

const toFullPipeline = (pipeline: Pipeline, program: ProgramDescr) => ({
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

export type FullPipeline = ReturnType<typeof toFullPipeline>

export const getFullPipeline = async (pipeline_name: string) => {
  const pipeline = await handled(getPipeline)({ path: { pipeline_name } })
  const program = pipeline.descriptor.program_name
    ? await handled(getProgram)({
        path: { program_name: pipeline.descriptor.program_name },
        query: { with_code: true }
      })
    : emptyProgramDescr
  return toFullPipeline(pipeline, program)
}

/**
 * Fails if pipeline exists
 */
export const createPipeline = async (pipeline: NewPipelineRequest) => {
  return handled(newPipeline)({ body: pipeline })
}

/**
 * Pipeline should already exist
 */
export const updatePipeline = async (
  oldPipeline: { name: string; _programName: string | null | undefined } | undefined,
  newPipeline: FullPipeline
) => {
  const program_name =
    (oldPipeline?.name !== newPipeline.name ? undefined : newPipeline._programName) ??
    newPipeline.name + '_program'

  await createOrReplaceProgram({
    body: { code: newPipeline.code, description: '' },
    path: { program_name }
  })
  await _updatePipeline({
    body: ((p) =>
      ({
        name: p.name,
        description: p.description,
        connectors: p._connectors,
        config: p.config,
        program_name
      }) satisfies UpdatePipelineRequest)(newPipeline),
    path: { pipeline_name: oldPipeline!.name }
  })
  if (oldPipeline?._programName && oldPipeline._programName !== program_name) {
    await deleteProgram({ path: { program_name: oldPipeline._programName } })
  }
}

export const patchPipeline = async (
  pipelineName: string,
  pipeline: Omit<UpdatePipelineRequest, 'connectors'> & { code?: string }
) => {
  const { code, ...pipelinePatch } = pipeline
  await _updatePipeline({ body: pipelinePatch, path: { pipeline_name: pipelineName } })
  if (code) {
    await updateProgram({
      body: { code, name: (pipelinePatch.name || pipelineName) + '_program' },
      path: { program_name: pipelineName + '_program' }
    })
  }
}

export const getPipelines = async () => {
  const pipelines = await handled(listPipelines)()
  const programs = await handled(getPrograms)()
  return leftJoin(
    pipelines,
    programs,
    (p) => p.descriptor.program_name ?? p.descriptor.name + '_program',
    (p) => p.name,
    (pipeline, program) => toPipelineThumb(pipeline, program ?? emptyProgramDescr)
  )
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
    status: consolidatePipelineStatus(
      pipeline.state.current_status,
      pipeline.state.error,
      program?.status ?? 'Success'
    )
  }
}

export type PipelineStatus = ReturnType<typeof consolidatePipelineStatus>

export const getPipelineStats = async (pipeline_name: string) => {
  return handled(pipelineStats)({ path: { pipeline_name } }).then(
    (status) => ({
      pipelineName: pipeline_name,
      status: status as ControllerStatus | null
    }),
    (e) => {
      if (e.error_code !== 'PipelineShutdown') {
        throw new Error(e)
      }
      return {
        pipelineName: pipeline_name,
        status: 'not running' as const
      }
    }
  )
}

const consolidatePipelineStatus = (
  pipelineStatus: _PipelineStatus,
  pipelineError: ErrorResponse | null | undefined,
  programStatus: ProgramStatus
) => {
  return match([pipelineStatus, pipelineError, programStatus])
    .with(['Shutdown', P.nullish, 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Shutdown', P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Shutdown', P.nullish, 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Shutdown', P.nullish, { SqlError: P.select() }], (SqlError) => ({ SqlError }))
    .with(['Shutdown', P.nullish, { RustError: P.select() }], (RustError) => ({ RustError }))
    .with(['Shutdown', P.nullish, { SystemError: P.select() }], (SystemError) => ({ SystemError }))
    .with(['Shutdown', P.nullish, 'Success'], () => 'Shutdown' as const)
    .with(['Shutdown', P.select(P.nonNullable), P.any], () => 'Shutdown' as const)
    .with(['Provisioning', P.nullish, P._], () => 'Starting up' as const)
    .with(['Initializing', P.nullish, P._], () => 'Initializing' as const)
    .with(['Paused', P.nullish, 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Paused', P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Paused', P.nullish, 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Paused', P.nullish, P._], () => 'Paused' as const)
    .with(['Running', P.nullish, P._], () => 'Running' as const)
    .with(['ShuttingDown', P.nullish, P._], () => 'ShuttingDown' as const)
    .with(['Failed', P.select(P.nonNullable), P._], (PipelineError) => ({ PipelineError }))
    .otherwise(() => {
      throw new Error(
        `Unable to consolidatePipelineStatus: ${pipelineStatus} ${pipelineError} ${programStatus}`
      )
    })
}

export const deletePipeline = async (pipeline_name: string) => {
  const pipeline = await handled(getPipeline)({ path: { pipeline_name } })
  await pipelineDelete({ path: { pipeline_name } })
  if (pipeline.descriptor.program_name) {
    await deleteProgram({ path: { program_name: pipeline.descriptor.program_name } })
  }
}

export const pipelineAction = (pipeline_name: string, action: 'start' | 'pause' | 'shutdown') =>
  handled(_pipelineAction)({ path: { pipeline_name, action } })

export const getAuthConfig = () => handled(getAuthenticationConfig)()
