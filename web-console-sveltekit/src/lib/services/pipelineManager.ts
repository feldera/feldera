import { handled } from '$lib/functions/request'
import {
  getPipeline as _getPipeline,
  getPipelineStats as _getPipelineStats,
  listPipelines,
  putPipeline as _putPipeline,
  patchPipeline as _patchPipeline,
  deletePipeline as _deletePipeline,
  type PipelineStatus as _PipelineStatus,
  type ProgramStatus,
  postPipelineAction as _postPipelineAction,
  type ErrorResponse,
  postPipeline as _postPipeline,
  type PipelineDescr,
  type PatchPipeline,
  getConfigAuthentication,
  type ExtendedPipelineDescr,
  listApiKeys,
  createApiKey,
  deleteApiKey as _deleteApiKey
} from '$lib/services/manager'
export type {
  PipelineDescr,
  ExtendedPipelineDescr,
  SqlCompilerMessage,
  InputEndpointConfig,
  OutputEndpointConfig,
  RuntimeConfig
} from '$lib/services/manager'
import { P, match } from 'ts-pattern'
import type { ControllerStatus } from '$lib/types/pipelineManager'

import { createClient } from '@hey-api/client-fetch'
import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'

const unauthenticatedClient = createClient({
  bodySerializer: JSONbig.stringify,
  responseTransformer: JSONbig.parse as any,
  baseUrl: felderaEndpoint
})

export type ExtendedPipelineDescrNoCode = Omit<ExtendedPipelineDescr, 'program_code'>

// const emptyProgramDescr: ProgramDescr = {
//   code: '',
//   config: {},
//   description: '',
//   name: '',
//   program_id: '',
//   schema: { inputs: [], outputs: [] },
//   status: 'Success',
//   version: -1
// }

const toPipelineThumb = (pipeline: Omit<ExtendedPipelineDescr, 'program_code'>) => ({
  name: pipeline.name,
  description: pipeline.description,
  runtimeConfig: pipeline.runtime_config,
  programConfig: pipeline.program_config,
  config: pipeline.runtime_config,
  status: consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_error
  )
})

export type PipelineThumb = ReturnType<typeof toPipelineThumb>

export const getPipeline = async (pipeline_name: string): Promise<PipelineDescr> => {
  const { description, name, program_code, program_config, runtime_config } = await handled(
    _getPipeline
  )({ path: { pipeline_name: encodeURIComponent(pipeline_name) } })
  return {
    description,
    name,
    program_code,
    program_config,
    runtime_config
  }
}

export const getExtendedPipeline = async (pipeline_name: string) => {
  const { program_status, deployment_status, deployment_error, ...pipeline } = await handled(
    _getPipeline
  )({ path: { pipeline_name: encodeURIComponent(pipeline_name) } })
  return {
    ...pipeline,
    status: consolidatePipelineStatus(program_status, deployment_status, deployment_error)
  }
}

/**
 * Fails if pipeline exists
 */
export const postPipeline = async (pipeline: PipelineDescr) => {
  return handled(_postPipeline)({ body: pipeline })
}

/**
 * Pipeline should already exist
 */
export const putPipeline = async (pipeline_name: string, newPipeline: PipelineDescr) => {
  await _putPipeline({
    body: newPipeline,
    path: { pipeline_name: encodeURIComponent(pipeline_name) }
  })
}

export const patchPipeline = async (pipeline_name: string, pipeline: PatchPipeline) => {
  await _patchPipeline({
    path: { pipeline_name: encodeURIComponent(pipeline_name) },
    body: pipeline
  })
}

export const getPipelines = async (): Promise<PipelineThumb[]> => {
  const pipelines = await handled(listPipelines)({ query: { code: false } })
  return pipelines.map(toPipelineThumb)
}

export const getPipelineStatus = async (pipeline_name: string) => {
  const pipeline = await handled(_getPipeline)({
    path: { pipeline_name: encodeURIComponent(pipeline_name) }
  })
  return {
    status: consolidatePipelineStatus(
      pipeline.program_status,
      pipeline.deployment_status,
      pipeline.deployment_error
    )
  }
}

export type PipelineStatus = ReturnType<typeof consolidatePipelineStatus>

export const getPipelineStats = async (pipeline_name: string) => {
  return handled(_getPipelineStats)({
    path: { pipeline_name: encodeURIComponent(pipeline_name) }
  }).then(
    (status) => ({
      pipelineName: pipeline_name,
      status: status as ControllerStatus | null
    }),
    (e) => {
      if (e.error_code === 'PipelineNotRunningOrPaused') {
        return {
          pipelineName: pipeline_name,
          status: 'not running' as const
        }
      }
      if (e instanceof TypeError && e.message === 'Failed to fetch') {
        return {
          pipelineName: pipeline_name,
          status: 'not running' as const
        }
      }
      throw e
    }
  )
}

const consolidatePipelineStatus = (
  programStatus: ProgramStatus,
  pipelineStatus: _PipelineStatus,
  pipelineError: ErrorResponse | null | undefined
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
  await handled(_deletePipeline)({ path: { pipeline_name } })
}

export const postPipelineAction = (pipeline_name: string, action: 'start' | 'pause' | 'shutdown') =>
  handled(_postPipelineAction)({ path: { pipeline_name, action } })

export const getAuthConfig = () =>
  handled(getConfigAuthentication)({ client: unauthenticatedClient })

export const getApiKeys = () => handled(listApiKeys)()

export const postApiKey = (name: string) => handled(createApiKey)({ body: { name } })

export const deleteApiKey = (name: string) =>
  handled(_deleteApiKey)({ path: { api_key_name: name } })
