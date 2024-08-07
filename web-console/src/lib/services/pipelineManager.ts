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
  deleteApiKey as _deleteApiKey,
  httpOutput,
  getConfigDemos
} from '$lib/services/manager'
export type {
  // PipelineDescr,
  // ExtendedPipelineDescr,
  SqlCompilerMessage,
  InputEndpointConfig,
  OutputEndpointConfig,
  RuntimeConfig
} from '$lib/services/manager'
import { P, match } from 'ts-pattern'
import type { ControllerStatus } from '$lib/types/pipelineManager'
export type { ProgramSchema } from '$lib/services/manager'

import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient } = AxaOidc

import { client, createClient } from '@hey-api/client-fetch'
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
  status: consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_desired_status,
    pipeline.deployment_error
  )
})

const toPipeline = (pipeline: ExtendedPipelineDescr) => ({
  name: pipeline.name,
  description: pipeline.description,
  runtimeConfig: pipeline.runtime_config,
  programConfig: pipeline.program_config,
  programCode: pipeline.program_code
})

const fromPipeline = <T extends Partial<Pipeline>>(pipeline: T) => ({
  name: pipeline?.name,
  description: pipeline?.description,
  runtime_config: pipeline?.runtimeConfig,
  program_config: pipeline?.programConfig,
  program_code: pipeline?.programCode
})

export type PipelineThumb = ReturnType<typeof toPipelineThumb>

export type Pipeline = ReturnType<typeof toPipeline>

export const getPipeline = async (pipeline_name: string) => {
  return handled(_getPipeline)({ path: { pipeline_name: encodeURIComponent(pipeline_name) } }).then(
    toPipeline
  )
}

const toExtendedPipeline = <
  Pipeline extends {
    program_status: ProgramStatus
    deployment_status: _PipelineStatus
    deployment_desired_status: _PipelineStatus
    deployment_error?: ErrorResponse | null | undefined
  }
>({
  program_status,
  deployment_status,
  deployment_desired_status,
  deployment_error,
  ...pipeline
}: Pipeline) => ({
  ...pipeline,
  status: consolidatePipelineStatus(
    program_status,
    deployment_status,
    deployment_desired_status,
    deployment_error
  )
})

export const getExtendedPipeline = async (pipeline_name: string) => {
  return handled(_getPipeline)({ path: { pipeline_name: encodeURIComponent(pipeline_name) } }).then(
    toExtendedPipeline
  )
}

/**
 * Fails if pipeline exists
 */
export const postPipeline = async (pipeline: PipelineDescr) => {
  if (!pipeline.name) {
    throw new Error('Cannot create pipeline with empty name')
  }
  return handled(_postPipeline)({ body: pipeline }).then(toPipelineThumb)
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

export const patchPipeline = async (pipeline_name: string, pipeline: Partial<Pipeline>) => {
  await handled(_patchPipeline)({
    path: { pipeline_name: encodeURIComponent(pipeline_name) },
    body: fromPipeline(pipeline)
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
      pipeline.deployment_desired_status,
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
  desiredStatus: _PipelineStatus,
  pipelineError: ErrorResponse | null | undefined
) => {
  return match([pipelineStatus, desiredStatus, pipelineError, programStatus])
    .with(['Shutdown', P.any, P.nullish, 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Shutdown', P.any, P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Shutdown', P.any, P.nullish, 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Shutdown', P.any, P.nullish, { SqlError: P.select() }], (SqlError) => ({ SqlError }))
    .with(['Shutdown', P.any, P.nullish, { RustError: P.select() }], (RustError) => ({ RustError }))
    .with(['Shutdown', P.any, P.nullish, { SystemError: P.select() }], (SystemError) => ({
      SystemError
    }))
    .with(['Shutdown', 'Running', P.any, P._], () => 'Starting up' as const) // Workaround when fetching status right after POST start action
    .with(['Provisioning', 'Running', P.any, P._], () => 'Starting up' as const) // Workaround when fetching status right after POST start action
    .with(['Shutdown', 'Paused', P.any, P._], () => 'Starting up' as const) // Workaround when fetching status right after POST start_paused action
    .with(['Shutdown', 'Shutdown', P.nullish, 'Success'], () => 'Shutdown' as const)
    .with(['Shutdown', 'Shutdown', P.select(P.nonNullable), P.any], () => 'Shutdown' as const)
    .with(['Provisioning', P.any, P.nullish, P._], () => 'Starting up' as const)
    .with(['Initializing', P.any, P.nullish, P._], () => 'Initializing' as const)
    .with(['Paused', P.any, P.nullish, 'CompilingSql'], () => 'Compiling sql' as const)
    .with(['Paused', P.any, P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Paused', P.any, P.nullish, 'CompilingRust'], () => 'Compiling bin' as const)
    .with(['Paused', 'Running', P.nullish, P._], () => 'Running' as const)
    .with(['Paused', P.any, P.nullish, P._], () => 'Paused' as const)
    .with(['Running', P.any, P.nullish, P._], () => 'Running' as const)
    .with(['ShuttingDown', P.any, P.nullish, P._], () => 'ShuttingDown' as const)
    .with(['Failed', P.any, P.select(P.nonNullable), P._], (PipelineError) => ({ PipelineError }))
    .otherwise(() => {
      throw new Error(
        `Unable to consolidatePipelineStatus: ${pipelineStatus} ${desiredStatus} ${pipelineError} ${programStatus}`
      )
    })
}

export const deletePipeline = async (pipeline_name: string) => {
  await handled(_deletePipeline)({ path: { pipeline_name } })
}

export type PipelineAction = 'start' | 'pause' | 'shutdown' | 'start_paused'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export const postPipelineAction = async (
  pipeline_name: string,
  action: PipelineAction
): Promise<() => Promise<void>> => {
  await handled(_postPipelineAction)({
    path: { pipeline_name, action: action === 'start_paused' ? 'pause' : action }
  })
  return async () => {
    const desiredStatus = (
      {
        start: 'Running',
        pause: 'Paused',
        shutdown: 'Shutdown',
        start_paused: 'Paused'
      } satisfies Record<PipelineAction, PipelineStatus>
    )[action]
    const ignoreStatuses = [
      'Initializing',
      'Compiling bin',
      'Compiling sql',
      'Queued',
      'Starting up'
    ] as PipelineStatus[]
    while (true) {
      await sleep(300)
      const status = (await getPipelineStatus(pipeline_name)).status
      if (status === desiredStatus) {
        break
      }
      if (ignoreStatuses.includes(status)) {
        continue
      }
      throw new Error(
        `Unexpected status ${status} while waiting for pipeline ${pipeline_name} to complete action ${action}`
      )
    }
    return
  }
}

export const getAuthConfig = () =>
  handled(getConfigAuthentication)({ client: unauthenticatedClient })

export const getApiKeys = () => handled(listApiKeys)()

export const postApiKey = (name: string) => handled(createApiKey)({ body: { name } })

export const deleteApiKey = (name: string) =>
  handled(_deleteApiKey)({ path: { api_key_name: name } })

export const relationEggressStream = async (pipelineName: string, relationName: string) => {
  // const result = await httpOutput({path: {pipeline_name: pipelineName, table_name: relationName}, query: {'format': 'json', 'mode': 'watch', 'array': false, 'query': 'table'}})
  const fetch = (() => {
    try {
      const oidcClient = OidcClient.get()
      return oidcClient.fetchWithTokens(globalThis.fetch)
    } catch {
      return globalThis.fetch
    }
  })()
  const result = await fetch(
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/egress/${relationName}?format=json&mode=watch&array=false&query=table`,
    {
      method: 'POST'
    }
  )
  return result.body
}

export const getDemos = handled(getConfigDemos)
