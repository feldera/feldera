import { handled } from '$lib/functions/request'
import {
  getPipeline as _getPipeline,
  getPipelineStats as _getPipelineStats,
  listPipelines,
  putPipeline as _putPipeline,
  patchPipeline as _patchPipeline,
  deletePipeline as _deletePipeline,
  type PipelineStatus as _PipelineStatus,
  type ProgramStatus as _ProgramStatus,
  postPipelineAction as _postPipelineAction,
  type ErrorResponse,
  postPipeline as _postPipeline,
  type PipelineInfo,
  type PatchPipeline,
  getConfigAuthentication,
  type PipelineSelectedInfo,
  listApiKeys,
  postApiKey as _postApiKey,
  deleteApiKey as _deleteApiKey,
  httpOutput,
  getConfig as _getConfig,
  getConfigDemos,
  httpInput,
  type Field,
  type SqlCompilerMessage,
  type PostPutPipeline
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
import type { ControllerStatus, XgressRecord } from '$lib/types/pipelineManager'
export type { ProgramSchema } from '$lib/services/manager'
export type ProgramStatus = _ProgramStatus | { SqlWarning: SqlCompilerMessage[] }

import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient } = AxaOidc

import { client, createClient } from '@hey-api/client-fetch'
import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import invariant from 'tiny-invariant'
import { tuple } from '$lib/functions/common/tuple'

const unauthenticatedClient = createClient({
  bodySerializer: JSONbig.stringify,
  responseTransformer: JSONbig.parse as any,
  baseUrl: felderaEndpoint
})

type PipelineDescr = PostPutPipeline

type ExtendedPipelineDescr = PipelineSelectedInfo

export type ExtendedPipelineDescrNoCode = Omit<ExtendedPipelineDescr, 'program_code'>

const toPipelineThumb = (
  pipeline: Omit<ExtendedPipelineDescr, 'program_code' | 'udf_rust' | 'udf_toml'>
) => ({
  name: pipeline.name,
  description: pipeline.description,
  runtimeConfig: pipeline.runtime_config,
  programConfig: pipeline.program_config,
  status: consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_desired_status,
    pipeline.deployment_error
  ),
  programStatus: pipeline.program_status,
  refreshVersion: pipeline.refresh_version
})

const toPipeline = <
  P extends Omit<PipelineDescr, 'program_code'> & { program_code?: string | null | undefined }
>(
  pipeline: P
) => ({
  name: pipeline.name,
  description: pipeline.description,
  runtimeConfig: pipeline.runtime_config,
  programConfig: pipeline.program_config,
  programCode: pipeline.program_code ?? '',
  programUdfRs: pipeline.udf_rust ?? '',
  programUdfToml: pipeline.udf_toml ?? ''
})

const toExtendedPipeline = ({
  program_status,
  deployment_status,
  deployment_desired_status,
  deployment_error,
  ...pipeline
}: ExtendedPipelineDescr) => ({
  createdAt: pipeline.created_at,
  deploymentDesiredStatus: deployment_desired_status,
  deploymentError: deployment_error,
  deploymentStatus: deployment_status,
  deploymentStatusSince: pipeline.deployment_status_since,
  description: pipeline.description,
  id: pipeline.id,
  name: pipeline.name,
  programCode: pipeline.program_code ?? '',
  programUdfRs: pipeline.udf_rust ?? '',
  programUdfToml: pipeline.udf_toml ?? '',
  programConfig: pipeline.program_config,
  programInfo: pipeline.program_info,
  programStatus: program_status,
  programStatusSince: pipeline.program_status_since,
  programVersion: pipeline.program_version,
  runtimeConfig: pipeline.runtime_config,
  version: pipeline.version,
  refreshVersion: pipeline.refresh_version,
  status: consolidatePipelineStatus(
    program_status,
    deployment_status,
    deployment_desired_status,
    deployment_error
  )
})

const fromPipeline = <T extends Partial<Pipeline>>(pipeline: T) => ({
  name: pipeline?.name,
  description: pipeline?.description,
  runtime_config: pipeline?.runtimeConfig,
  program_config: pipeline?.programConfig,
  program_code: pipeline?.programCode,
  udf_rust: pipeline?.programUdfRs,
  udf_toml: pipeline?.programUdfToml
})

export type PipelineThumb = ReturnType<typeof toPipelineThumb>
export type Pipeline = ReturnType<typeof toPipeline>
export type ExtendedPipeline = ReturnType<typeof toExtendedPipeline>

export const getPipeline = async (pipeline_name: string) => {
  return handled(
    _getPipeline,
    `Failed to fetch ${pipeline_name} pipeline`
  )({ path: { pipeline_name: encodeURIComponent(pipeline_name) } }).then(toPipeline)
}

export const getExtendedPipeline = async (
  pipeline_name: string,
  options?: { fetch?: (request: Request) => ReturnType<typeof fetch>; onNotFound?: () => void }
) => {
  return handled(
    _getPipeline,
    `Failed to fetch ${pipeline_name} pipeline`
  )({
    path: { pipeline_name: encodeURIComponent(pipeline_name) },
    ...options
  }).then(toExtendedPipeline, (e) => {
    if (typeof e === 'object' && 'error_code' in e && e.error_code === 'UnknownPipelineName') {
      options?.onNotFound?.()
    }
    throw e
  })
}

/**
 * Fails if pipeline exists
 */
export const postPipeline = async (pipeline: PipelineDescr) => {
  if (!pipeline.name) {
    throw new Error('Cannot create pipeline with empty name')
  }
  return handled(
    _postPipeline,
    `Failed to create ${pipeline.name} pipeline`
  )({ body: pipeline }).then(toPipelineThumb)
}

/**
 * Pipeline should already exist
 */
export const putPipeline = async (pipeline_name: string, newPipeline: PipelineDescr) => {
  await handled(
    _putPipeline,
    `Failed to update ${pipeline_name} pipeline`
  )({
    body: newPipeline,
    path: { pipeline_name: encodeURIComponent(pipeline_name) }
  })
}

export const patchPipeline = async (pipeline_name: string, pipeline: Partial<Pipeline>) => {
  return await handled(
    _patchPipeline,
    `Failed to update ${pipeline_name} pipeline`
  )({
    path: { pipeline_name: encodeURIComponent(pipeline_name) },
    body: fromPipeline(pipeline)
  }).then(toExtendedPipeline)
}

export const getPipelines = async (): Promise<PipelineThumb[]> => {
  const pipelines = await handled(
    listPipelines,
    'Failed to fetch the list of pipelines'
  )({ query: { selector: 'status' } })
  return pipelines.map(toPipelineThumb)
}

export const getPipelineStatus = async (pipeline_name: string) => {
  const pipeline = await handled(
    _getPipeline,
    `Failed to get ${pipeline_name} pipeline's status`
  )({
    path: { pipeline_name: encodeURIComponent(pipeline_name) },
    query: { selector: 'status' }
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
  })
    .then((status) => ({
      pipelineName: pipeline_name,
      status: status as ControllerStatus | null
    }))
    .catch((e) => {
      if (e.error_code === 'PipelineInteractionNotDeployed') {
        return {
          pipelineName: pipeline_name,
          status: 'not running' as const
        }
      }
      throw new Error(`Failed to fetch ${pipeline_name} pipeline stats`)
    })
}

const consolidatePipelineStatus = (
  programStatus: _ProgramStatus,
  pipelineStatus: _PipelineStatus,
  desiredStatus: _PipelineStatus,
  pipelineError: ErrorResponse | null | undefined
) => {
  return match([pipelineStatus, desiredStatus, pipelineError, programStatus])
    .with(['Shutdown', P.any, P.nullish, 'CompilingSql'], () => 'Compiling SQL' as const)
    .with(['Shutdown', P.any, P.nullish, 'SqlCompiled'], () => 'SQL compiled' as const)
    .with(['Shutdown', P.any, P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Shutdown', P.any, P.nullish, 'CompilingRust'], () => 'Compiling binary' as const)
    .with(
      ['Shutdown', P.any, P.nullish, { SqlError: P.select() }],
      (SqlError): { SqlError: SqlCompilerMessage[] } | { SqlWarning: SqlCompilerMessage[] } =>
        SqlError.every(({ warning }) => warning) ? { SqlWarning: SqlError } : { SqlError }
    )
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
    .with(['Paused', P.any, P.nullish, 'CompilingSql'], () => 'Compiling SQL' as const)
    .with(['Paused', P.any, P.nullish, 'Pending'], () => 'Queued' as const)
    .with(['Paused', P.any, P.nullish, 'CompilingRust'], () => 'Compiling binary' as const)
    .with(['Paused', 'Running', P.nullish, P._], () => 'Resuming' as const)
    .with(['Paused', 'Shutdown', P.nullish, P._], () => 'ShuttingDown' as const)
    .with(['Paused', P.any, P.nullish, P._], () => 'Paused' as const)
    .with(['Running', 'Paused', P.nullish, P._], () => 'Pausing' as const)
    .with(['Running', 'Shutdown', P.nullish, P._], () => 'ShuttingDown' as const)
    .with(['Running', P.any, P.nullish, P._], () => 'Running' as const)
    .with(['ShuttingDown', P.any, P.nullish, P._], () => 'ShuttingDown' as const)
    .with(['Failed', P.any, P.select(P.nonNullable), P._], (PipelineError) => ({ PipelineError }))
    .with(['Unavailable', P.any, P.any, P.any], () => 'Unavailable' as const)
    .otherwise(() => {
      throw new Error(
        `Unable to consolidatePipelineStatus: ${pipelineStatus} ${desiredStatus} ${pipelineError} ${programStatus}`
      )
    })
}

export const deletePipeline = async (pipeline_name: string) => {
  await handled(
    _deletePipeline,
    `Failed to delete ${pipeline_name} pipeline`
  )({ path: { pipeline_name } })
}

export type PipelineAction = 'start' | 'pause' | 'shutdown' | 'start_paused'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export const postPipelineAction = async (
  pipeline_name: string,
  action: PipelineAction
): Promise<() => Promise<void>> => {
  await handled(
    _postPipelineAction,
    `Failed to ${action} ${pipeline_name} pipeline`
  )({
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
      'Compiling binary',
      'SQL compiled',
      'Compiling SQL',
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

export const getConfig = () => handled(_getConfig)({ client: unauthenticatedClient })

export const getApiKeys = () => handled(listApiKeys, `Failed to fetch API keys`)()

export const postApiKey = (name: string) =>
  handled(_postApiKey, `Failed to create API key`)({ body: { name } })

export const deleteApiKey = (name: string) =>
  handled(_deleteApiKey, `Failed to delete ${name} API key`)({ path: { api_key_name: name } })

const getAuthenticatedFetch = () => {
  try {
    const oidcClient = OidcClient.get()
    return oidcClient.fetchWithTokens(globalThis.fetch)
  } catch {
    return globalThis.fetch
  }
}

export const relationEgressStream = async (pipelineName: string, relationName: string) => {
  // const result = await httpOutput({path: {pipeline_name: pipelineName, table_name: relationName}, query: {'format': 'json', 'mode': 'watch', 'array': false, 'query': 'table'}})
  const fetch = getAuthenticatedFetch()
  const result = await fetch(
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/egress/${relationName}?format=json&array=false`,
    {
      method: 'POST'
    }
  )
  return result.status === 200 && result.body ? result.body : (result.json() as Promise<Error>)
}
export const pipelineLogsStream = async (pipelineName: string) => {
  const fetch = getAuthenticatedFetch()
  const result = await fetch(`${felderaEndpoint}/v0/pipelines/${pipelineName}/logs`, {
    method: 'GET'
  })
  return result.status === 200 && result.body ? result.body : (result.json() as Promise<Error>)
}

export const adHocQuery = async (pipelineName: string, query: string) => {
  const fetch = getAuthenticatedFetch()
  const result = await fetch(
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/query?sql=${encodeURIComponent(query)}&format=json`
  )
  if (result.status !== 200) {
    const json = await result.json()
    return new ReadableStream<Uint8Array>({
      start(controller) {
        const encodedString = new TextEncoder().encode(
          JSON.stringify({ error: json.details.error ?? json.message })
        )
        controller.enqueue(encodedString)
        controller.close()
      }
    })
  }
  invariant(result.body !== null)
  return result.body
}

export type XgressEntry = { previewSlice: string } & (
  | { insert: XgressRecord }
  | { delete: XgressRecord }
)

/**
 * @param force Insert changes immediately even if pipeline is stopped
 */
export const relationIngress = async (
  pipelineName: string,
  relationName: string,
  data: XgressEntry[],
  force?: 'force'
) => {
  return httpInput({
    path: { pipeline_name: pipelineName, table_name: relationName },
    parseAs: 'text', // Response is empty, so no need to parse it as JSON
    query: { format: 'json', array: true, update_format: 'insert_delete', force: !!force },
    body: data as any
  })
}

const extractDemoType = (demo: { title: string }) => {
  const match = /([\w \-_\/\\\(\)\[\]+]+):?(.*)?/.exec(demo.title)
  if (match && match[2]) {
    return tuple(match[2], match[1])
  }
  return tuple('Example', match?.[1] ?? '')
}

export const getDemos = () =>
  handled(getConfigDemos, `Failed to fetch available demos`)().then((demos) =>
    demos.map((demo) => {
      const [title, type] = extractDemoType(demo)
      return {
        ...demo,
        title,
        type
      }
    })
  )
