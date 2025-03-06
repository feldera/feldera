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
  type PostPutPipeline,
  type ProgramError
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
export type ProgramStatus = _ProgramStatus

import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient } = AxaOidc

import { client, createClient } from '@hey-api/client-fetch'
import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import invariant from 'tiny-invariant'
import { tuple } from '$lib/functions/common/tuple'
import { sleep } from '$lib/functions/common/promise'
import { type NamesInUnion, unionName } from '$lib/functions/common/union'
import { nonNull } from '$lib/functions/common/function'

const unauthenticatedClient = createClient({
  bodySerializer: JSONbig.stringify,
  responseTransformer: JSONbig.parse as any,
  baseUrl: felderaEndpoint
})

type PipelineDescr = PostPutPipeline

type ExtendedPipelineDescr = PipelineSelectedInfo

export type ExtendedPipelineDescrNoCode = Omit<ExtendedPipelineDescr, 'program_code'>

export const programStatusOf = (status: PipelineStatus) =>
  match(status)
    .returnType<ProgramStatus | undefined>()
    .with(
      'Preparing',
      'Provisioning',
      'Initializing',
      'Pausing',
      'Resuming',
      'Unavailable',
      'Running',
      'Paused',
      'ShuttingDown',
      { PipelineError: P.any },
      'Shutdown',
      () => 'Success' as const
    )
    .with({ Queued: P.any }, () => 'Pending' as const)
    .with({ CompilingSql: P.any }, () => 'CompilingSql')
    .with({ SqlCompiled: P.any }, () => 'SqlCompiled')
    .with({ CompilingRust: P.any }, () => 'CompilingRust')
    .with('SqlError', 'RustError', 'SystemError', (programStatus) => programStatus)
    .exhaustive()

const toPipelineThumb = (
  pipeline: Omit<ExtendedPipelineDescr, 'program_code' | 'udf_rust' | 'udf_toml'>
) => ({
  name: pipeline.name,
  description: pipeline.description,
  ...consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_desired_status,
    pipeline.deployment_error
  ),
  compilerOutput: toCompilerOutput(pipeline.program_error),
  deploymentStatusSince: pipeline.deployment_status_since,
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
  programStatusSince: pipeline.program_status_since,
  programVersion: pipeline.program_version,
  runtimeConfig: pipeline.runtime_config,
  version: pipeline.version,
  refreshVersion: pipeline.refresh_version,
  ...consolidatePipelineStatus(
    program_status,
    deployment_status,
    deployment_desired_status,
    deployment_error
  ),
  compilerOutput: toCompilerOutput(pipeline.program_error)
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

// export const getPipeline = async (pipeline_name: string) => {
//   return handled(
//     _getPipeline,
//     `Failed to fetch ${pipeline_name} pipeline`
//   )({ path: { pipeline_name: encodeURIComponent(pipeline_name) } }).then(toPipeline)
// }

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
  return consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_desired_status,
    pipeline.deployment_error
  )
}

export type PipelineStatus = ReturnType<typeof consolidatePipelineStatus>['status']

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

const toCompilerOutput = (programError: ProgramError) => {
  return {
    sql: programError.sql_compilation,
    rust: programError.rust_compilation,
    systemError: programError.system_error
  }
}

export type CompilerOutput = ReturnType<typeof toCompilerOutput>

const consolidatePipelineStatus = (
  programStatus: ProgramStatus,
  pipelineStatus: _PipelineStatus,
  desiredStatus: _PipelineStatus,
  pipelineError: ErrorResponse | null | undefined
) => {
  const status = match([pipelineStatus, desiredStatus, programStatus])
    .with(['Shutdown', P.any, 'Pending'], () => ({
      Queued: { cause: desiredStatus === 'Shutdown' ? ('compile' as const) : ('upgrade' as const) }
    }))
    .with(['Shutdown', P.any, 'CompilingSql'], () => ({
      CompilingSql: {
        cause: desiredStatus === 'Shutdown' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Shutdown', P.any, 'SqlCompiled'], () => ({
      SqlCompiled: {
        cause: desiredStatus === 'Shutdown' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Shutdown', P.any, 'CompilingRust'], () => ({
      CompilingRust: {
        cause: desiredStatus === 'Shutdown' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Shutdown', P.any, 'SqlError'], () => 'SqlError' as const)
    .with(['Shutdown', P.any, 'RustError'], () => 'RustError' as const)
    .with(['Shutdown', P.any, 'SystemError'], () => 'SystemError' as const)
    .with(['Shutdown', 'Running', P._], () => 'Preparing' as const)
    .with(['Shutdown', 'Paused', P._], () => 'Preparing' as const)
    .with(['Shutdown', 'Shutdown', 'Success'], () => 'Shutdown' as const)
    .with(['Provisioning', P.any, P._], () => 'Provisioning' as const)
    .with(['Initializing', P.any, P._], () => 'Initializing' as const)
    .with(['Paused', 'Running', P._], () => 'Resuming' as const)
    .with(['Paused', 'Shutdown', P._], () => 'ShuttingDown' as const)
    .with(['Paused', P.any, P._], () => 'Paused' as const)
    .with(['Running', 'Paused', P._], () => 'Pausing' as const)
    .with(['Running', 'Shutdown', P._], () => 'ShuttingDown' as const)
    .with(['Running', P.any, P._], () => 'Running' as const)
    .with(['ShuttingDown', P.any, P._], () => 'ShuttingDown' as const)
    .with(
      ['Failed', P.any, P._],
      P.when(() => nonNull(pipelineError)),
      () => {
        invariant(pipelineError)
        return { PipelineError: pipelineError }
      }
    )
    .with(['Unavailable', P.any, P.any], () => 'Unavailable' as const)
    .otherwise(() => {
      throw new Error(
        `Unable to consolidatePipelineStatus: ${pipelineStatus} ${desiredStatus} ${pipelineError} ${programStatus}`
      )
    })
  return {
    status
  }
}

export const deletePipeline = async (pipeline_name: string) => {
  await handled(
    _deletePipeline,
    `Failed to delete ${pipeline_name} pipeline`
  )({ path: { pipeline_name } })
}

export type PipelineAction = 'start' | 'pause' | 'shutdown' | 'start_paused'

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
    const ignoreStatuses: NamesInUnion<PipelineStatus>[] = [
      'Preparing',
      'Provisioning',
      'Initializing',
      'CompilingRust',
      'SqlCompiled',
      'CompilingSql',
      'Queued'
    ]
    while (true) {
      await sleep(300)
      const status = (await getPipelineStatus(pipeline_name)).status
      if (status === desiredStatus) {
        break
      }
      if (ignoreStatuses.includes(unionName(status))) {
        continue
      }
      throw new Error(
        `Unexpected status ${JSON.stringify(status)} while waiting for pipeline ${pipeline_name} to complete action ${action}`
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

const streamingFetch = async <E1, E2>(
  fetch: typeof globalThis.fetch,
  input: RequestInfo | URL,
  init: RequestInit,
  handleRequestError: (msg: string) => E1,
  handleResponseError: (json: any) => E2
) => {
  try {
    const controller = new AbortController()
    const result = await fetch(input, {
      ...init,
      signal: controller.signal
    })
    return result.status === 200 && result.body
      ? {
          stream: result.body,
          cancel: controller.abort.bind(controller)
        }
      : result.json().then(handleResponseError)
  } catch (e) {
    const msg = e instanceof Error ? e.message : JSON.stringify(e, undefined, '\t')
    return handleRequestError(msg)
  }
}

export const relationEgressStream = async (pipelineName: string, relationName: string) => {
  // const result = await httpOutput({path: {pipeline_name: pipelineName, table_name: relationName}, query: {'format': 'json', 'mode': 'watch', 'array': false, 'query': 'table'}})
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/egress/${relationName}?format=json&array=false`,
    {
      method: 'POST'
    },
    (msg) =>
      new Error(`Failed to connect to the egress stream of relation ${relationName}: \n${msg}`),
    (e) => new Error(e.details?.error ?? e.message, { cause: e })
  )
}

export const pipelineLogsStream = async (pipelineName: string) => {
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/logs`,
    {},
    (msg) => new Error(`Failed to connect to the log stream: \n${msg}`),
    (e) => new Error(e.details?.error ?? e.message, { cause: e })
  )
}

export const adHocQuery = async (pipelineName: string, query: string) => {
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/query?sql=${encodeURIComponent(query)}&format=json`,
    {},
    (msg) => new Error(`Failed to invoke an ad-hoc query: \n${msg}`),
    (e) => new Error(e.details?.error ?? e.message, { cause: e })
  )
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
