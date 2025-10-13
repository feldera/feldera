import {
  getPipeline as _getPipeline,
  getPipelineStats as _getPipelineStats,
  listPipelines,
  putPipeline as _putPipeline,
  patchPipeline as _patchPipeline,
  deletePipeline as _deletePipeline,
  type CombinedStatus as _CombinedStatus,
  type CombinedDesiredStatus as _CombinedDesiredStatus,
  type ProgramStatus as _ProgramStatus,
  postPipelineStart,
  postPipelinePause,
  postPipelineStop,
  postPipelineClear,
  postUpdateRuntime as _postUpdateRuntime,
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
  getConfigSession as _getConfigSession,
  httpInput,
  type Field,
  type SqlCompilerMessage,
  type PostPutPipeline,
  type ProgramError,
  type RuntimeDesiredStatus,
  postPipelineResume,
  postPipelineActivate,
  type GetPipelineSupportBundleData,
  postPipelineApprove
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

import { client, createClient, type RequestResult } from '@hey-api/client-fetch'
import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import invariant from 'tiny-invariant'
import { tuple } from '$lib/functions/common/tuple'
import { sleep } from '$lib/functions/common/promise'
import { type NamesInUnion, unionName } from '$lib/functions/common/union'
import { singleton } from '$lib/functions/common/array'

const unauthenticatedClient = createClient({
  bodySerializer: JSONbig.stringify,
  responseTransformer: JSONbig.parse as any,
  baseUrl: felderaEndpoint
})

type PipelineDescr = PostPutPipeline

type ExtendedPipelineDescr = PipelineSelectedInfo

export type ExtendedPipelineDescrNoCode = Omit<ExtendedPipelineDescr, 'program_code'>

export type CompilerOutput = ReturnType<typeof toCompilerOutput>

const toCompilerOutput = (programError: ProgramError | null | undefined) => {
  return {
    sql: programError?.sql_compilation,
    rust: programError?.rust_compilation,
    systemError: programError?.system_error
  }
}

const _postPipelineAction = ({
  path
}: {
  path: {
    pipeline_name: string
    action: PipelineAction
  }
}) =>
  match(path.action)
    .with('start', () =>
      postPipelineStart({ path, query: { initial: 'running', bootstrap_policy: 'await_approval' } })
    )
    .with('resume', () => postPipelineResume({ path }))
    .with('pause', () => postPipelinePause({ path }))
    .with('stop', 'kill', (action) =>
      postPipelineStop({ path, query: { force: action === 'kill' } })
    )
    .with('standby', () =>
      postPipelineStart({ path, query: { initial: 'standby', bootstrap_policy: 'await_approval' } })
    )
    .with('activate', () => postPipelineActivate({ path }))
    .with('start_paused', () =>
      postPipelineStart({ path, query: { initial: 'paused', bootstrap_policy: 'await_approval' } })
    )
    .with('clear', () => postPipelineClear({ path }))
    .with('approve_changes', () => postPipelineApprove({ path }))
    .exhaustive()

export type PipelineStatus = ReturnType<typeof consolidatePipelineStatus>['status']

const consolidatePipelineStatus = (
  programStatus: ProgramStatus,
  deploymentStatus: _CombinedStatus,
  desiredStatus: _CombinedDesiredStatus,
  pipelineError: ErrorResponse | null | undefined
) => {
  const status = match([deploymentStatus, desiredStatus, programStatus])
    .with(['Stopped', P.any, 'Pending'], () => ({
      Queued: { cause: desiredStatus === 'Stopped' ? ('compile' as const) : ('upgrade' as const) }
    }))
    .with(['Stopped', P.any, 'CompilingSql'], () => ({
      CompilingSql: {
        cause: desiredStatus === 'Stopped' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Stopped', P.any, 'SqlCompiled'], () => ({
      SqlCompiled: {
        cause: desiredStatus === 'Stopped' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Stopped', P.any, 'CompilingRust'], () => ({
      CompilingRust: {
        cause: desiredStatus === 'Stopped' ? ('compile' as const) : ('upgrade' as const)
      }
    }))
    .with(['Stopped', P.any, 'SqlError'], () => 'SqlError' as const)
    .with(['Stopped', P.any, 'RustError'], () => 'RustError' as const)
    .with(['Stopped', P.any, 'SystemError'], () => 'SystemError' as const)
    .with(['Stopped', 'Running', P._], () => 'Preparing' as const)
    .with(['Stopped', 'Paused', P._], () => 'Preparing' as const)
    .with(['Stopped', 'Stopped', 'Success'], () => 'Stopped' as const)
    .with(['Provisioning', P.any, P._], () => 'Provisioning' as const)
    .with(['Initializing', P.any, P._], () => 'Initializing' as const)
    .with(['Stopping', P.any, P._], () => 'Stopping' as const)
    .with(['Paused', 'Running', P._], () => 'Resuming' as const)
    .with(['Paused', 'Stopped', P._], () => 'Stopping' as const)
    .with(['Paused', 'Suspended', P._], () => 'Stopping' as const)
    .with(['Paused', P.any, P._], () => 'Paused' as const)
    .with(['Running', 'Paused', P._], () => 'Pausing' as const)
    .with(['Running', 'Stopped', P._], () => 'Stopping' as const)
    .with(['Running', 'Suspended', P._], () => 'Stopping' as const)
    .with(['Suspended', 'Suspended', P._], () => 'Suspended' as const)
    .with(['Standby', P._, P._], () => 'Standby' as const)
    .with(['Bootstrapping', P._, P._], () => 'Bootstrapping' as const)
    .with(['Replaying', P._, P._], () => 'Replaying' as const)
    .with(['Running', P.any, P._], () => 'Running' as const)
    .with(['Unavailable', P.any, P.any], () => 'Unavailable' as const)
    .with(['AwaitingApproval', P.any, P._], () => 'AwaitingApproval' as const)
    .with([P._, 'Suspended', P._], () => 'Suspending' as const)
    .otherwise(() => {
      // throw new Error(
      //   `Unable to consolidatePipelineStatus: ${deploymentStatus} ${desiredStatus} ${pipelineError} ${programStatus}`
      // )
      console.error(
        `Unable to consolidatePipelineStatus: ${deploymentStatus} ${desiredStatus} ${pipelineError} ${programStatus}`
      )
      return 'Unavailable' as const
    })

  return {
    status
  }
}

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
      'Stopping',
      'Stopped',
      'Suspending',
      'Suspended',
      'Standby',
      'Bootstrapping',
      'Replaying',
      'AwaitingApproval',
      () => 'Success' as const
    )
    .with({ Queued: P.any }, () => 'Pending' as const)
    .with({ CompilingSql: P.any }, () => 'CompilingSql')
    .with({ SqlCompiled: P.any }, () => 'SqlCompiled')
    .with({ CompilingRust: P.any }, () => 'CompilingRust')
    .with('SqlError', 'RustError', 'SystemError', (programStatus) => programStatus)
    .exhaustive()

const toPipelineThumb = (
  pipeline: Omit<ExtendedPipelineDescr, 'program_code' | 'program_error' | 'udf_rust' | 'udf_toml'>
) => ({
  name: pipeline.name,
  description: pipeline.description,
  storageStatus: pipeline.storage_status,
  ...consolidatePipelineStatus(
    pipeline.program_status,
    pipeline.deployment_status,
    pipeline.deployment_desired_status,
    pipeline.deployment_error
  ),
  deploymentStatusSince: pipeline.deployment_status_since,
  deploymentError: pipeline.deployment_error,
  programStatusSince: pipeline.program_status_since,
  refreshVersion: pipeline.refresh_version,
  platformVersion: pipeline.platform_version,
  deploymentResourcesStatus: pipeline.deployment_resources_status,
  deploymentResourcesStatusSince: new Date(pipeline.deployment_resources_status_since),
  programConfig: pipeline.program_config!,
  deploymentRuntimeStatusDetails: pipeline.deployment_runtime_status_details
})

const toPipeline = <
  P extends Omit<PipelineDescr, 'program_code'> & { program_code?: string | null | undefined }
>(
  pipeline: P
) => ({
  name: pipeline.name,
  description: pipeline.description ?? '',
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
  programStatusSince: pipeline.program_status_since,
  description: pipeline.description,
  id: pipeline.id,
  name: pipeline.name,
  programCode: pipeline.program_code ?? '',
  programUdfRs: pipeline.udf_rust ?? '',
  programUdfToml: pipeline.udf_toml ?? '',
  programConfig: pipeline.program_config,
  programInfo: pipeline.program_info,
  programVersion: pipeline.program_version,
  runtimeConfig: pipeline.runtime_config,
  version: pipeline.version,
  refreshVersion: pipeline.refresh_version,
  platformVersion: pipeline.platform_version,
  storageStatus: pipeline.storage_status,
  ...consolidatePipelineStatus(
    program_status,
    deployment_status,
    deployment_desired_status,
    deployment_error
  ),
  compilerOutput: toCompilerOutput(pipeline.program_error),
  deploymentResourcesStatus: pipeline.deployment_resources_status,
  deploymentResourcesStatusSince: new Date(pipeline.deployment_resources_status_since),
  deploymentRuntimeStatusDetails: pipeline.deployment_runtime_status_details
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

const mapResponse = <R, T, E extends { message: string }>(
  request: RequestResult<R, E>,
  f: (v: R) => T,
  g?: (e: E) => T
) => {
  return request.then((response) => {
    if (response.error) {
      if (g) {
        return g(response.error)
      }
      throw new Error(response.error.message, {
        cause: { ...response.error, response: response.response }
      })
    }
    return f(response.data!)
  })
}

export const getExtendedPipeline = async (
  pipeline_name: string,
  options?: { fetch?: (request: Request) => ReturnType<typeof fetch>; onNotFound?: () => void }
) => {
  return mapResponse(
    _getPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      ...options
    }),
    toExtendedPipeline,
    (e) => {
      if (e.error_code === 'UnknownPipelineName') {
        options?.onNotFound?.()
      }
      throw new Error(e.message, { cause: e })
    }
  )
}

/**
 * Fails if pipeline exists
 */
export const postPipeline = async (pipeline: PipelineDescr) => {
  if (!pipeline.name) {
    throw new Error('Cannot create pipeline with empty name')
  }
  return mapResponse(_postPipeline({ body: pipeline }), toPipelineThumb)
}

/**
 * Pipeline should already exist
 */
export const putPipeline = async (pipeline_name: string, newPipeline: PipelineDescr) => {
  await mapResponse(
    _putPipeline({
      body: newPipeline,
      path: { pipeline_name: encodeURIComponent(pipeline_name) }
    }),
    (v) => v
  )
}

export const patchPipeline = async (pipeline_name: string, pipeline: Partial<Pipeline>) => {
  return mapResponse(
    _patchPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      body: fromPipeline(pipeline)
    }),
    toExtendedPipeline
  )
}

export const getPipelines = async (): Promise<PipelineThumb[]> => {
  return mapResponse(listPipelines({ query: { selector: 'status' } }), (pipelines) =>
    pipelines.map(toPipelineThumb)
  )
}

export const getPipelineStatus = async (pipeline_name: string) => {
  return mapResponse(
    _getPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      query: { selector: 'status' }
    }),
    (pipeline) =>
      consolidatePipelineStatus(
        pipeline.program_status,
        pipeline.deployment_status,
        pipeline.deployment_desired_status,
        pipeline.deployment_error
      )
  )
}

export const getPipelineStats = async (pipeline_name: string) => {
  return mapResponse(
    _getPipelineStats({
      path: { pipeline_name: encodeURIComponent(pipeline_name) }
    }),
    (status) => ({
      pipelineName: pipeline_name,
      status: status as ControllerStatus | null | 'not running'
    }),
    (e) => {
      if (e.error_code === 'PipelineInteractionNotDeployed') {
        return {
          pipelineName: pipeline_name,
          status: 'not running' as const
        }
      }
      throw new Error(e.message, { cause: e })
    }
  )
}

export const deletePipeline = async (pipeline_name: string) => {
  await mapResponse(_deletePipeline({ path: { pipeline_name } }), (v) => v)
}

export type PipelineAction =
  | 'start'
  | 'start_paused'
  | 'pause'
  | 'resume'
  | 'standby'
  | 'activate'
  | 'stop'
  | 'kill'
  | 'clear'
  | 'approve_changes'

export const postPipelineAction = async (pipeline_name: string, action: PipelineAction) => {
  return mapResponse(
    _postPipelineAction({
      path: {
        pipeline_name,
        action
      }
    }),
    (v) => v
  )
}

export const postUpdateRuntime = async (pipeline_name: string) => {
  return mapResponse(_postUpdateRuntime({ path: { pipeline_name } }), (v) => v)
}

export const getAuthConfig = () =>
  mapResponse(getConfigAuthentication({ client: unauthenticatedClient }), (v) => v)

export const getConfig = () => mapResponse(_getConfig(), (v) => v)
export const getConfigSession = () => mapResponse(_getConfigSession(), (v) => v)

export const getApiKeys = () => mapResponse(listApiKeys(), (v) => v)

export const postApiKey = (name: string) => mapResponse(_postApiKey({ body: { name } }), (v) => v)

export const deleteApiKey = (name: string) =>
  mapResponse(
    _deleteApiKey({ path: { api_key_name: name } }),
    (v) => v,
    () => {
      throw new Error(`Failed to delete ${name} API key`)
    }
  )

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
      signal: AbortSignal.any([controller.signal, ...singleton(init.signal)])
    })
    return result.status === 200 && result.body
      ? {
          stream: result.body,
          cancel: controller.abort.bind(controller)
        }
      : result.json().then((body) => handleResponseError({ ...body, status_code: result.status }))
  } catch (e) {
    const msg = e instanceof Error ? e.message : JSON.stringify(e, undefined, '\t')
    return handleRequestError(msg)
  }
}

export const relationEgressStream = async (pipelineName: string, relationName: string) => {
  // const result = await httpOutput({path: {pipeline_name: pipelineName, table_name: relationName}, query: {'format': 'json', 'mode': 'watch', 'array': false, 'query': 'table'}})
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/egress/${encodeURIComponent(relationName)}?format=json&array=false`,
    {
      method: 'POST'
    },
    (msg) =>
      new Error(`Failed to connect to the egress stream of relation ${relationName}: \n${msg}`),
    (e) => new Error(e.details?.error ?? e.message, { cause: e })
  )
}

export const pipelineLogsStream = async (pipelineName: string, requestInit?: RequestInit) => {
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/logs`,
    requestInit ?? {},
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

export const pipelineTimeSeriesStream = async (pipelineName: string) => {
  return streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/time_series_stream`,
    {},
    (msg) =>
      new Error(
        `Failed to connect to the time series stream of pipeline ${pipelineName}: \n${msg}`
      ),
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
  mapResponse(getConfigDemos(), (demos) =>
    demos.map((demo) => {
      const [title, type] = extractDemoType(demo)
      return {
        ...demo,
        title,
        type
      }
    })
  )

export type SupportBundleOptions = NonNullable<Required<GetPipelineSupportBundleData['query']>>

export const getPipelineSupportBundleUrl = (
  pipelineName: string,
  options: SupportBundleOptions
) => {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(options)) {
    query.append(key, String(value))
  }
  return `${felderaEndpoint}/v0/pipelines/${encodeURIComponent(pipelineName)}/support_bundle?${query.toString()}`
}

export const getAuthorizationHeader = async (): Promise<Record<string, string>> => {
  try {
    const oidcClient = OidcClient.get()
    const tokens = await oidcClient.getValidTokenAsync()
    return {
      Authorization: `Bearer ${tokens.tokens.accessToken}`
    }
  } catch {
    return {}
  }
}
