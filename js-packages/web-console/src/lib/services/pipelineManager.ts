import {
  type CombinedDesiredStatus as _CombinedDesiredStatus,
  type CombinedStatus as _CombinedStatus,
  deleteApiKey as _deleteApiKey,
  deletePipeline as _deletePipeline,
  getClusterEvent as _getClusterEvent,
  getConfig as _getConfig,
  getConfigSession as _getConfigSession,
  getPipeline as _getPipeline,
  getPipelineDataflowGraph as _getPipelineDataflowGraph,
  getPipelineEvent as _getPipelineEvent,
  getPipelineStats as _getPipelineStats,
  type ProgramStatus as _ProgramStatus,
  patchPipeline as _patchPipeline,
  postApiKey as _postApiKey,
  postPipeline as _postPipeline,
  postUpdateRuntime as _postUpdateRuntime,
  putPipeline as _putPipeline,
  type ControllerStatus,
  type ErrorResponse,
  type GetPipelineSupportBundleData,
  getConfigAuthentication,
  getConfigDemos,
  httpInput,
  listApiKeys,
  listClusterEvents,
  listPipelineEvents,
  listPipelines,
  type PipelineSelectedInfo,
  type PostPutPipeline,
  type ProgramError,
  postPipelineActivate,
  postPipelineApprove,
  postPipelineClear,
  postPipelinePause,
  postPipelineResume,
  postPipelineStart,
  postPipelineStop,
  startSamplyProfile
} from '$lib/services/manager'

export type {
  InputEndpointConfig,
  OutputEndpointConfig,
  RuntimeConfig,
  SqlCompilerMessage
} from '$lib/services/manager'

import { match, P } from 'ts-pattern'
import type { XgressRecord } from '$lib/types/pipelineManager'

export type { ProgramSchema } from '$lib/services/manager'
export type ProgramStatus = _ProgramStatus

import JSONbig from 'true-json-bigint'
import { singleton } from '$lib/functions/common/array'
import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import { applyAuthToRequest, handleAuthResponse } from '$lib/services/auth'
import { createClient } from '$lib/services/manager/client'

const unauthenticatedClient = createClient({
  bodySerializer: JSONbig.stringify,
  baseUrl: felderaEndpoint
})

type PipelineDescr = PostPutPipeline

type ExtendedPipelineDescr = PipelineSelectedInfo

export type ExtendedPipelineDescrNoCode = Omit<ExtendedPipelineDescr, 'program_code'>

export type CompilerOutput = ReturnType<typeof toCompilerOutput>

export type FetchOptions = { fetch?: typeof globalThis.fetch }

const toCompilerOutput = (programError: ProgramError | null | undefined) => {
  return {
    sql: programError?.sql_compilation,
    rust: programError?.rust_compilation,
    systemError: programError?.system_error
  }
}

const _postPipelineAction = (
  {
    path
  }: {
    path: {
      pipeline_name: string
      action: PipelineAction
    }
  },
  options?: FetchOptions
) =>
  match(path.action)
    .with('start', () =>
      postPipelineStart({
        path,
        query: { initial: 'running', bootstrap_policy: 'await_approval' },
        ...options
      })
    )
    .with('resume', () => postPipelineResume({ path, ...options }))
    .with('pause', () => postPipelinePause({ path, ...options }))
    .with('stop', 'kill', (action) =>
      postPipelineStop({ path, query: { force: action === 'kill' }, ...options })
    )
    .with('standby', () =>
      postPipelineStart({
        path,
        query: { initial: 'standby', bootstrap_policy: 'await_approval' },
        ...options
      })
    )
    .with('activate', () => postPipelineActivate({ path, ...options }))
    .with('start_paused', () =>
      postPipelineStart({
        path,
        query: { initial: 'paused', bootstrap_policy: 'await_approval' },
        ...options
      })
    )
    .with('clear', () => postPipelineClear({ path, ...options }))
    .with('approve_changes', () => postPipelineApprove({ path, ...options }))
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
  deploymentRuntimeStatusDetails: pipeline.deployment_runtime_status_details,
  connectors: pipeline.connectors
    ? {
        numErrors: pipeline.connectors.num_errors
      }
    : undefined
})

const toPipeline = <
  P extends Omit<PipelineDescr, 'program_code'> & { program_code?: string | null | undefined }
>(
  pipeline: P
) => ({
  name: pipeline.name,
  description: pipeline.description ?? '',
  runtimeConfig: pipeline.runtime_config,
  programConfig: pipeline.program_config!,
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
  programConfig: pipeline.program_config!,
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

type RequestResult<R, E> = Promise<
  (
    | {
        data: R
        error: undefined
      }
    | {
        data: undefined
        error: E
      }
  ) & {
    request: Request
    response: Response
  }
>

const mapResponse = <R, T, E extends { message: string }>(
  request: RequestResult<R, E>,
  f: (v: R) => T,
  g?: (e: E) => T
) => {
  return request.then((response) => {
    if ('error' in response && response.error) {
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
  callbacks?: { onNotFound: () => void },
  options?: FetchOptions
) => {
  return mapResponse(
    _getPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      ...options
    }),
    toExtendedPipeline,
    (e) => {
      if (e.error_code === 'UnknownPipelineName') {
        callbacks?.onNotFound?.()
      }
      throw new Error(e.message, { cause: e })
    }
  )
}

/**
 * Fails if pipeline exists
 */
export const postPipeline = async (pipeline: PipelineDescr, options?: FetchOptions) => {
  if (!pipeline.name) {
    throw new Error('Cannot create pipeline with empty name')
  }
  return mapResponse(_postPipeline({ body: pipeline, ...options }), toPipelineThumb)
}

/**
 * Pipeline should already exist
 */
export const putPipeline = async (
  pipeline_name: string,
  newPipeline: PipelineDescr,
  options?: FetchOptions
) => {
  await mapResponse(
    _putPipeline({
      body: newPipeline,
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      ...options
    }),
    (v) => v
  )
}

export const patchPipeline = async (
  pipeline_name: string,
  pipeline: Partial<Pipeline>,
  options?: FetchOptions
) => {
  return mapResponse(
    _patchPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      body: fromPipeline(pipeline),
      ...options
    }),
    toExtendedPipeline
  )
}

export const getPipelines = async (options?: FetchOptions): Promise<PipelineThumb[]> => {
  return mapResponse(
    listPipelines({
      query: { selector: 'status_with_connectors' },
      ...options
    }),
    (pipelines) => pipelines.map(toPipelineThumb)
  )
}

export const getPipelineStatus = async (pipeline_name: string, options?: FetchOptions) => {
  return mapResponse(
    _getPipeline({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      query: { selector: 'status' },
      ...options
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

export const getPipelineStats = async (pipeline_name: string, options?: FetchOptions) => {
  return mapResponse(
    _getPipelineStats({
      path: { pipeline_name: encodeURIComponent(pipeline_name) },
      ...options
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

export const deletePipeline = async (pipeline_name: string, options?: FetchOptions) => {
  await mapResponse(_deletePipeline({ path: { pipeline_name }, ...options }), (v) => v)
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

export const postPipelineAction = async (
  pipeline_name: string,
  action: PipelineAction,
  options?: FetchOptions
) => {
  return mapResponse(
    _postPipelineAction(
      {
        path: {
          pipeline_name,
          action
        }
      },
      options
    ),
    (v) => v
  )
}

export const postUpdateRuntime = async (pipeline_name: string, options?: FetchOptions) => {
  return mapResponse(_postUpdateRuntime({ path: { pipeline_name }, ...options }), (v) => v)
}

export const getAuthConfig = (options?: FetchOptions) =>
  mapResponse(getConfigAuthentication({ client: unauthenticatedClient, ...options }), (v) => v)

export const getConfig = (options?: FetchOptions) => mapResponse(_getConfig(options), (v) => v)
export const getConfigSession = (options?: FetchOptions) =>
  mapResponse(_getConfigSession(options), (v) => v)

export const getApiKeys = (options?: FetchOptions) => mapResponse(listApiKeys(options), (v) => v)

export const postApiKey = (name: string, options?: FetchOptions) =>
  mapResponse(_postApiKey({ body: { name }, ...options }), (v) => v)

export const deleteApiKey = (name: string, options?: FetchOptions) =>
  mapResponse(
    _deleteApiKey({ path: { api_key_name: name }, ...options }),
    (v) => v,
    () => {
      throw new Error(`Failed to delete ${name} API key`)
    }
  )

export const getPipelineDataflowGraph = (pipelineName: string) =>
  mapResponse(_getPipelineDataflowGraph({ path: { pipeline_name: pipelineName } }), (v) => v)

export const getClusterEvents = () => mapResponse(listClusterEvents(), (v) => v)

export const getClusterEvent = (eventId: string) =>
  mapResponse(
    _getClusterEvent({ path: { event_id: eventId }, query: { selector: 'all' } }),
    (v) => v
  )

export const getPipelineEvents = (pipelineName: string) =>
  mapResponse(listPipelineEvents({ path: { pipeline_name: pipelineName } }), (v) => v)

export const getPipelineEvent = (pipelineName: string, eventId: string) =>
  mapResponse(
    _getPipelineEvent({
      path: { pipeline_name: pipelineName, event_id: eventId },
      query: { selector: 'all' }
    }),
    (v) => v
  )

/**
 * Get samply profile stream with progress tracking support.
 * Returns the stream and filename extracted from Content-Disposition header.
 */
const getSamplyProfileStream = (pipelineName: string, latest: boolean) => {
  const result = streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${encodeURIComponent(pipelineName)}/samply_profile${latest ? '?latest=true' : ''}`,
    {
      method: 'GET'
    }
  )
  return {
    cancel: result.cancel,
    abortReason: result.abortReason,
    response: result.response.then((response) => {
      if (response instanceof Error) {
        throw response
      }
      return response
    })
  }
}

/**
 * Parse filename from Content-Disposition header
 * Returns null if filename cannot be extracted
 */
const parseFilenameFromContentDisposition = (contentDisposition: string | null): string | null => {
  if (!contentDisposition) {
    return null
  }

  // Try to parse filename from Content-Disposition header
  // Format: attachment; filename="filename.ext"
  const match = contentDisposition.match(/filename="?([^"]+)"?/)
  if (match && match[1]) {
    return match[1]
  }

  return null
}

type BlobDownloadHandle = {
  downloadPromise: Promise<{ dataPromise: Promise<Blob>; filename: string }>
  cancel: () => void
}

/**
 * Download samply profile with optional progress tracking.
 * @param pipelineName - Name of the pipeline
 * @param latest - If true and profiling is in progress, returns expectedInSeconds instead of the last profile
 * @param onProgress - Optional callback for download progress (bytesDownloaded, bytesTotal)
 * @returns Object with a promise to profile data blob and filename, or expectedInSeconds if in progress, plus cancel function
 */
export const getSamplyProfile = async (
  pipelineName: string,
  latest: boolean,
  onProgress?: (bytesDownloaded: number, bytesTotal: number) => void
): Promise<BlobDownloadHandle | { expectedInSeconds: number }> => {
  const result = getSamplyProfileStream(pipelineName, latest)
  const response = await result.response

  // Check for 204 No Content (profile collection in progress)
  if (response.status === 204) {
    const expectedInSeconds = parseInt(response.headers.get('Retry-After') ?? '')
    if (isNaN(expectedInSeconds)) {
      throw new Error(`Profile collection is in progress, but estimated time is not known`)
    }
    return { expectedInSeconds }
  }

  return {
    downloadPromise: Promise.resolve(streamToDownload(response, onProgress, result.abortReason)),
    cancel: result.cancel
  }
}

export const collectSamplyProfile = async (pipelineName: string, durationSeconds: number) => {
  const result = await startSamplyProfile({
    path: { pipeline_name: pipelineName },
    query: { duration_secs: durationSeconds }
  })
  if (!result.error) {
    return { data: result.data }
  }
  throw new Error(apiErrorText(result.error), { cause: result.error })
}

/**
 * Returns the raw stream for downloading a pipeline support bundle.
 * Use getPipelineSupportBundle wrapper for progress tracking.
 */
export const getPipelineSupportBundleStream = (
  pipelineName: string,
  options: Partial<SupportBundleOptions>
) => {
  const query = new URLSearchParams(
    Object.fromEntries(Object.entries(options).map(([k, v]) => [k, String(v)]))
  )
  const result = streamingFetch(
    getAuthenticatedFetch(),
    `${felderaEndpoint}/v0/pipelines/${pipelineName}/support_bundle?${query.toString()}`,
    {
      method: 'GET'
    }
  )
  return {
    cancel: result.cancel,
    abortReason: result.abortReason,
    response: result.response.then((response) => {
      if (response instanceof Error) {
        throw response
      }
      return response
    })
  }
}

// Create a transform stream that tracks progress
const progressTransform = (
  totalBytes: number,
  onProgress: (bytesDownloaded: number, bytesTotal: number) => void
) => {
  let bytesDownloaded = 0
  return new TransformStream({
    transform(chunk, controller) {
      bytesDownloaded += chunk.byteLength
      onProgress?.(bytesDownloaded, totalBytes)
      controller.enqueue(chunk)
    }
  })
}

/**
 * Convert a readable stream to a Blob with optional progress tracking.
 * @param result - Stream result with optional content length
 * @param onProgress - Optional callback for download progress (bytesDownloaded, bytesTotal)
 * @param abortReason - Function to get the custom abort reason if cancelled
 */
const streamToDownload = (
  response: Response,
  onProgress?: (bytesDownloaded: number, bytesTotal: number) => void,
  abortReason?: () => Error | undefined
): { dataPromise: Promise<Blob>; filename: string } => {
  const totalBytes = ((length) => (length ? parseInt(length) : undefined))(
    response.headers.get('content-length')
  )

  const streamWithProgress =
    nonNull(totalBytes) && onProgress
      ? response.body!.pipeThrough(progressTransform(totalBytes, onProgress))
      : response.body

  // Extract filename from Content-Disposition header
  const contentDisposition = response.headers.get('Content-Disposition')
  const filename = parseFilenameFromContentDisposition(contentDisposition)
  if (!filename) {
    throw new Error('Server did not provide a filename in Content-Disposition header')
  }

  const responseWithProgress = new Response(streamWithProgress)
  return {
    filename,
    dataPromise: responseWithProgress.blob().catch((e) => {
      // If we have a custom abort reason and this looks like an abort error, throw the custom error
      const customAbortReason = abortReason?.()
      if (customAbortReason) {
        throw customAbortReason
      }
      throw e
    })
  }
}

/**
 * Downloads a pipeline support bundle with optional progress tracking.
 * @param pipelineName - Name of the pipeline
 * @param options - Support bundle options (profiling, logs, etc.)
 * @param onProgress - Optional callback for download progress (bytesDownloaded, bytesTotal)
 * @returns Object with promise to bundle data blob, filename from Content-Disposition header, and cancel function
 */
export const getPipelineSupportBundle = (
  pipelineName: string,
  options: Partial<SupportBundleOptions>,
  onProgress?: (bytesDownloaded: number, bytesTotal: number) => void
): BlobDownloadHandle => {
  const result = getPipelineSupportBundleStream(pipelineName, options)
  return {
    downloadPromise: result.response.then((response) =>
      streamToDownload(response, onProgress, result.abortReason)
    ),
    cancel: result.cancel
  }
}

/**
 * Returns a fetch function that applies authentication headers and handles 401 responses.
 * Uses the same middleware as the global @hey-api/client-fetch instance.
 */
const getAuthenticatedFetch = (options?: FetchOptions): typeof globalThis.fetch => {
  const f = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    // Create a Request object and apply auth headers
    const request = applyAuthToRequest(new Request(input, init))

    // Perform the fetch
    const response = await (options?.fetch ?? globalThis.fetch)(request)

    // Handle 401 responses with token refresh
    return handleAuthResponse(response, request, options?.fetch ?? globalThis.fetch)
  }
  return Object.assign(f, { preconnect: globalThis.fetch.preconnect })
}

const apiErrorText = (error: ErrorResponse) => {
  return `${error.message}${error.details ? `\n${error.details}` : ''}`
}

const streamingFetch = (
  fetch: typeof globalThis.fetch,
  input: RequestInfo | URL,
  init: RequestInit
): {
  response: Promise<Response | Error>
  cancel: () => void
  /**
   * Enables downstream consumers of the body stream to know the original cause of the stream abort in the case of manual cancellation
   * @returns
   */
  abortReason: () => Error | undefined
} => {
  const controller = new AbortController()
  let abortReason: Error | undefined
  const promise = fetch(input, {
    ...init,
    signal: AbortSignal.any([controller.signal, ...singleton(init.signal)])
  })

  return {
    cancel: () => {
      abortReason = new Error('cancelled', { cause: 'cancelled' })
      controller.abort(abortReason)
    },
    abortReason: () => abortReason,
    response: promise.then(
      (response) => {
        // Handle successful response with body
        if (response.ok) {
          return response
        }
        // For other non-2XX status codes, try to parse JSON error
        return response.json().then((body) => {
          return new Error(apiErrorText(body), { cause: body })
        })
      },
      (e) => {
        const msg = e instanceof Error ? e.message : JSON.stringify(e, undefined, '\t')
        return new Error(msg, { cause: e.cause })
      }
    )
  }
}

const streamResponse = async (request: {
  response: Promise<Response | Error>
  cancel: () => void
  abortReason: () => Error | undefined
}) => {
  const response = await request.response
  if (response instanceof Error) {
    return response
  }
  return {
    response,
    stream: response.body!,
    cancel: request.cancel,
    abortReason: request.abortReason
  }
}

export const relationEgressStream = async (
  pipelineName: string,
  relationName: string,
  options?: FetchOptions
) => {
  return streamResponse(
    streamingFetch(
      getAuthenticatedFetch(options),
      `${felderaEndpoint}/v0/pipelines/${pipelineName}/egress/${encodeURIComponent(relationName)}?format=json&array=false`,
      {
        method: 'POST'
      }
    )
  )
}

export const pipelineLogsStream = async (
  pipelineName: string,
  requestInit?: RequestInit,
  options?: FetchOptions
) => {
  return streamResponse(
    streamingFetch(
      getAuthenticatedFetch(options),
      `${felderaEndpoint}/v0/pipelines/${pipelineName}/logs`,
      requestInit ?? {}
    )
  )
}

export const adHocQuery = async (pipelineName: string, query: string, options?: FetchOptions) => {
  return streamResponse(
    streamingFetch(
      getAuthenticatedFetch(options),
      `${felderaEndpoint}/v0/pipelines/${pipelineName}/query?sql=${encodeURIComponent(query)}&format=json`,
      {}
    )
  )
}

export const pipelineTimeSeriesStream = async (pipelineName: string, options?: FetchOptions) => {
  return streamResponse(
    streamingFetch(
      getAuthenticatedFetch(options),
      `${felderaEndpoint}/v0/pipelines/${pipelineName}/time_series_stream`,
      {}
    )
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
  force?: 'force',
  options?: FetchOptions
) => {
  return httpInput({
    path: { pipeline_name: pipelineName, table_name: relationName },
    parseAs: 'text', // Response is empty, so no need to parse it as JSON
    query: { format: 'json', array: true, update_format: 'insert_delete', force: !!force },
    body: data as any,
    ...options
  })
}

const extractDemoType = (demo: { title: string }) => {
  const match = /([\w \-_/\\()[\]+]+):?(.*)?/.exec(demo.title)
  if (match && match[2]) {
    return tuple(match[2], match[1])
  }
  return tuple('Example', match?.[1] ?? '')
}

export const getDemos = (options?: FetchOptions) =>
  mapResponse(getConfigDemos(options), (demos) =>
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
