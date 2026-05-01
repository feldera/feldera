import { useToast } from '$lib/compositions/useToastNotification'
import { triggerFileDownload } from '$lib/services/browser'
import {
  getPipelineSupportBundle as _getPipelineSupportBundle,
  adHocQuery,
  collectSamplyProfile,
  deleteApiKey,
  deletePipeline,
  dismissDeploymentError,
  type FetchOptions,
  getApiKeys,
  getAuthConfig,
  getClusterEvent,
  getClusterEvents,
  getConfig,
  getConfigSession,
  getDemos,
  getExtendedPipeline,
  getPipelineDataflowGraph,
  getPipelineEvent,
  getPipelineEvents,
  getPipelineStats,
  getPipelines,
  getPipelineThumb,
  getSamplyProfile,
  patchPipeline,
  pipelineLogsStream,
  pipelineTimeSeriesStream,
  postApiKey,
  postPipeline,
  postPipelineAction,
  postUpdateRuntime,
  putPipeline,
  relationEgressStream,
  relationIngress,
  type SupportBundleOptions
} from '$lib/services/pipelineManager'

const networkErrors = ['Failed to fetch', 'Network request failed', 'Timeout']
const isNetworkError = (e: any): e is TypeError =>
  e instanceof TypeError && networkErrors.includes(e.message)

// 401s reach this wrapper after the auth middleware has already attempted a
// token refresh and failed. The base client's error interceptor attaches the
// original Response to the thrown error, and `mapResponse` in pipelineManager.ts
// re-exposes it via `cause.response` when it rewraps the error as an Error.
const isAuthError = (e: any) => e?.response?.status === 401 || e?.cause?.response?.status === 401

let isNetworkHealthy = $state(true)
let isAuthHealthy = $state(true)

const updateHealthOnSuccess = () => {
  isNetworkHealthy ||= true
  isAuthHealthy ||= true
}

const updateHealthOnError = (e: unknown) => {
  if (isNetworkError(e)) {
    isNetworkHealthy = false
  } else {
    // Any non-network error means the server replied — network is healthy.
    isNetworkHealthy ||= true
    if (isAuthError(e)) {
      isAuthHealthy = false
    }
  }
}

const _trackHealth =
  (options?: FetchOptions, onError?: (e: Error) => void, doNotReportIf?: (e: Error) => boolean) =>
  <F extends (...args: any) => Promise<any>>(
    x: F,
    errorMsg?: (...args: F extends (...args: infer Args) => any ? Args : never) => string
  ) =>
  (...a: F extends (...args: infer Args) => any ? Args : never) => {
    return x(...a, options).then(
      (v: any) => {
        updateHealthOnSuccess()
        return v
      },
      (e: any) => {
        if (doNotReportIf?.(e)) {
          throw e
        }
        updateHealthOnError(e)
        onError?.(
          isNetworkError(e)
            ? new Error((errorMsg?.(...a) ?? `Request failed`) + ': ' + e.message)
            : e
        )
        throw e
      }
    ) as ReturnType<F>
  }

export type PipelineManagerApi = Omit<
  ReturnType<typeof usePipelineManager>,
  'isNetworkHealthy' | 'isAuthHealthy'
>

export const usePipelineManager = (options?: FetchOptions) => {
  const { toastError } = useToast()

  /**
   * Returns `true` when `error` stems from an intentional cancellation and
   * should be silently suppressed rather than shown as a toast notification.
   * Returns `false` for genuine failures that should be reported to the user.
   */
  const doNotReportIfCancelled = (error: Error) => {
    if (error?.cause === 'cancelled') {
      return true // explicit .cancel() on a streaming fetch
    }
    if (error?.name === 'AbortError') {
      return true // A manually triggered AbortController signal fired (e.g. user navigated away)
    }
    return false
  }

  const reportError = _trackHealth(options, toastError('API request'), doNotReportIfCancelled)
  const trackHealth = _trackHealth(options)

  const getPipelineSupportBundle = (
    ...[pipelineName, options, onProgress]: Parameters<typeof _getPipelineSupportBundle>
  ) => {
    const result = _getPipelineSupportBundle(pipelineName, options, onProgress)
    return {
      cancel: result.cancel,

      dataPromise: reportError(
        () => {
          const download = result.downloadPromise.then(async (download) => ({
            filename: download.filename,
            data: await download.dataPromise
          }))
          return download
        },
        () => `Failed to download support bundle of pipeline ${pipelineName}`
      )()
    }
  }

  const downloadPipelineSupportBundle = (
    pipelineName: string,
    options: SupportBundleOptions,
    onProgress?: (bytesDownloaded: number, bytesTotal: number) => void
  ) => {
    const { dataPromise, cancel } = getPipelineSupportBundle(pipelineName, options, onProgress)
    dataPromise.then((download) => triggerFileDownload(download.filename, download.data))
    return {
      cancel,
      dataPromise
    }
  }

  const downloadSamplyProfile = async (
    pipelineName: string,
    latest: boolean,
    onProgress?: (bytesDownloaded: number, bytesTotal: number) => void
  ) => {
    const result = await getSamplyProfile(pipelineName, latest, onProgress)
    if ('expectedInSeconds' in result) {
      return result
    }
    result.downloadPromise.then((download) =>
      download.dataPromise.then((data) => {
        triggerFileDownload(download.filename, data)
      })
    )
    return { cancel: result.cancel }
  }

  return {
    get isNetworkHealthy() {
      return isNetworkHealthy
    },
    get isAuthHealthy() {
      return isAuthHealthy
    },
    getExtendedPipeline: reportError(
      getExtendedPipeline,
      (pipelineName) => `Failed to fetch ${pipelineName} pipeline`
    ),
    postPipeline: reportError(
      postPipeline,
      (pipelineName) => `Failed to create ${pipelineName} pipeline`
    ),
    putPipeline: reportError(
      putPipeline,
      (pipelineName) => `Failed to update ${pipelineName} pipeline`
    ),
    patchPipeline: reportError(
      patchPipeline,
      (pipelineName) => `Failed to update ${pipelineName} pipeline`
    ),
    getPipelines: trackHealth(getPipelines),
    getPipelineThumb: reportError(
      getPipelineThumb,
      (pipelineName) => `Failed to get ${pipelineName} pipeline's status`
    ),
    getPipelineStats: getPipelineStats,
    deletePipeline: reportError(
      deletePipeline,
      (pipelineName) => `Failed to delete ${pipelineName} pipeline`
    ),
    postPipelineAction: reportError(
      postPipelineAction,
      (pipelineName, action) => `Failed to ${action} ${pipelineName} pipeline`
    ),
    postUpdateRuntime: reportError(
      postUpdateRuntime,
      (pipelineName) => `Failed to update runtime for ${pipelineName} pipeline`
    ),
    getAuthConfig: getAuthConfig,
    getConfig: getConfig,
    getConfigSession: reportError(getConfigSession, () => 'Failed to fetch session configuration'),
    getApiKeys: reportError(getApiKeys, () => 'Failed to fetch API keys'),
    postApiKey: async (name: string, options?: FetchOptions | undefined) => {
      const x = await reportError(postApiKey, (keyName) => `Failed to create ${keyName} API key`)(
        name,
        options
      )
      return x
    },
    deleteApiKey: reportError(deleteApiKey, (keyName) => `Failed to delete ${keyName} API key`),
    dismissDeploymentError: reportError(
      dismissDeploymentError,
      (pipelineName) => `Failed to dismiss deployment error for ${pipelineName} pipeline`
    ),
    getClusterEvents: reportError(getClusterEvents),
    getClusterEvent: reportError(getClusterEvent),
    getPipelineEvents: reportError(
      getPipelineEvents,
      (pipelineName) => `Failed to fetch ${pipelineName} pipeline events`
    ),
    getPipelineEvent: reportError(
      getPipelineEvent,
      (pipelineName) => `Failed to fetch ${pipelineName} pipeline event`
    ),
    getSamplyProfile: reportError(getSamplyProfile),
    downloadSamplyProfile: reportError(
      downloadSamplyProfile,
      (pipelineName) => `Failed to download samply profile for ${pipelineName} pipeline`
    ),
    collectSamplyProfile: reportError(collectSamplyProfile),
    relationEgressStream: reportError(
      relationEgressStream,
      (_, relationName) => `Failed to connect to the egress stream of relation ${relationName}`
    ),
    pipelineLogsStream: reportError(
      pipelineLogsStream,
      () => `Failed to connect to the log stream`
    ),
    adHocQuery: reportError(adHocQuery, () => `Failed to invoke an ad-hoc query`),
    pipelineTimeSeriesStream: reportError(
      pipelineTimeSeriesStream,
      () => `Failed to connect to the time series stream`
    ),
    relationIngress: reportError(
      relationIngress,
      (_, tableName) => `Failed to push data to the ${tableName} table`
    ),
    getDemos: reportError(getDemos, () => `Failed to fetch available demos`),
    downloadPipelineSupportBundle,
    getPipelineDataflowGraph: reportError(
      getPipelineDataflowGraph,
      (pipelineName) => `Failed to load dataflow graph of pipeline ${pipelineName}`
    ),
    getPipelineSupportBundle
  }
}
