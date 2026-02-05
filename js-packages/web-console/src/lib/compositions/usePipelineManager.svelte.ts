import { useToast } from '$lib/compositions/useToastNotification'
import { triggerFileDownload } from '$lib/services/browser'
import {
  getPipelineSupportBundle as _getPipelineSupportBundle,
  adHocQuery,
  collectSamplyProfile,
  deleteApiKey,
  deletePipeline,
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
  getPipelineStatus,
  getPipelines,
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

let isNetworkHealthy = $state(true)

const _trackHealth =
  (options?: FetchOptions) =>
  <Args extends any[], Data>(x: (...a: [...Args, FetchOptions | undefined]) => Promise<Data>) =>
  (...a: Args) =>
    x(...a, options).then(
      (res) => {
        isNetworkHealthy ||= true
        return res
      },
      (e) => {
        if (isNetworkError(e)) {
          isNetworkHealthy = false
        }
        throw e
      }
    )

const _reportError =
  (onError: (e: Error) => void, options?: FetchOptions, doNotReportIf?: (e: Error) => boolean) =>
  <F extends (...args: any) => Promise<any>>(
    x: F,
    errorMsg?: (...args: F extends (...args: infer Args) => any ? Args : never) => string
  ) =>
  (...a: F extends (...args: infer Args) => any ? Args : never) => {
    return x(...a, options).then(
      (v: any) => {
        isNetworkHealthy ||= true
        return v
      },
      (e: any) => {
        if (doNotReportIf?.(e)) {
          throw e
        }
        if (isNetworkError(e)) {
          isNetworkHealthy = false
        }
        onError(
          isNetworkError(e)
            ? new Error((errorMsg?.(...a) ?? `Request failed`) + ': ' + e.message)
            : e
        )
        throw e
      }
    ) as ReturnType<F>
  }

export type PipelineManagerApi = Omit<ReturnType<typeof usePipelineManager>, 'isNetworkHealthy'>

export const usePipelineManager = (options?: FetchOptions) => {
  const { toastError } = useToast()

  const doNotReportIfCancelled = (error: Error) => {
    return error.cause === 'cancelled'
  }

  const reportError = _reportError(toastError, options, doNotReportIfCancelled)
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
    getPipelineStatus: reportError(
      getPipelineStatus,
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
    getClusterEvents: reportError(getClusterEvents),
    getClusterEvent: reportError(getClusterEvent),
    getPipelineEvents: reportError(getPipelineEvents),
    getPipelineEvent: reportError(getPipelineEvent),
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
