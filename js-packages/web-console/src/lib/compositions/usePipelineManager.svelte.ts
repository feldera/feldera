import { useToast } from '$lib/compositions/useToastNotification'
import { triggerFileDownload } from '$lib/services/browser'
import {
  adHocQuery,
  deleteApiKey,
  deletePipeline,
  type FetchOptions,
  getApiKeys,
  getAuthConfig,
  getConfig,
  getConfigSession,
  getDemos,
  getExtendedPipeline,
  getPipelineDataflowGraph,
  getPipelineStats,
  getPipelineStatus,
  getPipelineSupportBundle,
  getPipelineSupportBundleUrl,
  getPipelines,
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
import type { FunctionType } from '$lib/types/common/function'

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
  (onError: (e: Error) => void, options?: FetchOptions) =>
  <F extends FunctionType>(
    x: F,
    errorMsg?: (...args: F extends (...args: infer Args) => any ? Args : never) => string
  ) =>
  (...a: F extends (...args: infer Args) => any ? Args : never): ReturnType<F> => {
    return x(...a, options).then(
      (v: any) => {
        isNetworkHealthy ||= true
        return v
      },
      (e: any) => {
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
    )
  }

export type PipelineManagerApi = Omit<ReturnType<typeof usePipelineManager>, 'isNetworkHealthy'>

// let toastError: (e: Error) => void

export const usePipelineManager = (options?: FetchOptions) => {
  // try {
  //   toastError ??= useToast().toastError
  // } catch {}
  const { toastError } = useToast()

  const reportError = _reportError(toastError, options)
  const trackHealth = _trackHealth(options)

  const downloadPipelineSupportBundle = async (
    pipelineName: string,
    options: SupportBundleOptions,
    onProgress?: (bytesDownloaded: number, bytesTotal: number) => void
  ) => {
    const blob = await getPipelineSupportBundle(pipelineName, options, onProgress)
    // const url = getPipelineSupportBundleUrl(pipelineName, options)
    // const headers = {
    //   ...(await getAuthorizationHeaders()),
    //   Accept: 'application/zip'
    // }
    const fileName = `fda-bundle-${pipelineName}-${new Date().toISOString().replace(/\.\d{3}/, '')}.zip`
    // // Use simple fetch approach (loads into memory) instead of streaming
    triggerFileDownload(fileName, blob)
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
    downloadPipelineSupportBundle: reportError(
      downloadPipelineSupportBundle,
      (pipelineName) => `Failed to download support bundle for ${pipelineName} pipeline`
    ),
    getPipelineDataflowGraph: reportError(
      getPipelineDataflowGraph,
      (pipelineName) => `Failed to load dataflow graph of pipeline ${pipelineName}`
    ),
    getPipelineSupportBundle: reportError(
      getPipelineSupportBundle,
      (pipelineName) => `Failed to load circuit profile of pipeline ${pipelineName}`
    )
  }
}
