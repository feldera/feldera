import {
  adHocQuery,
  deleteApiKey,
  deletePipeline,
  getApiKeys,
  getAuthConfig,
  getConfig,
  getDemos,
  getExtendedPipeline,
  getPipelines,
  getPipelineStats,
  getPipelineStatus,
  patchPipeline,
  pipelineLogsStream,
  postApiKey,
  postPipeline,
  postPipelineAction,
  putPipeline,
  relationEgressStream,
  relationIngress
} from '$lib/services/pipelineManager'
import { useToast } from '$lib/compositions/useToastNotification'
import type { FunctionType } from '$lib/types/common/function'

const networkErrors = ['Failed to fetch', 'Network request failed', 'Timeout']
const isNetworkError = (e: any): e is TypeError =>
  e instanceof TypeError && networkErrors.includes(e.message)

let isNetworkHealthy = $state(true)

const trackHealth =
  <Args extends any[], Data>(x: (...a: Args) => Promise<Data>) =>
  (...a: Args) =>
    x(...a).then(
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

export type PipelineManagerApi = Omit<ReturnType<typeof usePipelineManager>, 'isNetworkHealthy'>

// let toastError: (e: Error) => void

export const usePipelineManager = () => {
  // try {
  //   toastError ??= useToast().toastError
  // } catch {}
  let { toastError } = useToast()

  const reportError =
    <F extends FunctionType>(
      x: F,
      errorMsg?: (...args: F extends (...args: infer Args) => any ? Args : never) => string
    ) =>
    (...a: F extends (...args: infer Args) => any ? Args : never): ReturnType<F> => {
      return x(...a).then(
        (v: any) => {
          isNetworkHealthy ||= true
          return v
        },
        (e: any) => {
          if (isNetworkError(e)) {
            isNetworkHealthy = false
          }
          toastError(
            isNetworkError(e)
              ? new Error((errorMsg?.(...a) ?? `Request failed`) + ': ' + e.message)
              : e
          )
          throw e
        }
      )
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
    getAuthConfig: getAuthConfig,
    getConfig: getConfig,
    getApiKeys: reportError(getApiKeys, () => 'Failed to fetch API keys'),
    postApiKey: reportError(postApiKey, (keyName) => `Failed to create ${keyName} API key`),
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
    relationIngress: reportError(
      relationIngress,
      (_, tableName) => `Failed to push data to the ${tableName} table`
    ),
    getDemos: reportError(getDemos, () => `Failed to fetch available demos`)
  }
}
