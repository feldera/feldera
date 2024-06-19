import type { EgressMode } from './manager/models/EgressMode'
import type { NeighborhoodQuery } from './manager/models/NeighborhoodQuery'
import type { OutputQuery } from './manager/models/OutputQuery'

import { ApiRequestOptions } from './manager/core/ApiRequestOptions'
import { OpenAPIConfig } from './manager/core/OpenAPI'
import { getQueryString, request as __request } from './manager/core/request'

/**
 * TODO:
 * This function borrows implementation from '$lib/services/manager/services/HttpInputOutputService.ts'
 * It is a part of temporary fix to use only some of the code generated with OpenAPI codegen
 * All usages of this function should eventually be refactored out
 */
export function httpOutputOptions(
  pipelineName: string,
  tableName: string,
  format: string,
  query?: OutputQuery | null,
  mode?: EgressMode | null,
  quantiles?: number | null,
  array?: boolean | null,
  backpressure?: boolean | null,
  requestBody?: NeighborhoodQuery | null
) {
  return {
    method: 'POST' as const,
    url: '/v0/pipelines/{pipeline_name}/egress/{table_name}',
    path: {
      pipeline_name: pipelineName,
      table_name: tableName
    },
    query: {
      format: format,
      query: query,
      mode: mode,
      quantiles: quantiles,
      array: array,
      backpressure: backpressure
    },
    body: requestBody,
    mediaType: 'application/json',
    errors: {
      400: `Unknown data format specified in the '?format=' argument.`,
      404: `Specified table or view does not exist.`,
      410: `Pipeline is not currently running because it has been shutdown or not yet started.`,
      500: `Request failed.`
    }
  }
}

/**
 * TODO:
 * This function borrows implementation from '$lib/services/manager/services/manager/core/request.ts'
 * It is a part of temporary fix to use only some of the code generated with OpenAPI codegen
 * All usages of this function should eventually be refactored out
 */
export const getUrl = (config: OpenAPIConfig, options: ApiRequestOptions): string => {
  const encoder = config.ENCODE_PATH || encodeURI

  const path = options.url
    .replace('{api-version}', config.VERSION)
    .replace(/{(.*?)}/g, (substring: string, group: string) => {
      if (options.path?.hasOwnProperty(group)) {
        return encoder(String(options.path[group]))
      }
      return substring
    })

  const url = `${config.BASE}${path}`
  if (options.query) {
    return `${url}${getQueryString(options.query)}`
  }
  return url
}
