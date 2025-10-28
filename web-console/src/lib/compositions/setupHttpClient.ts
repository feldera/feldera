import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import type { CreateClientConfig } from '$lib/services/manager/client'

export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: felderaEndpoint,
  responseStyle: 'fields',
  throwOnError: true,
  bodySerializer: JSONbig.stringify,
  responseTransformer: JSONbig.parse as any
})
