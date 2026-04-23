import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
import type { CreateClientConfig } from '$lib/services/manager/client.gen'

export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: felderaEndpoint,
  responseStyle: 'data',
  throwOnError: true,
  bodySerializer: JSONbig.stringify
})
