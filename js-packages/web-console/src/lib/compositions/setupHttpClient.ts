import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '#lib/functions/configs/felderaEndpoint.ts'
import type { CreateClientConfig } from '#lib/services/manager/client.gen.ts'

export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: felderaEndpoint,
  responseStyle: 'data',
  throwOnError: true,
  bodySerializer: JSONbig.stringify
})
