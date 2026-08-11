import { defineConfig } from '@hey-api/openapi-ts'
import {
  type CustomTypes,
  customTypesPlugin,
  overrideOpenapiType
} from './src/lib/functions/common/openapi-ts'

/**
 * Hand-written types substituted for the generated ones, keyed by the custom
 * `format` that marks a schema.
 */
const customTypes = {
  microseconds: { module: '$lib/functions/common/duration', name: 'Microseconds' }
} as const satisfies CustomTypes

export default defineConfig({
  input: '../../openapi.json',
  output: './src/lib/services/manager',
  parser: {
    patch: {
      schemas: {
        InputEndpointMetrics: (schema) => {
          overrideOpenapiType(schema, 'processing_latency_p99_micros', 'microseconds')
        }
      }
    }
  },
  plugins: [
    {
      name: '@hey-api/client-fetch',
      runtimeConfigPath: '$lib/compositions/setupHttpClient',
      throwOnError: true
    },
    {
      name: '@hey-api/sdk',
      responseStyle: 'data'
    },
    customTypesPlugin(customTypes)
  ]
})
