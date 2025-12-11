import { defineConfig } from '@hey-api/openapi-ts'

export default defineConfig({
  input: '../../openapi.json',
  output: './src/lib/services/manager',
  plugins: [
    {
      name: '@hey-api/client-fetch',
      runtimeConfigPath: '$lib/compositions/setupHttpClient'
    }
  ]
})
