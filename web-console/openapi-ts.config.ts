import { defineConfig } from '@hey-api/openapi-ts'

export default defineConfig({
  plugins: [
    {
      name: '@hey-api/client-fetch',
      runtimeConfigPath: '$lib/compositions/setupHttpClient'
    }
  ],
  input: '../openapi.json',
  output: './src/lib/services/manager'
})
