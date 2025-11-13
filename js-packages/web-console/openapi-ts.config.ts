import { defineConfig } from '@hey-api/openapi-ts'

export default defineConfig({
  client: '@hey-api/client-fetch',
  input: '../../openapi.json',
  output: './src/lib/services/manager'
})
