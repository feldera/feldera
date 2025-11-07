import { defineConfig, type UserConfig } from 'vite'

export default defineConfig(async () => {
  return {
    server: {
        port: 5174
    }
  } satisfies UserConfig
})
