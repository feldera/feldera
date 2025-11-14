import { defineConfig, type UserConfig } from 'vite'

export default defineConfig(async () => {
  return {
    server: {
        port: 5174
    },
    define: {
      'process.env': process.env
    }
  } satisfies UserConfig
})
