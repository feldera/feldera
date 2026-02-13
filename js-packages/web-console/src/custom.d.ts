declare module 'virtual:felderaApiJsonSchemas.json' {
  // eslint-disable-next-line
  const module: {
    readonly uri: string
    readonly fileMatch?: string[]
    readonly schema?: any
  }[]
  export default module
}

declare module 'virtual:feldera-triage-plugins' {
  import type { TriagePlugin } from '$lib/types/triage'
  const plugins: TriagePlugin[]
  export default plugins
}
