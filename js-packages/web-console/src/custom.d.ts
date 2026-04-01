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
  import type { ZipItem } from 'but-unzip'
  import type { TriagePlugin, DecodedBundle } from 'triage-types'
  export { TriageResults } from 'triage-types'
  export function createBundle(files: ZipItem[]): Promise<DecodedBundle>
  const plugins: TriagePlugin[]
  export default plugins
}
