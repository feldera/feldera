import { nonNull } from '$lib/functions/common/function'

export const normalizeRuntimeVersion = (version: string) => version.replace(/\+.*/, '')

export const getRuntimeVersion = (
  version: {
    runtime: string
    base: string
    configured: string | null | undefined
  },
  unstableFeatures: string[]
) => {
  return nonNull(version.configured) && unstableFeatures.includes('runtime_version')
    ? {
        version: normalizeRuntimeVersion(version.configured),
        status: 'custom' as const
      }
    : {
        version: normalizeRuntimeVersion(version.runtime),
        status:
          normalizeRuntimeVersion(version.runtime) === normalizeRuntimeVersion(version.base)
            ? ('latest' as const)
            : ('update_available' as const)
      }
}
