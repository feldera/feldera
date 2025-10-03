import { nonNull } from '$lib/functions/common/function'

export const normalizeRuntimeVersion = (version: string) => version.replace(/\+.*/, '')

export const getRuntimeVersionStatus = (version: {
  runtime: string
  base: string
  configured: string | null | undefined
}) => {
  return normalizeRuntimeVersion(version.runtime) === normalizeRuntimeVersion(version.base)
    ? ('latest' as const)
    : nonNull(version.configured)
      ? ('custom' as const)
      : ('update_available' as const)
}
