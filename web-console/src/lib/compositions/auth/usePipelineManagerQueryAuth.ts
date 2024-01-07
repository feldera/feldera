import { useAuth } from '$lib/compositions/auth/useAuth'
import { ApiError } from '$lib/services/manager'
import { PipelineManagerQueryOptions, PublicPipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { match, P } from 'ts-pattern'

import { useQuery } from '@tanstack/react-query'

export const usePipelineManagerQueryAuth = (): PipelineManagerQueryOptions => {
  const { setAuth } = useAuth()
  const { data: authConfig } = useQuery(PublicPipelineManagerQuery.getAuthConfig())
  if (!authConfig) {
    return {}
  }
  return match(authConfig)
    .with({ AwsCognito: P.select() }, ({ grantType }) =>
      match(grantType)
        .returnType<PipelineManagerQueryOptions>()
        .with('token', () => ({
          onError: async (error: ApiError) => {
            if (error.status === 401) {
              setAuth('Unauthenticated')
            }
            return false
          }
        }))
        .with('code', () => ({
          onError: async (_error: ApiError) => {
            return true
          }
        }))
        .exhaustive()
    )
    .with({ GoogleIdentity: P._ }, () => ({}))
    .with('NoAuth', () => ({}))
    .exhaustive()
}
