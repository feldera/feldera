import { getAuthConfig } from '$lib/services/pipelineManager'
import { SvelteKitAuth } from '@auth/sveltekit'
import Cognito from '@auth/sveltekit/providers/cognito'
import Google from '@auth/sveltekit/providers/google'
import Auth0 from '@auth/sveltekit/providers/auth0'
import type { Provider } from '@auth/sveltekit/providers'
import { P, match } from 'ts-pattern'
import { base } from '$app/paths'

export const { handle, signIn, signOut } = SvelteKitAuth(async (event) => {
  const authConfig = await getAuthConfig()
  const providers = match(authConfig)
    .returnType<Provider[]>()
    .with({ AwsCognito: P.select() }, (config) => [
      Cognito({
        clientId: config.client_id,
        clientSecret: config.client_secret,
        issuer: config.issuer
      })
    ])
    .with({ GoogleIdentity: P.select() }, (config) => [
      Google({
        clientId: config.client_id,
        clientSecret: config.client_secret
      })
    ])
    .with({ Auth0: P.select() }, (config) => [
      Auth0({
        clientId: config.client_id,
        clientSecret: config.client_secret
      })
    ])
    .with(
      P.when((v) => typeof v === 'object' && Object.keys(v).length === 0),
      () => []
    )
    .exhaustive()
  return {
    providers,
    secret: import.meta.env.VITE_AUTH_SECRET,
    trustHost: true
    // basePath: `${base}/auth` // [origin]/auth/callback/[provider]
  }
})
