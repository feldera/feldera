import { getAuthConfig } from '$lib/services/pipelineManager'
import { SvelteKitAuth } from '@auth/sveltekit'
import Cognito from '@auth/sveltekit/providers/cognito'
import Google from '@auth/sveltekit/providers/google'
import Auth0 from '@auth/sveltekit/providers/auth0'
import type { Provider } from '@auth/sveltekit/providers'
import { P, match } from 'ts-pattern'
import { base } from '$app/paths'
import invariant from 'tiny-invariant'
import { sequence } from '@sveltejs/kit/hooks'
// import { PUBLIC_AUTH_SECRET } from '$env/static/public';

declare global {
  namespace App {
      interface Locals {
        authEnabled: boolean
      }
      interface PageData {
        authEnabled: boolean
      }
    }
  }

export const { handle, signIn, signOut } = (() => {
  let authEnabled = false
  const { handle, signIn, signOut } = SvelteKitAuth(async (event) => {
  const authConfig = await getAuthConfig()
  const providers = match(authConfig)
    .returnType<Provider[]>()
    .with({ AwsCognito: P.select() }, (config) => {
      config.jwk_uri
      const clientId = /client_id=(\w+)/.exec(config.login_url)?.[1]
      const issuer = /(.*)\/.well-known\/jwks.json/.exec(config.jwk_uri)?.[1]
      // console.log('zzcc', authConfig, clientId, issuer)
      invariant(clientId)
      invariant(issuer)
      return [
      Cognito({
        clientId,
        clientSecret: 'no secret',
        checks: ['pkce'],
        issuer
      })
    ]})
    .with({ GoogleIdentity: P.select() }, (config) => [
      Google({
        clientId: config.client_id,
        checks: ['pkce']
      })
    ])
    .with({ Auth0: P.select() }, (config) => [
      Auth0({
        clientId: config.client_id,
        checks: ['pkce']
      })
    ])
    .with(
      P.when((v) => typeof v === 'object' && Object.keys(v).length === 0),
      () => []
    )
    .exhaustive()
  invariant(import.meta.env.VITE_AUTH_SECRET, 'Need to provide VITE_AUTH_SECRET during build')
  console.log('VITE_AUTH_SECRET', import.meta.env.VITE_AUTH_SECRET)
  console.log('auth ok')
  authEnabled = providers.length !== 0
  return {
    providers,
    secret: import.meta.env.VITE_AUTH_SECRET,
    trustHost: true,
    // basePath: `${base}/auth` // [origin]/auth/callback/[provider] ( /new/auth/callback/cognito )
  }
})
return { handle: ((input) => {
  input.event.locals.authEnabled = authEnabled
  return handle(input)
}) satisfies typeof handle, signIn, signOut }
})()


// export const handle: typeof handleAuth = async (input) => {
//   const res = await handleAuth(input)
//   input.event.locals
//   sequence(handleAuth, x => x.resolve(x.event, {''}))(input)
//   res
// }