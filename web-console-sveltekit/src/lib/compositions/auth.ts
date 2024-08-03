import { getAuthConfig } from '$lib/services/pipelineManager'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'
// import { decode } from '@auth/core/jwt'
//
// import { SvelteKitAuth } from '@auth/sveltekit'
// import Auth0, { type Auth0Profile } from '@auth/sveltekit/providers/auth0'
// import Cognito, { type CognitoProfile } from '@auth/sveltekit/providers/cognito'
// import Google from '@auth/sveltekit/providers/google'
// import { sequence } from '@sveltejs/kit/hooks'

// import type {
//   AuthorizationEndpointHandler,
//   CommonProviderOptions,
//   OAuth2Config,
//   OAuthConfig,
//   OIDCConfig,
//   Provider,
//   TokenEndpointHandler
// } from '@auth/sveltekit/providers'
import { base } from '$app/paths'

// type AuthDetails =
//   | {
//       enabled: false
//     }
//   | {
//       enabled: true
//       providerId: string
//       providerSignOutUrl: string
//     }

// declare global {
//   namespace App {
//     interface Locals {
//       authDetails: AuthDetails
//     }
//     interface PageData {
//       authDetails: AuthDetails
//     }
//   }
// }

// declare module '@auth/core/types' {
//   // Extend session to hold the access_token
//   interface Session {
//     accessToken: string
//   }
// }

// declare module '@auth/core/jwt' {
//   // Extend token to hold the access_token before it gets put into session
//   interface JWT {
//     access_token: string
//   }
// }

// const providerAuth0 = ({
//   clientId,
//   endpoint
// }: {
//   clientId: string
//   endpoint: string
// }): OIDCConfig<Auth0Profile> => ({
//   type: 'oidc',
//   id: 'auth0',
//   name: 'Auth0',
//   style: { text: '#fff', bg: '#EB5424' },
//   clientId,
//   issuer: endpoint,
//   authorization: `${endpoint}authorize`,
//   token: `${endpoint}oauth/token`,
//   client: {
//     token_endpoint_auth_method: 'none' as const
//   },
//   checks: ['pkce' as const]
// })

// const signOutUrlAuth0 = ({ clientId, endpoint }: { clientId: string; endpoint: string }) =>
//   `${endpoint}v2/logout?client_id=${clientId}&returnTo={redirect_uri}`

// const providerCognito = ({
//   clientId,
//   endpoint,
//   issuer
// }: {
//   clientId: string
//   endpoint: string
//   issuer: string
// }): OIDCConfig<CognitoProfile> => ({
//   type: 'oidc' as const,
//   id: 'cognito',
//   name: 'Cognito',
//   style: { brandColor: '#C17B9E' },
//   clientId,
//   issuer,
//   authorization: `${endpoint}authorize`,
//   token: `${endpoint}token`,
//   client: {
//     token_endpoint_auth_method: 'none' as const
//   },
//   checks: ['pkce' as const]
// })

const signOutUrlCognito = ({
  client_id,
  authority,
  redirect_uri
}: {
  client_id: string
  authority: string
  redirect_uri: string
}) =>
  `${authority}logout?client_id=${client_id}&logout_uri=${redirect_uri}&redirect_uri=${redirect_uri}%2Fauth%2Fcallback%2Fcognito&response_type=code&scope=openid+profile+email`

// export const { authenticate } = (() => {
//   let providerId: string | undefined
//   let providerSignOutUrl: string | undefined
//   const { authenticate } = SvelteKitAuth(async (event) => {
//     const authConfig = await getAuthConfig()
//     const providers = match(authConfig)
//       .returnType<{ provider: OAuthConfig<any> & { id: string }; signOutUrl: string }[]>()
//       .with({ AwsCognito: P.select() }, (config) => {
//         const clientId = /client_id=(\w+)/.exec(config.login_url)?.[1]
//         const endpoint = /^(.*)login\?/.exec(config.login_url)?.[1]
//         const issuer = /(.*)\/.well-known\/jwks.json/.exec(config.jwk_uri)?.[1]
//         invariant(clientId, 'Cognito clientId is not valid')
//         invariant(endpoint, 'Cognito endpoint is not valid')
//         invariant(issuer, 'Cognito issuer is not valid')
//         return [
//           {
//             provider: providerCognito({ clientId, endpoint, issuer }),
//             signOutUrl: signOutUrlCognito({ clientId, endpoint })
//           }
//         ]
//       })
//       .with({ GoogleIdentity: P.select() }, (config) => [
//         {
//           provider: Google({
//             clientId: config.client_id,
//             checks: ['pkce']
//           }),
//           signOutUrl: ''
//         }
//       ])
//       // .with({ Auth0: P.select() }, (config) => [
//       //   {
//       //     provider: providerAuth0({clientId: config.client_id, endpoint: config.endpoint}),
//       //     signOutUrl: signOutUrlAuth0({clientId: config.client_id, endpoint: config.endpoint})
//       //   }
//       // ])
//       .with(
//         P.when((v) => typeof v === 'object' && Object.keys(v).length === 0),
//         () => []
//       )
//       .exhaustive()
//     invariant(import.meta.env.VITE_AUTH_SECRET, 'You need to provide VITE_AUTH_SECRET during build')

//     providerId = providers[0]?.provider.id
//     providerSignOutUrl = providers[0]?.signOutUrl
//     return {
//       providers: providers.map((p) => p.provider),
//       secret: import.meta.env.VITE_AUTH_SECRET,
//       trustHost: true,
//       callbacks: {
//         redirect: ({ url, baseUrl }) => {
//           // Allows relative callback URLs
//           if (url.startsWith('/')) return `${baseUrl}${url}`
//           // Allow callback URLs on the same origin and other origins
//           return url
//         },
//         session: async (params) => {
//           params.session.accessToken = params.token.access_token
//           return params.session
//         },
//         jwt: async (params) => {
//           if (params.account?.access_token) {
//             params.token.access_token = params.account.access_token
//           }

//           return params.token
//         }
//       }
//       // basePath: `${base}/auth` // [origin]/auth/callback/[provider] ( /new/auth/callback/cognito )
//     }
//   })
//   return {
//     authenticate: ((input) => {
//       input.event.locals.authDetails =
//         providerId && providerSignOutUrl
//           ? { enabled: true, providerId, providerSignOutUrl } // fetch('https://dev-jzraqtxsr8a3hhhv.us.auth0.com/oidc/logout?id_token_hint={yourIdToken}&post_logout_redirect_uri={yourCallbackUrl}')}
//           : { enabled: false }
//       return handle(input)
//     }) satisfies typeof authenticate,
//   }
// })()

export type OidcConfig = {
  client_id: string
  authority: string
  response_type: 'code'
  scope: string
  metadata: {
    issuer: string
    authorization_endpoint: string
    token_endpoint: string
    revocation_endpoint: string
    userinfo_endpoint: string
    end_session_endpoint: string
  }
  redirect_uri: string
  post_logout_redirect_uri: string
  client_authentication: 'client_secret_basic'
  loadUserInfo: true
}

type AuthConfig = { oidc: OidcConfig; logoutExtras?: Record<string, string> }

export const loadAuthConfig = async () => {
  const authConfig = await getAuthConfig()
  return (
    match(authConfig)
      .returnType<AuthConfig | null>()
      .with({ AwsCognito: P.select() }, (config) => {
        const clientId = /client_id=(\w+)/.exec(config.login_url)?.[1]
        const endpoint = /^(.*)login\?/.exec(config.login_url)?.[1]
        const issuer = /(.*)\/.well-known\/jwks.json/.exec(config.jwk_uri)?.[1]
        invariant(clientId, 'Cognito clientId is not valid')
        invariant(endpoint, 'Cognito endpoint is not valid')
        invariant(issuer, 'Cognito issuer is not valid')
        return {
          oidc: {
            authority: endpoint,
            client_id: clientId,
            response_type: 'code',
            scope: 'openid profile email',
            metadata: {
              issuer,
              authorization_endpoint: `${endpoint}authorize`,
              token_endpoint: `${endpoint}token`,
              revocation_endpoint: `${endpoint}oauth2/revoke`,
              userinfo_endpoint: `${endpoint}oauth2/userInfo`,
              end_session_endpoint: `${endpoint}logout` // signOutUrlCognito({client_id: clientId, authority: endpoint, redirect_uri: window.location.origin + base})
            },
            redirect_uri: `${window.location.origin}${base}/auth/callback/`,
            post_logout_redirect_uri: `${base}/`,
            client_authentication: 'client_secret_basic',
            loadUserInfo: true
          },
          logoutExtras: {
            client_id: clientId,
            redirect_uri: `${window.location.origin}${base}/auth/callback/`,
            response_type: 'code'
          }
        }
      })
      .with({ GoogleIdentity: P.select() }, (config) => ({}) as any)
      // .with({ Auth0: P.select() }, (config) => [
      //   {
      //     provider: providerAuth0({clientId: config.client_id, endpoint: config.endpoint}),
      //     signOutUrl: signOutUrlAuth0({clientId: config.client_id, endpoint: config.endpoint})
      //   }
      // ])
      .with(
        P.when((v) => typeof v === 'object' && Object.keys(v).length === 0),
        () => null
      )
      .exhaustive()
  )
  // invariant(import.meta.env.VITE_AUTH_SECRET, 'You need to provide VITE_AUTH_SECRET during build')
}

export type UserProfile = {
  id?: string
  name?: string | null
  email?: string | null
  image?: string | null
}
