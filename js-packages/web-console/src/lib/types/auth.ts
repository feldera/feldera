import type { OidcUserInfo } from '@axa-fr/oidc-client'

export type UserProfile = {
  id?: string
  name?: string | null
  email?: string | null
  picture?: string | null
}

export type AuthDetails =
  | 'none'
  | {
      login: () => Promise<void>
    }
  | {
      logout: (params: { callbackUrl: string }) => Promise<void>
      userInfo: OidcUserInfo
      profile: UserProfile
      accessToken: string
    }
