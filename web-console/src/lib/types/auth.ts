import type { OidcUserInfo } from '@axa-fr/oidc-client'

export type UserProfile = {
  id?: string
  name?: string | null
  email?: string | null
  picture?: string | null
}

export type SignInDetails = {
  logout: (params: { callbackUrl: string | undefined }) => Promise<void>
  userInfo: OidcUserInfo
  profile: UserProfile
  accessToken: string
}

export type AuthDetails =
  | 'none'
  | {
      login: () => Promise<void>
    }
  | SignInDetails
