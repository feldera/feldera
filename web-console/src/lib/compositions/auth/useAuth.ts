import { LS_PREFIX } from '$lib/types/localStorage'
import { createContext, useContext } from 'react'

import { useLocalStorage } from '@mantine/hooks'

export type AuthUserInfo = {
  username: string
}

export type AuthData = {
  user: AuthUserInfo
  bearer: string
  signOutUrl: string
}

export type AuthStoreState = AuthData | 'Unauthenticated' | 'NoAuth'

export const useAuthStore = () => {
  const [auth, setAuth] = useLocalStorage<AuthStoreState>({
    key: LS_PREFIX + 'auth',
    defaultValue: 'Unauthenticated'
  })
  return { auth, setAuth }
}

export const authContext = createContext<AuthStoreState>(undefined!)

export const useAuth = () => {
  const { setAuth } = useAuthStore()
  const auth = useContext(authContext)
  return { auth, setAuth }
}
