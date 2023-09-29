import { useAuthenticator } from '@aws-amplify/ui-react'

export const useAuthentication = () => {
  const { authStatus, user, signOut } = useAuthenticator(context => [context.user])
  if (authStatus !== 'authenticated') {
    return null
  }
  const session = user.getSignInUserSession()!
  return { user, bearer: session.getAccessToken().getJwtToken(), signOut }
}
