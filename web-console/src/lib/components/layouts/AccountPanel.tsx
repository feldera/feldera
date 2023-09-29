import { useAuthentication } from 'src/lib/compositions/useAuth'
import invariant from 'tiny-invariant'

import { Button, Paper, Typography } from '@mui/material'

export const AccountPanel = () => {
  const auth = useAuthentication()
  invariant(auth)
  return (
    <Paper elevation={0} sx={{ width: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant='h6' sx={{ p: 4 }}>
        Hello, {auth.user.username}!
      </Typography>
      <Button onClick={auth.signOut}>Log out</Button>
    </Paper>
  )
}
