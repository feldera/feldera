import { ReactElement } from 'react'
import { useAuthentication } from 'src/lib/compositions/useAuth'
import invariant from 'tiny-invariant'

import { Icon } from '@iconify/react'
import { Button, ButtonProps } from '@mui/material'

export const AccountButton = (props: ButtonProps) => {
  const auth = useAuthentication()
  invariant(auth)
  let profileImage = null as null | ReactElement
  if (!profileImage) {
    profileImage = <Icon icon='bx:user'></Icon>
  }
  return (
    <Button
      variant='outlined'
      startIcon={profileImage}
      sx={{ borderRadius: '2rem', textDecoration: 'none' }}
      {...props}
    >
      Account
    </Button>
  )
}
