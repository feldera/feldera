'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
import { Fragment, SyntheticEvent, useState } from 'react'
import invariant from 'tiny-invariant'

import { useClipboard } from '@mantine/hooks'
import { Button, Link } from '@mui/material'
import Avatar from '@mui/material/Avatar'
import Badge from '@mui/material/Badge'
import Box from '@mui/material/Box'
import Divider from '@mui/material/Divider'
import Menu from '@mui/material/Menu'
import MenuItem from '@mui/material/MenuItem'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

const BadgeContentSpan = styled('span')(({ theme }) => ({
  width: 8,
  height: 8,
  borderRadius: '50%',
  backgroundColor: theme.palette.success.main,
  boxShadow: `0 0 0 2px ${theme.palette.background.paper}`
}))

const UserDropdown = () => {
  const { copy } = useClipboard()
  const [anchorEl, setAnchorEl] = useState<Element | null>(null)
  const { auth: _auth, setAuth } = useAuth()
  invariant(typeof _auth === 'object' && 'Authenticated' in _auth)
  const auth = _auth['Authenticated']

  const handleDropdownOpen = (event: SyntheticEvent) => {
    setAnchorEl(event.currentTarget)
  }

  const handleDropdownClose = () => {
    setAnchorEl(null)
  }

  const styles = {
    py: 2,
    px: 4,
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    gap: 2,
    color: 'text.primary',
    textDecoration: 'none',
    '& svg': {
      fontSize: '1.375rem',
      color: 'text.secondary'
    }
  }

  return (
    <Fragment>
      <Badge
        overlap='circular'
        onClick={handleDropdownOpen}
        sx={{ ml: 2, cursor: 'pointer' }}
        badgeContent={<BadgeContentSpan />}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Avatar
          alt={'Avatar of ' + auth.user.username}
          onClick={handleDropdownOpen}
          sx={{ width: 40, height: 40 }}
          data-testid='button-avatar-menu'
          src={auth.user.avatar}
        />
      </Badge>
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={() => handleDropdownClose()}
        sx={{ '& .MuiMenu-paper': { width: 230, marginTop: 4 } }}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
      >
        <Box sx={{ py: 2, px: 4, display: 'flex', flexDirection: 'column', gap: 4 }}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Badge
              overlap='circular'
              badgeContent={<BadgeContentSpan />}
              anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            >
              <Avatar
                alt={'Avatar of ' + auth.user.username}
                sx={{ width: '2.5rem', height: '2.5rem' }}
                src={auth.user.avatar}
              />
            </Badge>
            <Box sx={{ display: 'flex', marginLeft: 3, alignItems: 'flex-start', flexDirection: 'column' }}>
              <Typography sx={{ fontWeight: 600 }}>{auth.user.username}</Typography>
              {!!auth.user.contacts.email && (
                <Typography variant='body2' sx={{ fontSize: '0.8rem' }}>
                  {auth.user.contacts.email}
                </Typography>
              )}
              {!!auth.user.contacts.phone && (
                <Typography variant='body2' sx={{ fontSize: '0.8rem' }}>
                  {auth.user.contacts.phone}
                </Typography>
              )}
              <Typography variant='body2' sx={{ fontSize: '0.8rem', color: 'text.disabled' }}>
                user
              </Typography>
            </Box>
          </Box>
          <Box>
            <Button
              variant='outlined'
              size='small'
              sx={{ display: 'flex' }}
              onClick={() => copy(auth.credentials.bearer)}
            >
              <Typography variant='caption'>Copy bearer token</Typography>
            </Button>
          </Box>
        </Box>
        <Divider sx={{ mt: 0, mb: 1 }} />
        <MenuItem sx={{ p: 0 }} onClick={() => handleDropdownClose()} disabled data-testid='button-profile'>
          <Box sx={styles}>
            <i className={`bx bx-user`} style={{}} />
            Profile
          </Box>
        </MenuItem>
        <Link href='/settings/' data-testid='button-settings'>
          <MenuItem sx={{ p: 0 }}>
            <Box sx={styles}>
              <i className={`bx bx-cog`} style={{}} />
              Settings
            </Box>
          </MenuItem>
        </Link>
        <Divider />
        <Link href={auth.signOutUrl} data-testid='button-signout'>
          <MenuItem sx={styles} onClick={() => setTimeout(() => setAuth('Unauthenticated'), 0)}>
            <i className={`bx bx-log-out`} style={{}} />
            Logout
          </MenuItem>
        </Link>
      </Menu>
    </Fragment>
  )
}

export default UserDropdown
