'use client'

import { useAuth } from '$lib/compositions/auth/useAuth'
// This is the top bar that decides whether to show the hamburger menu or not it
import Menu from 'mdi-material-ui/Menu'

import { Settings } from '@core/context/settingsTypes'
import ModeToggler from '@core/layouts/components/shared-components/ModeToggler'
import UserDropdown from '@core/layouts/components/shared-components/UserDropdown'
import { Tooltip } from '@mui/material'
// also has the search bar and the user dropdown and the notification dropdown.
import Box from '@mui/material/Box'
import IconButton from '@mui/material/IconButton'

interface Props {
  hidden: boolean
  settings: Settings
  toggleNavVisibility: () => void
  saveSettings: (values: Settings) => void
}

const AppBarContent = (props: Props) => {
  const { hidden, settings, saveSettings, toggleNavVisibility } = props
  const { auth } = useAuth()
  return (
    <Box sx={{ width: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', py: 4 }}>
        <Box className='actions-left' sx={{ display: 'flex', alignItems: 'top', gap: 4, pointerEvents: 'auto' }}>
          {hidden ? (
            <IconButton color='inherit' onClick={toggleNavVisibility} sx={{ mb: 'auto' }}>
              <Menu />
            </IconButton>
          ) : null}
        </Box>
        <Box
          className='actions-right'
          sx={{ display: 'flex', alignItems: 'center', mb: 'auto', pointerEvents: 'auto' }}
        >
          <ModeToggler settings={settings} saveSettings={saveSettings} />
          {auth === 'NoAuth' ? (
            <Tooltip title='No authentication'>
              <IconButton disableRipple data-testid='icon-no-auth' sx={{ width: 40, height: 40 }}>
                <i className={`bx bx-lock-open-alt`} style={{}} />
              </IconButton>
            </Tooltip>
          ) : (
            <UserDropdown />
          )}
        </Box>
      </Box>
    </Box>
  )
}

export default AppBarContent
