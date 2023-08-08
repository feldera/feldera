// This is the top bar that decides whether to show the hamburger menu or not it
// also has the search bar and the user dropdown and the notification dropdown.
import Box from '@mui/material/Box'
import { Theme } from '@mui/material/styles'
import TextField from '@mui/material/TextField'
import IconButton from '@mui/material/IconButton'
import useMediaQuery from '@mui/material/useMediaQuery'
import InputAdornment from '@mui/material/InputAdornment'
import Menu from 'mdi-material-ui/Menu'
import Magnify from 'mdi-material-ui/Magnify'
import { Settings } from 'src/@core/context/settingsContext'
import ModeToggler from 'src/@core/layouts/components/shared-components/ModeToggler'
import UserDropdown from 'src/@core/layouts/components/shared-components/UserDropdown'
import NotificationDropdown from 'src/@core/layouts/components/shared-components/NotificationDropdown'
import { usePageHeader } from 'src/compositions/ui/pageTitle'

interface Props {
  hidden: boolean
  settings: Settings
  toggleNavVisibility: () => void
  saveSettings: (values: Settings) => void
}

const AppBarContent = (props: Props) => {
  const { hidden, settings, saveSettings, toggleNavVisibility } = props
  const hiddenSm = useMediaQuery((theme: Theme) => theme.breakpoints.down('sm'))
  const header = usePageHeader(s => s.header)
  return (
    <Box sx={{ width: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', py: 4 }}>
        <Box className='actions-left' sx={{ display: 'flex', alignItems: 'top', gap: 4 }}>
          {hidden ? (
            <IconButton color='inherit' onClick={toggleNavVisibility} sx={{ mb: 'auto' }}>
              <Menu />
            </IconButton>
          ) : null}
          {!hiddenSm ? <Box sx={{ width: '20rem' }}>{header}</Box> : <></>}
          <TextField
            size='small'
            sx={{ '& .MuiOutlinedInput-root': { borderRadius: 4 } }}
            InputProps={{
              startAdornment: (
                <InputAdornment position='start'>
                  <Magnify fontSize='small' />
                </InputAdornment>
              )
            }}
          />
        </Box>
        <Box className='actions-right' sx={{ display: 'flex', alignItems: 'center', mb: 'auto' }}>
          <ModeToggler settings={settings} saveSettings={saveSettings} />
          <NotificationDropdown />
          <UserDropdown />
        </Box>
      </Box>
      {hiddenSm ? <Box>{header}</Box> : <></>}
    </Box>
  )
}

export default AppBarContent
