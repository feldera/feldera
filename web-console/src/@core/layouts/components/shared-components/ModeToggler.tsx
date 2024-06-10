// Switches the layout between light/dark mode.
//
// Currently does not persist across full-reloads, for that we have to save the
import WeatherNight from 'mdi-material-ui/WeatherNight'
import WeatherSunny from 'mdi-material-ui/WeatherSunny'

import { Settings } from '@core/context/settingsTypes'
// settings in local storage.
import { PaletteMode } from '@mui/material'
import IconButton from '@mui/material/IconButton'

interface Props {
  settings: Settings
  saveSettings: (values: Settings) => void
}

const ModeToggler = (props: Props) => {
  const { settings, saveSettings } = props

  const handleModeChange = (mode: PaletteMode) => {
    saveSettings({ ...settings, mode })
  }

  const handleModeToggle = () => {
    if (settings.mode === 'light') {
      handleModeChange('dark')
    } else {
      handleModeChange('light')
    }
  }

  return (
    <IconButton
      color='inherit'
      aria-haspopup='true'
      onClick={handleModeToggle}
      data-testid={settings.mode === 'dark' ? 'button-light-mode' : 'button-dark-mode'}
    >
      {settings.mode === 'dark' ? <WeatherSunny /> : <WeatherNight />}
    </IconButton>
  )
}

export default ModeToggler
