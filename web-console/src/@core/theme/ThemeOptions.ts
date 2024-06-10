import '@fontsource-variable/public-sans'

import { Settings } from '@core/context/settingsTypes'
import { ThemeOptions } from '@mui/material'
import { deepmerge } from '@mui/utils'

import breakpoints from './breakpoints'
import palette from './palette'
import shadows from './shadows'
import spacing from './spacing'

// https://stackoverflow.com/questions/74269160/how-to-import-google-font-with-vitejs

const themeOptions = (settings: Settings): ThemeOptions => {
  const { mode, themeColor } = settings

  const themeConfig = {
    palette: palette(mode, themeColor),
    typography: {
      fontFamily: 'Public Sans Variable'
    },
    shadows: shadows(mode),
    ...spacing,
    breakpoints: breakpoints(),
    shape: {
      borderRadius: 6
    },
    mixins: {
      toolbar: {
        minHeight: 64
      }
    }
  }

  return deepmerge(themeConfig, {
    palette: {
      primary: {
        ...themeConfig.palette[themeColor]
      }
    }
  })
}

export default themeOptions
