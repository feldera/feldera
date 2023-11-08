import localFont from 'next/font/local'
import { Settings } from 'src/@core/context/settingsTypes'

import { ThemeOptions } from '@mui/material'
import { deepmerge } from '@mui/utils'

import breakpoints from './breakpoints'
import palette from './palette'
import shadows from './shadows'
import spacing from './spacing'

const publicSans = localFont({
  src: '../../../public/fonts/PublicSans-VariableFont_wght.ttf',
  display: 'swap'
})

const themeOptions = (settings: Settings): ThemeOptions => {
  const { mode, themeColor } = settings

  const themeConfig = {
    palette: palette(mode, themeColor),
    typography: {
      fontFamily: publicSans.style.fontFamily
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
