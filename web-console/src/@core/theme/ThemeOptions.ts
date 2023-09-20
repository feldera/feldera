import { Public_Sans } from 'next/font/google'
import { Settings } from 'src/@core/context/settingsTypes'

import { ThemeOptions } from '@mui/material'
import { deepmerge } from '@mui/utils'

import breakpoints from './breakpoints'
import palette from './palette'
import shadows from './shadows'
import spacing from './spacing'

const publicSans = Public_Sans({
  weight: ['300', '400', '500', '600', '700'],
  style: ['normal'],
  subsets: ['latin']
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
