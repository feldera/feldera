// Website theme config.
//
// You can use the `useTheme()` hook to access this data.

import { PaletteMode } from '@mui/material'
import { ContentWidth } from 'src/@core/layouts/types'

type ThemeConfig = {
  mode: PaletteMode
  templateName: string
  routingLoader: boolean
  disableRipple: boolean
  navigationSize: number
  menuTextTruncate: boolean
  contentWidth: ContentWidth
  responsiveFontSizes: boolean
}

const themeConfig: ThemeConfig = {
  templateName: 'dbsp',
  mode: 'light' /* light | dark */,
  contentWidth: 'boxed' /* full | boxed */,
  routingLoader: true,
  menuTextTruncate: true,
  navigationSize: 260 /* Number in PX(Pixels) (Note: This is for the Vertical navigation menu only) */,
  responsiveFontSizes: true,
  disableRipple: false
}

export default themeConfig
