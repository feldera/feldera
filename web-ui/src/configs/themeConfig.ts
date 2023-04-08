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
  templateName: 'dbsp' /* App Name */,
  mode: 'light' /* light | dark */,
  contentWidth: 'boxed' /* full | boxed */,
  routingLoader: true /* true | false */,
  menuTextTruncate: true /* true | false */,
  navigationSize: 260 /* Number in PX(Pixels) (Note: This is for the Vertical navigation menu only) */,
  responsiveFontSizes: true /* true | false */,
  disableRipple: false /* true | false */
}

export default themeConfig
