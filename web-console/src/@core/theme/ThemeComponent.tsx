import themeConfig from '$lib/functions/configs/themeConfig'
import { ReactNode } from 'react'

import { Settings } from '@core/context/settingsTypes'
import CssBaseline from '@mui/material/CssBaseline'
import GlobalStyles from '@mui/material/GlobalStyles'
import { createTheme, responsiveFontSizes, ThemeProvider } from '@mui/material/styles'

import GlobalStyling from './globalStyles'
import overrides from './overrides'
import themeOptions from './ThemeOptions'
import typography from './typography'

interface Props {
  settings: Settings
  children: ReactNode
}

const ThemeComponent = (props: Props) => {
  const { settings, children } = props

  // Merged ThemeOptions of Core and User
  const coreThemeConfig = themeOptions(settings)

  // Pass ThemeOptions to CreateTheme Function to create partial theme without component overrides
  let theme = createTheme(coreThemeConfig)

  // Continue theme creation and pass merged component overrides to CreateTheme function
  theme = createTheme(theme, {
    components: { ...overrides(theme) },
    typography: { ...typography(theme) }
  })

  // Set responsive font sizes to true
  if (themeConfig.responsiveFontSizes) {
    theme = responsiveFontSizes(theme)
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles styles={() => GlobalStyling(theme) as any} />
      {children}
    </ThemeProvider>
  )
}

export default ThemeComponent
