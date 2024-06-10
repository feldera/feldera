import { hexToRGBA } from '@core/utils/hex-to-rgba'
import { Theme } from '@mui/material/styles'

const Backdrop = (theme: Theme) => {
  return {
    MuiBackdrop: {
      styleOverrides: {
        root: {
          backgroundColor:
            theme.palette.mode === 'light'
              ? hexToRGBA(theme.palette.customColors.main, 0.7)
              : hexToRGBA(theme.palette.background.default, 0.7)
        },
        invisible: {
          backgroundColor: 'transparent'
        }
      }
    }
  }
}

export default Backdrop
