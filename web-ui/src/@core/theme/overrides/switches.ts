// ** MUI Imports
import { Theme } from '@mui/material/styles'

const Switch = (theme: Theme) => {
  return {
    MuiSwitch: {
      styleOverrides: {
        root: {
          '& .MuiSwitch-track': {
            backgroundColor: `rgb(${theme.palette.customColors.main})`
          }
        }
      }
    }
  }
}

export default Switch
