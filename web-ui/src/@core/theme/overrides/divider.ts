// ** MUI Imports
import { Theme } from '@mui/material/styles'

const Divider = (theme: Theme) => {
  return {
    MuiDivider: {
      styleOverrides: {
        root: {
          margin: `${theme.spacing(2)} 0`
        }
      }
    }
  }
}

export default Divider
