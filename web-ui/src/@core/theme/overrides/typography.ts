// ** MUI Imports
import { Theme } from '@mui/material/styles'

const Typography = (theme: Theme) => {
  return {
    MuiTypography: {
      styleOverrides: {
        gutterBottom: {
          marginBottom: theme.spacing(2)
        }
      }
    }
  }
}

export default Typography
