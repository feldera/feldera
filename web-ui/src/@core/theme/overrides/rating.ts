// ** MUI Imports
import { Theme } from '@mui/material/styles'

const Rating = (theme: Theme) => {
  return {
    MuiRating: {
      styleOverrides: {
        root: {
          color: theme.palette.warning.main
        }
      }
    }
  }
}

export default Rating
