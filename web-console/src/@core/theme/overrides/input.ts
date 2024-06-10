import { hexToRGBA } from '@core/utils/hex-to-rgba'
import { Theme } from '@mui/material/styles'

const input = (theme: Theme) => {
  return {
    MuiInputLabel: {
      styleOverrides: {
        root: {
          color: theme.palette.text.secondary
        },
        '& .Mui-error': {
          backgroundColor: `rgb(126,10,15, ${theme.palette.mode === 'dark' ? 0 : 0.1})`,
          color: theme.palette.error.main
        }
      }
    },
    MuiInput: {
      styleOverrides: {
        root: {
          '&:before': {
            borderBottom: `1px solid ${hexToRGBA(theme.palette.customColors.main, 0.22)}`
          },
          '&:hover:not(.Mui-disabled):before': {
            borderBottom: `1px solid ${hexToRGBA(theme.palette.customColors.main, 0.32)}`
          },
          '&.Mui-disabled:before': {
            borderBottom: `1px solid ${theme.palette.text.disabled}`
          }
        }
      }
    },
    MuiFilledInput: {
      styleOverrides: {
        root: {
          backgroundColor: hexToRGBA(theme.palette.customColors.main, 0.04),
          '&:hover:not(.Mui-disabled)': {
            backgroundColor: hexToRGBA(theme.palette.customColors.main, 0.08)
          },
          '&:before': {
            borderBottom: `1px solid ${hexToRGBA(theme.palette.customColors.main, 0.22)}`
          },
          '&:hover:not(.Mui-disabled):before': {
            borderBottom: `1px solid ${hexToRGBA(theme.palette.customColors.main, 0.32)}`
          }
        }
      }
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          '&:hover:not(.Mui-focused) .MuiOutlinedInput-notchedOutline': {
            borderColor: hexToRGBA(theme.palette.customColors.main, 0.32)
          },
          '&:hover.Mui-error .MuiOutlinedInput-notchedOutline': {
            borderColor: theme.palette.error.main
          },
          '& .MuiOutlinedInput-notchedOutline': {
            borderColor: hexToRGBA(theme.palette.customColors.main, 0.22)
          },
          '&.Mui-disabled .MuiOutlinedInput-notchedOutline': {
            borderColor: theme.palette.text.disabled
          }
        }
      }
    }
  }
}

export default input
