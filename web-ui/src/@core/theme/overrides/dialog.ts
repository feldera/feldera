// ** MUI Imports
import { Theme } from '@mui/material/styles'

// ** Util Import
import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

const Dialog = (theme: Theme) => {
  return {
    MuiDialog: {
      styleOverrides: {
        paper: {
          boxShadow: theme.shadows[6],
          '&:not(.MuiDialog-paperFullScreen)': {
            '@media (max-width:599px)': {
              margin: theme.spacing(4),
              width: `calc(100% - ${theme.spacing(8)})`,
              maxWidth: `calc(100% - ${theme.spacing(8)}) !important`
            }
          },
          '& > .MuiList-root': {
            paddingLeft: theme.spacing(1),
            paddingRight: theme.spacing(1)
          }
        }
      }
    },
    MuiDialogTitle: {
      styleOverrides: {
        root: {
          padding: theme.spacing(5)
        }
      }
    },
    MuiDialogContent: {
      styleOverrides: {
        root: {
          padding: theme.spacing(5),
          '& + .MuiDialogContent-root': {
            paddingTop: 0
          },
          '& + .MuiDialogActions-root': {
            paddingTop: 0
          },

          // Styling for Mobile Date Picker starts
          '& .PrivatePickersToolbar-root': {
            padding: theme.spacing(4, 5),
            color: theme.palette.primary.contrastText,
            backgroundColor: theme.palette.primary.main,
            '& .MuiTypography-root': {
              color: theme.palette.primary.contrastText
            },
            '& span.MuiTypography-overline': {
              fontSize: '1rem',
              lineHeight: '24px',
              letterSpacing: '0.15px'
            },
            '& ~ div[class^="css-"] > div[class^="css-"]': {
              marginTop: theme.spacing(6),
              marginBottom: theme.spacing(6),
              '& > div[class^="css-"]': {
                backgroundColor:
                  theme.palette.mode === 'light' ? theme.palette.grey[50] : theme.palette.background.default,
                '& ~ .MuiIconButton-root span.MuiTypography-caption': {
                  color: 'inherit'
                }
              }
            },
            '& .PrivateTimePickerToolbar-hourMinuteLabel': {
              alignItems: 'center',
              '& > .MuiButton-root span.MuiTypography-root': {
                fontWeight: 300,
                lineHeight: '72px',
                fontSize: '3.75rem',
                letterSpacing: '-0.5px'
              },
              '& > .MuiTypography-root': {
                color: hexToRGBA(theme.palette.primary.contrastText, 0.54),
                '& + .MuiButton-root > span.MuiTypography-root': {
                  color: hexToRGBA(theme.palette.primary.contrastText, 0.54)
                }
              }
            },
            '& .PrivateTimePickerToolbar-ampmSelection span.MuiTypography-root:not(.Mui-selected)': {
              color: hexToRGBA(theme.palette.primary.contrastText, 0.54)
            }
          }

          // Styling for Mobile Date Picker ends
        }
      }
    },
    MuiDialogActions: {
      styleOverrides: {
        root: {
          padding: theme.spacing(5),
          '&.dialog-actions-dense': {
            padding: theme.spacing(2.5),
            paddingTop: 0
          }
        }
      }
    }
  }
}

export default Dialog
