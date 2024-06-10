import { hexToRGBA } from '@core/utils/hex-to-rgba'
import { Theme } from '@mui/material/styles'

const Chip = (theme: Theme) => {
  return {
    MuiChip: {
      styleOverrides: {
        root: {
          height: 28,
          '&.MuiChip-rounded': {
            borderRadius: 4
          }
        },
        sizeSmall: {
          height: 24
        },
        outlined: {
          '&.MuiChip-colorDefault': {
            borderColor: hexToRGBA(theme.palette.customColors.main, 0.22)
          }
        },
        deleteIcon: {
          width: 18,
          height: 18
        },
        avatar: {
          width: 22,
          height: 22,
          color: theme.palette.text.primary
        },
        iconColorDefault: {
          color: theme.palette.text.primary
        },
        label: {
          textTransform: 'uppercase',
          padding: theme.spacing(0, 2.5)
        },
        deletableColorPrimary: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.primary.main, 0.7),
            '&:hover': {
              color: theme.palette.primary.main
            }
          }
        },
        deletableColorSecondary: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.secondary.main, 0.7),
            '&:hover': {
              color: theme.palette.secondary.main
            }
          }
        },
        deletableColorSuccess: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.success.main, 0.7),
            '&:hover': {
              color: theme.palette.success.main
            }
          }
        },
        deletableColorError: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.error.main, 0.7),
            '&:hover': {
              color: theme.palette.error.main
            }
          }
        },
        deletableColorWarning: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.warning.main, 0.7),
            '&:hover': {
              color: theme.palette.warning.main
            }
          }
        },
        deletableColorInfo: {
          '&.MuiChip-light .MuiChip-deleteIcon': {
            color: hexToRGBA(theme.palette.info.main, 0.7),
            '&:hover': {
              color: theme.palette.info.main
            }
          }
        }
      }
    }
  }
}

export default Chip
