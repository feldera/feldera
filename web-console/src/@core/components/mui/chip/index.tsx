import React, { ForwardedRef } from 'react'

import { hexToRGBA } from '@core/utils/hex-to-rgba'
import MuiChip from '@mui/material/Chip'
import { useTheme } from '@mui/material/styles'

import { CustomChipProps } from './types'

const Chip = React.forwardRef((props: CustomChipProps, ref: ForwardedRef<any>) => {
  const { sx, skin, color } = props
  const theme = useTheme()
  const colors = {
    default: { color: theme.palette.primary.main, backgroundColor: hexToRGBA(theme.palette.primary.main, 0.16) },
    primary: { color: theme.palette.primary.main, backgroundColor: hexToRGBA(theme.palette.primary.main, 0.16) },
    secondary: { color: theme.palette.secondary.main, backgroundColor: hexToRGBA(theme.palette.secondary.main, 0.16) },
    success: { color: theme.palette.success.main, backgroundColor: hexToRGBA(theme.palette.success.main, 0.16) },
    error: { color: theme.palette.error.main, backgroundColor: hexToRGBA(theme.palette.error.main, 0.16) },
    warning: { color: theme.palette.warning.main, backgroundColor: hexToRGBA(theme.palette.warning.main, 0.16) },
    info: { color: theme.palette.info.main, backgroundColor: hexToRGBA(theme.palette.info.main, 0.16) }
  }

  const propsToPass = { ...props, ref: ref }
  propsToPass.rounded = undefined

  return (
    <MuiChip
      {...propsToPass}
      variant='filled'
      className='MuiChip-rounded MuiChip-light'
      sx={skin === 'light' && color ? Object.assign(colors[color], sx) : sx}
    />
  )
})

export default Chip
