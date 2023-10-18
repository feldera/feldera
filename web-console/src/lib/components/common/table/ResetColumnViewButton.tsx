import { Icon } from '@iconify/react'
import { IconButton, Tooltip } from '@mui/material'

export const ResetColumnViewButton = (props: { resetGridViewModel: () => void }) => {
  return (
    <Tooltip title='Reset column headers'>
      <IconButton onClick={props.resetGridViewModel}>
        <Icon icon='bx:move-horizontal' />
      </IconButton>
    </Tooltip>
  )
}
