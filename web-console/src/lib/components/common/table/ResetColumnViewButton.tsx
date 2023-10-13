import { Icon } from '@iconify/react'
import { IconButton, Tooltip } from '@mui/material'
import { GridCallbackDetails, GridColumnVisibilityModel } from '@mui/x-data-grid-pro'

import { DataGridColumnViewModel } from './DataGridPro'

export const ResetColumnViewButton = (props: {
  setColumnViewModel?: (
    val: DataGridColumnViewModel | ((prevState: DataGridColumnViewModel) => DataGridColumnViewModel)
  ) => void
  setColumnVisibilityModel?: (model: GridColumnVisibilityModel, details: GridCallbackDetails<any>) => void
}) => {
  const resetColumnView = () => {
    props.setColumnViewModel?.call(undefined, [])
    props.setColumnVisibilityModel?.call(undefined, {}, {})
  }
  return (
    <Tooltip title='Reset column headers'>
      <IconButton onClick={resetColumnView}>
        <Icon icon='bx:move-horizontal' />
      </IconButton>
    </Tooltip>
  )
}
