import IconMoveHorizontal from '~icons/bx/move-horizontal'

import { IconButton, Tooltip } from '@mui/material'
import { GridCallbackDetails, GridColumnVisibilityModel } from '@mui/x-data-grid-pro'

import { DataGridColumnViewModel } from './DataGridProDeclarative'

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
    <Tooltip title='Reset columns'>
      <IconButton onClick={resetColumnView} data-testid='button-reset-columns' size='small'>
        <IconMoveHorizontal />
      </IconButton>
    </Tooltip>
  )
}
