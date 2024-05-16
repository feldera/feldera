import { IconButton, Tooltip } from '@mui/material'
import { GridCallbackDetails, GridColumnVisibilityModel, useGridApiRef } from '@mui/x-data-grid-pro'

import { DataGridColumnViewModel } from './DataGridProDeclarative'

export const ResetColumnViewButton = (props: {
  setColumnViewModel?: (
    val: DataGridColumnViewModel | ((prevState: DataGridColumnViewModel) => DataGridColumnViewModel)
  ) => void
  setColumnVisibilityModel?: (model: GridColumnVisibilityModel, details: GridCallbackDetails<any>) => void
}) => {
  const apiRef = useGridApiRef()
  const resetColumnView = () => {
    props.setColumnViewModel?.([])
    props.setColumnVisibilityModel?.({}, { reason: 'restoreState', api: apiRef.current })
  }
  return (
    <Tooltip title='Reset columns'>
      <IconButton onClick={resetColumnView} data-testid='button-reset-columns' size='small'>
        <i className={`bx bx-move-horizontal`} style={{}} />
      </IconButton>
    </Tooltip>
  )
}
