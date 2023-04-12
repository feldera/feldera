// The save indicator that is shown while editing programs or pipelines and
// indicates if the modified version has been saved or is currently being saved.

import CustomChip from 'src/@core/components/mui/chip'
import { Tooltip } from '@mui/material'
import SyncIcon from '@mui/icons-material/Sync'
import CloudDoneIcon from '@mui/icons-material/CloudDone'

export type SaveIndicatorState = 'isNew' | 'isDebouncing' | 'isModified' | 'isSaving' | 'isUpToDate'

export interface SaveIndicatorProps {
  state: SaveIndicatorState
  stateToLabel: (state: SaveIndicatorState) => string
}

const SaveIndicator = (props: SaveIndicatorProps) => {
  const label = props.stateToLabel(props.state)

  const tooltip = props.state === ('isUpToDate' as const) ? 'Everything saved to cloud' : ''
  const doneIcon = <CloudDoneIcon />
  const saveIcon = <SyncIcon />

  return (
    <Tooltip title={tooltip}>
      <CustomChip
        label={label}
        skin='light'
        sx={{ mr: 2 }}
        icon={
          props.state === ('isSaving' as const) ||
          props.state === ('isModified' as const) ||
          props.state === ('isDebouncing' as const)
            ? saveIcon
            : doneIcon
        }
      />
    </Tooltip>
  )
}

export default SaveIndicator
