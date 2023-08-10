// The save indicator that is shown while editing programs or pipelines and
// indicates if the modified version has been saved or is currently being saved.

import ColoredChip from 'src/@core/components/mui/chip'
import { Tooltip } from '@mui/material'
import SyncIcon from '@mui/icons-material/Sync'
import CloudDoneIcon from '@mui/icons-material/CloudDone'

export type SaveIndicatorState = 'isNew' | 'isDebouncing' | 'isModified' | 'isSaving' | 'isUpToDate'

// Options passed to the SaveIndicator component.
export interface SaveIndicatorProps {
  id?: string
  state: SaveIndicatorState
  stateToLabel: (state: SaveIndicatorState) => string
}

// Given a save state return the icon displayed in the Chip.
const stateToIcon = (state: SaveIndicatorState) => {
  const doneIcon = <CloudDoneIcon />
  const saveIcon = <SyncIcon />

  switch (state) {
    case 'isDebouncing':
    case 'isSaving':
    case 'isModified':
      return saveIcon
    case 'isNew':
    case 'isUpToDate':
      return doneIcon
  }
}

// The SaveIndicator displaying status of the form save state.
const SaveIndicator = (props: SaveIndicatorProps) => {
  const label = props.stateToLabel(props.state)
  const tooltip = props.state === ('isUpToDate' as const) ? 'Everything saved to cloud' : ''

  return (
    <Tooltip title={tooltip}>
      <ColoredChip label={label} skin='light' sx={{ mr: 2 }} icon={stateToIcon(props.state)} id={props.id} />
    </Tooltip>
  )
}

export default SaveIndicator
