// The save indicator that is shown while editing programs or pipelines and
// indicates if the modified version has been saved or is currently being saved.

import ColoredChip from 'src/@core/components/mui/chip'

import CloudDoneIcon from '@mui/icons-material/CloudDone'
import SyncIcon from '@mui/icons-material/Sync'
import { Tooltip } from '@mui/material'

export type SaveIndicatorState = 'isNew' | 'isDebouncing' | 'isModified' | 'isSaving' | 'isUpToDate'

// Given a save state return the icon displayed in the Chip.
const stateToIcon = (state: SaveIndicatorState) => {
  switch (state) {
    case 'isDebouncing':
    case 'isSaving':
    case 'isModified':
      return <SyncIcon />
    case 'isNew':
    case 'isUpToDate':
      return <CloudDoneIcon />
  }
}

const stateToTestId = (state: SaveIndicatorState) => {
  switch (state) {
    case 'isDebouncing':
      return 'box-save-debouncing'
    case 'isSaving':
      return 'box-save-in-progress'
    case 'isModified':
      return 'box-save-modified'
    case 'isNew':
      return 'box-save-is-new'
    case 'isUpToDate':
      return 'box-save-saved'
  }
}

// The SaveIndicator displaying status of the form save state.
const SaveIndicator = (props: {
  id?: string
  state: SaveIndicatorState
  getLabel: (state: SaveIndicatorState) => string
}) => {
  const label = props.getLabel(props.state)
  const tooltip = props.state === ('isUpToDate' as const) ? 'Everything saved to cloud' : ''

  return (
    <Tooltip title={tooltip}>
      <ColoredChip
        label={label}
        skin='light'
        sx={{ mr: 2 }}
        icon={stateToIcon(props.state)}
        data-testid={stateToTestId(props.state)}
        id={props.id}
      />
    </Tooltip>
  )
}

export default SaveIndicator
