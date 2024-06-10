// The save indicator that is shown while editing programs or pipelines and
// indicates if the modified version has been saved or is currently being saved.

import { match } from 'ts-pattern'

import ColoredChip from '@core/components/mui/chip'
import IconCloudDone from '@mui/icons-material/CloudDone'
import IconCloudOff from '@mui/icons-material/CloudOff'
import IconSync from '@mui/icons-material/Sync'
import { Tooltip } from '@mui/material'

export type EntitySyncIndicatorStatus = 'isNew' | 'isLoading' | 'isModified' | 'isSaving' | 'isUpToDate'

// Given a save state return the icon displayed in the Chip.
const stateToIcon = (state: EntitySyncIndicatorStatus) =>
  match(state)
    .with('isSaving', 'isModified', () => <IconSync />)
    .with('isUpToDate', () => <IconCloudDone />)
    .with('isNew', () => <IconCloudOff />)
    .with('isLoading', () => <IconSync />)
    .exhaustive()

const stateToTestId = (state: EntitySyncIndicatorStatus) =>
  match(state)
    .with('isSaving', () => 'box-save-in-progress')
    .with('isModified', () => 'box-save-modified')
    .with('isUpToDate', () => 'box-save-saved')
    .with('isNew', () => 'box-save-is-new')
    .with('isLoading', () => 'box-is-loading')
    .exhaustive()

// The EntitySyncIndicator displaying status of the form save state.
export const EntitySyncIndicator = (props: {
  id?: string
  state: EntitySyncIndicatorStatus
  getLabel: (state: EntitySyncIndicatorStatus) => string
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
      />
    </Tooltip>
  )
}
