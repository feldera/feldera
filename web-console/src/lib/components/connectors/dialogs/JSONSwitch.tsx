import { Dispatch, SetStateAction } from 'react'

import { FormControlLabel, IconButton, Tooltip } from '@mui/material'
import { Box } from '@mui/system'

export const JsonSwitch = (props: {
  rawJSON: boolean
  setRawJSON: Dispatch<SetStateAction<boolean>>
  editorDirty: 'error' | 'dirty' | 'clean'
}) => (
  <Box sx={{ pl: 4 }}>
    <Tooltip title={props.editorDirty !== 'clean' ? 'Fix errors before switching the view' : undefined}>
      <FormControlLabel
        control={
          <IconButton
            onClick={() => props.setRawJSON(!props.rawJSON)}
            disabled={props.editorDirty !== 'clean'}
            data-testid='input-edit-json'
          >
            {props.rawJSON ? <i className='bx bx-check-square'></i> : <i className='bx bx-square'></i>}
          </IconButton>
        }
        label='Edit JSON'
      />
    </Tooltip>
  </Box>
)
