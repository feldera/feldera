// A input field that can be used to search the table (on the client-side).

import { ChangeEvent } from 'react'
import Box from '@mui/material/Box'
import TextField from '@mui/material/TextField'
import IconButton from '@mui/material/IconButton'
import { GridToolbarFilterButton } from '@mui/x-data-grid-pro'
import { Icon } from '@iconify/react'

interface Props {
  hasSearch: boolean
  hasFilter: boolean
  value: string
  clearSearch: () => void
  onChange: (e: ChangeEvent) => void
}

const QuickSearchToolbar = (props: Props) => {
  return (
    <Box
      sx={{
        gap: 2,
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'space-between',
        p: theme => theme.spacing(2, 6, 4, 6)
      }}
    >
      {props.hasFilter && <GridToolbarFilterButton />}
      {props.hasSearch && (
        <TextField
          size='small'
          value={props.value}
          onChange={props.onChange}
          placeholder='Search…'
          InputProps={{
            startAdornment: (
              <Box sx={{ mr: 2, display: 'flex' }}>
                <Icon icon='bx:search' fontSize={20} />
              </Box>
            ),
            endAdornment: (
              <IconButton size='small' title='Clear' aria-label='Clear' onClick={props.clearSearch}>
                <Icon icon='bx:x' fontSize={20} />
              </IconButton>
            )
          }}
          sx={{
            width: {
              xs: 1,
              sm: 'auto'
            },
            '& .MuiInputBase-root > svg': {
              mr: 2
            }
          }}
        />
      )}
    </Box>
  )
}

export default QuickSearchToolbar
