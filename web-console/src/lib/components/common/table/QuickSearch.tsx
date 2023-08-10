// An input field that can be used to search the table (on the client-side).

import { ChangeEvent } from 'react'

import { Icon } from '@iconify/react'
import Box from '@mui/material/Box'
import IconButton from '@mui/material/IconButton'
import TextField from '@mui/material/TextField'

interface Props {
  value: string
  clearSearch: () => void
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const QuickSearch = (props: Props) => {
  return (
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
  )
}

export default QuickSearch
