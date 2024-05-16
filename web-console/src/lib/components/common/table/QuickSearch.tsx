// An input field that can be used to search the table (on the client-side).

import { ChangeEvent } from 'react'

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
      inputProps={{
        'data-testid': 'input-quick-search'
      }}
      InputProps={{
        startAdornment: (
          <Box sx={{ mr: 2, display: 'flex' }}>
            <i className={`bx bx-search`} style={{ fontSize: 24 }} />
          </Box>
        ),
        endAdornment: (
          <IconButton size='small' title='Clear' aria-label='Clear' onClick={props.clearSearch}>
            <i className={`bx bx-x`} style={{ fontSize: 24 }} />
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
