import { ReactNode } from 'react'

import { Box, FormControl, InputLabel, Select } from '@mui/material'

export const BreadcrumbSelect = (props: { label: string; value: string; children: ReactNode[] }) => {
  return (
    <Box sx={{ mb: '-1rem' }}>
      <FormControl sx={{ mt: '-1rem' }}>
        <InputLabel>{props.label}</InputLabel>
        <Select size='small' value={props.value} label={props.label}>
          {props.children}
        </Select>
      </FormControl>
    </Box>
  )
}
