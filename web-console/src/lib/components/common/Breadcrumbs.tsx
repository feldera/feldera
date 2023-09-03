import { ReactNode } from 'react'

import { FormControl, InputLabel, Select } from '@mui/material'

export const BreadcrumbSelect = (props: { label: string; value: string; children: ReactNode[] }) => {
  return (
    <FormControl>
      <InputLabel>{props.label}</InputLabel>
      <Select value={props.value} label={props.label}>
        {props.children}
      </Select>
    </FormControl>
  )
}
