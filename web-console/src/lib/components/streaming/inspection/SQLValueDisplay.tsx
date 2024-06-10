import { getValueFormatter, SQLValueJS } from '$lib/functions/sqlValue'
import { ColumnType } from '$lib/services/manager'
import { useMemo } from 'react'

import { Typography } from '@mui/material'

export const SQLValueDisplay = ({ value, type }: { value: SQLValueJS; type: ColumnType }) => {
  const formatter = useMemo(() => getValueFormatter(type), [type])
  const text = formatter(value)
  if (text === null) {
    return <Typography sx={{ fontStyle: 'italic', fontFamily: 'monospace' }}>null</Typography>
  }
  return <>{text}</>
}
