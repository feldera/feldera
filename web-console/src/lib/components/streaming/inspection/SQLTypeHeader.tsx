import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { displaySQLColumnType } from '$lib/functions/sql'
import { Field } from '$lib/services/manager'

import { Stack, Typography } from '@mui/material'

export const SQLTypeHeader = ({ col }: { col: Field }) => {
  return (
    <Stack spacing={-1}>
      <Typography component={'span'}>{getCaseIndependentName(col)}</Typography>
      <Typography variant='caption' component={'span'}>
        {displaySQLColumnType(col)}
      </Typography>
    </Stack>
  )
}
