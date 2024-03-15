import { nonNull } from '$lib/functions/common/function'
import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { Field } from '$lib/services/manager'

import { Box, Stack, Typography } from '@mui/material'

export const SQLTypeHeader = ({ col }: { col: Field }) => {
  return (
    <Stack spacing={-1}>
      <Typography component={'span'}>{getCaseIndependentName(col)}</Typography>
      <Typography variant='caption' component={'span'}>
        {col.columntype.type}
        {((p, s) => (p && p > 0 ? '(' + [p, ...(nonNull(s) ? [s] : [])].join(',') + ')' : ''))(
          col.columntype.precision,
          col.columntype.scale
        )}
      </Typography>
    </Stack>
  )
}
