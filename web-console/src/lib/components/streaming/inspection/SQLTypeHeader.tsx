import { nonNull } from '$lib/functions/common/function'
import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { ColumnType, Field } from '$lib/services/manager'

import { Stack, Typography } from '@mui/material'

const displayType = (columntype: ColumnType): string =>
  (columntype.component ? displayType(columntype.component) + ' ' : '') +
  columntype.type +
  ((p, s) => (p && p > 0 ? '(' + [p, ...(nonNull(s) ? [s] : [])].join(',') + ')' : ''))(
    columntype.precision,
    columntype.scale
  )

export const SQLTypeHeader = ({ col }: { col: Field }) => {
  return (
    <Stack spacing={-1}>
      <Typography component={'span'}>{getCaseIndependentName(col)}</Typography>
      <Typography variant='caption' component={'span'}>
        {displayType(col.columntype)}
      </Typography>
    </Stack>
  )
}
