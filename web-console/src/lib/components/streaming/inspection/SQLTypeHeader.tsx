import { nonNull } from '$lib/functions/common/function'
import { Field } from '$lib/services/manager'

import { Typography } from '@mui/material'

export const SQLTypeHeader = ({ col }: { col: Field }) => {
  return (
    <span>
      <Typography component={'span'}>{col.name}</Typography>
      <Typography variant='subtitle2' component={'span'} sx={{ pl: 2 }}>
        {col.columntype.type}
        {((p, s) => (p && p > 0 ? '(' + [p, ...(nonNull(s) ? [s] : [])].join(',') + ')' : ''))(
          col.columntype.precision,
          col.columntype.scale
        )}
      </Typography>
    </span>
  )
}
