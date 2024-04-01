import { nonNull } from '$lib/functions/common/function'
import { ColumnType } from '$lib/services/manager'

const displaySQLType = (columntype: ColumnType): string =>
  (columntype.component ? displaySQLType(columntype.component) + ' ' : '') +
  columntype.type +
  ((p, s) => (p && p > 0 ? '(' + (nonNull(s) ? [p, s] : [p]).join(', ') + ')' : ''))(
    columntype.precision,
    columntype.scale
  )

export const displaySQLColumnType = ({ columntype }: { columntype: ColumnType }) =>
  displaySQLType(columntype) + (columntype.nullable ? '' : ' NOT NULL')
