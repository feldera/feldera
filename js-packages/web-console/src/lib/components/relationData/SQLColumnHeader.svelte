<script lang="ts">
  import { getCaseIndependentName } from 'common-ui/felderaRelation'
  import { displaySQLColumnType } from '#lib/functions/sql.ts'
  import type { Field } from '#lib/services/manager/index.ts'

  const {
    column,
    style,
    isSticky,
    class: _class
  }: {
    column: Field
    style?: string
    isSticky?: boolean
    class?: string
  } = $props()

  // Number columns are right-aligned like their values, so each heading sits over its column.
  const numericTypes = [
    'TINYINT',
    'SMALLINT',
    'INTEGER',
    'BIGINT',
    'DECIMAL',
    'REAL',
    'DOUBLE',
    'FLOAT'
  ]
  const isNumeric = $derived(numericTypes.includes(String(column.columntype?.type ?? '')))
</script>

<!-- <th
  class="font-normal"
  style={typeof style === 'string' ? style : ''}
  style:position={typeof style === 'string' ? undefined : style?.position}
  style:top={typeof style === 'string' ? undefined : style?.top}
  style:left={typeof style === 'string' ? undefined : style?.left}
  style:z-index={typeof style === 'string' ? undefined : style?.['z-index']}
  >{getCaseIndependentName(column)}</th> -->
<th
  class="whitespace-nowrap {isSticky
    ? 'sticky top-0 z-10'
    : ''} px-3 text-sm font-medium text-surface-900-100 {isNumeric
    ? 'text-right'
    : 'text-left'} {_class}"
  {style}
>
  {getCaseIndependentName(column)}
  <span class="pl-1 text-sm font-normal text-surface-700-300">{displaySQLColumnType(column)}</span>
</th>
