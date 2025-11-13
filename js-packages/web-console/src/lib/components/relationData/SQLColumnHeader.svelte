<script lang="ts">
  import { getCaseIndependentName } from '$lib/functions/felderaRelation'
  import { displaySQLColumnType } from '$lib/functions/sql'
  import type { Field } from '$lib/services/manager'

  let {
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
  class="whitespace-nowrap {isSticky ? 'sticky top-0 z-10' : ''} px-3 font-normal {_class}"
  {style}
>
  {getCaseIndependentName(column)}
  <span class="text-sm text-surface-600-400">{displaySQLColumnType(column)}</span>
</th>
