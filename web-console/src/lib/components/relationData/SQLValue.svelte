<script lang="ts">
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import type { HTMLTdAttributes } from 'svelte/elements'
  import JSONbig from 'true-json-bigint'

  let {
    value,
    props,
    class: _class,
    ...rest
  }: { value: SQLValueJS } & {
    props?: (text: string | null) => HTMLTdAttributes
  } & HTMLTdAttributes = $props()
  let text = $derived(
    value === null
      ? null
      : typeof value === 'string'
        ? value
        : JSONbig.stringify(value, undefined, 1)
  )
</script>

<td {...props?.(text)} class:italic={text === null} class="px-1 {_class}" {...rest}>
  {text === null ? 'NULL' : text.slice(0, 37)}{text && text.length > 36 ? '...' : ''}
</td>
