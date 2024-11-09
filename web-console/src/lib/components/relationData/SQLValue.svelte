<script lang="ts">
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import BigNumber from 'bignumber.js'
  import { format } from 'd3-format'
  import type { HTMLTdAttributes } from 'svelte/elements'
  import JSONbig from 'true-json-bigint'

  let {
    value,
    props,
    class: _class,
    ...rest
  }: { value: SQLValueJS } & {
    props?: (formatter: (value: SQLValueJS) => string) => HTMLTdAttributes
  } & HTMLTdAttributes = $props()
  let thumb = $derived(
    value === null
      ? 'NULL'
      : typeof value === 'string'
        ? value.slice(0, 37) + (value.length >= 37 ? '...' : '')
        : BigNumber.isBigNumber(value)
          ? value.toFixed(3, BigNumber.ROUND_DOWN).replace(/\.0+$/, '')
          : JSONbig.stringify(value, undefined, 1)
  )
  let displayValue = (value: SQLValueJS) => {
    return value === null
      ? 'NULL'
      : typeof value === 'string'
        ? value
        : BigNumber.isBigNumber(value)
          ? value.toFixed()
          : JSONbig.stringify(value, undefined, 1)
  }
  //   let thumb = $derived(
  //     text === null
  //       ? 'NULL'
  //       : text.slice(0, 37).replace(/\.0+$/, '') + (text.length >= 37 ? '...' : '')
  //   )
</script>

<td
  {...props?.(displayValue)}
  class:italic={thumb === null}
  class="px-1 {typeof value === 'number' || BigNumber.isBigNumber(value)
    ? 'text-right'
    : ''} {_class}"
  {...rest}
>
  {thumb}
</td>
