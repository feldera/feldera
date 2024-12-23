<script lang="ts">
  import type { SQLValueJS } from '$lib/functions/sqlValue'
  import { BigNumber } from 'bignumber.js/bignumber.js'
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
          ? value.toFixed(3, BigNumber.ROUND_DOWN).replace(/\.?0+$/, '')
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
</script>

<td
  {...props?.(displayValue)}
  class:italic={value === null}
  class="px-3 {typeof value === 'number' || BigNumber.isBigNumber(value)
    ? 'text-right'
    : ''} {_class}"
  {...rest}
>
  {thumb}
</td>
