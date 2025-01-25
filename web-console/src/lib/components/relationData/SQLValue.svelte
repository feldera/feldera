<script lang="ts">
  import type { SQLValueJS } from '$lib/types/sql.ts'
  import { BigNumber } from 'bignumber.js/bignumber.js'
  import type { HTMLTdAttributes } from 'svelte/elements'
  import JSONbig from 'true-json-bigint'

  const trim = (str: string) => str.slice(0, 50) + (str.length >= 50 ? '...' : '')

  /**
   * Get preview of a value by stringifying until a certain result string length is reached
   */
  const stringifyUntilLength = (value: any, minLength: number) => {
    if (typeof value === 'string') {
      return trim(value)
    }
    if (BigNumber.isBigNumber(value)) {
      return value.toFixed(3, BigNumber.ROUND_DOWN).replace(/\.?0+$/, '')
    }
    if (!Array.isArray(value)) {
      return JSONbig.stringify(value, undefined, 1)
    }
    let str = [] as string[]
    let i = 0
    let chunksLength = 0
    while (chunksLength < minLength && i < value.length) {
      const tmp = stringifyUntilLength(value[i], minLength - str.length)
      str.push(tmp)
      chunksLength += tmp.length
      ++i
    }
    return `[${str.join(',\n  ')}${i < value.length ? '' : ']'}`
  }

  let {
    value,
    props,
    class: _class,
    ...rest
  }: { value: SQLValueJS } & {
    props?: (formatter: (value: SQLValueJS) => string) => HTMLTdAttributes
  } & HTMLTdAttributes = $props()
  let thumb = $derived(value === null ? 'NULL' : trim(stringifyUntilLength(value, 50)))
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
