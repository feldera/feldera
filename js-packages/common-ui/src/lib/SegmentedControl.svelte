<!--
  A thin, app-themed wrapper around Skeleton's `SegmentedControl` — a row of
  mutually-exclusive buttons used to pick one value from a small set (a styled
  alternative to a `<select>` or radio group). Used e.g. for the Overview / Node
  / Top-nodes metrics switch and the Light / Dark theme switch.

  Generic over the value type `V` (a string union) so `value`/`onValueChange`
  stay type-safe. Each entry is a `SegmentedItem`; pass an optional `label`
  snippet to render richer item content (e.g. an icon beside the text).
-->
<script lang="ts" module>
  import type { Snippet as SvelteSnippet } from 'svelte'

  type Snippet<T extends unknown[] = []> = (...params: T) => ReturnType<SvelteSnippet<T>>

  export type SegmentedItem<V extends string = string> = {
    value: V
    label?: string
    disabled?: boolean
    testid?: string
  }
</script>

<script lang="ts" generics="V extends string">
  import { SegmentedControl as SC } from '@skeletonlabs/skeleton-svelte'

  let {
    value,
    onValueChange,
    items,
    label,
    class: className = '',
    itemTextClass = ''
  }: {
    value: V
    onValueChange: (value: V) => void
    items: SegmentedItem<V>[]
    label?: Snippet<[SegmentedItem<V>]>
    class?: string
    itemTextClass?: string
  } = $props()
</script>

<SC
  {value}
  onValueChange={(e) => {
    if (e.value) onValueChange(e.value as V)
  }}
  class={className}
>
  <SC.Control class="w-fit flex-none rounded bg-surface-100-900/50 border-none p-0.5">
    <SC.Indicator class="bg-white-dark shadow" />
    {#each items as item}
      <SC.Item
        value={item.value}
        disabled={item.disabled}
        data-testid={item.testid}
        class="z-1 btn h-5 cursor-pointer px-5 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40"
      >
        <SC.ItemText class="text-surface-950-50 {itemTextClass}">
          {#if label}{@render label(item)}{:else}{item.label ?? item.value}{/if}
        </SC.ItemText>
        <SC.ItemHiddenInput />
      </SC.Item>
    {/each}
  </SC.Control>
</SC>
