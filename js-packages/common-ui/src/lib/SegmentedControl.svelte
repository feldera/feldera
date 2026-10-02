<!--
  A thin, app-themed wrapper around Skeleton's `SegmentedControl` — a row of
  mutually-exclusive buttons used to pick one value from a small set (a styled
  alternative to a `<select>` or radio group). Used e.g. for the Overview / Node
  / Top-nodes metrics switch and the Light / Dark theme switch.

  Generic over the value type `V` (a string union) so `value`/`onValueChange`
  stay type-safe. Each entry is a `SegmentedItem`; pass an optional `label`
  snippet to render richer item content (e.g. an icon beside the text).

  Styled after the Figma "Segmented Control" (surface variant): a light grey track,
  the selected item raised in white with a hairline border and medium text, and a
  thin divider between two neighbouring unselected items. `size` picks the 24px,
  32px (default) or 40px height.
-->
<script lang="ts" module>
  import type { Snippet as SvelteSnippet } from 'svelte'

  type Snippet<T extends unknown[] = []> = (...params: T) => ReturnType<SvelteSnippet<T>>

  /**
   * Properties of every label (option) in the segment indicator
   */
  export type SegmentedItem<V extends string = string> = {
    value: V
    label?: string
    disabled?: boolean
    testid?: string
  }

  export type SegmentedControlSize = 'sm' | 'md' | 'lg'

  // Per size: the track height, the corner radius shared by the track, the items and the
  // selection, and each item's padding, gap and text.
  const sizeClasses: Record<
    SegmentedControlSize,
    { control: string; radius: string; item: string }
  > = {
    sm: { control: 'h-6', radius: 'rounded-[4px]', item: 'gap-1 px-3 text-[12px] leading-4' },
    md: { control: 'h-8', radius: 'rounded-[4px]', item: 'gap-2 px-4 text-[14px] leading-5' },
    lg: { control: 'h-10', radius: 'rounded-[6px]', item: 'gap-2 px-4 text-[16px] leading-6' }
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
    itemTextClass = '',
    size = 'md'
  }: {
    value: V
    onValueChange: (value: V) => void
    items: SegmentedItem<V>[]
    label?: Snippet<[SegmentedItem<V>]>
    class?: string
    itemTextClass?: string
    size?: SegmentedControlSize
  } = $props()

  const sized = $derived(sizeClasses[size])

  /** A divider goes between two neighbouring items when neither is selected. */
  const hasDividerBefore = (index: number) =>
    index > 0 && items[index].value !== value && items[index - 1].value !== value
</script>

<SC
  {value}
  onValueChange={(e) => {
    if (e.value) onValueChange(e.value as V)
  }}
  class={className}
>
  <SC.Control
    class="w-fit flex-none border-none bg-surface-950-50/[0.06] p-0 {sized.control} {sized.radius}"
  >
    <SC.Indicator class="border border-surface-950-50/10 bg-white-dark {sized.radius}" />
    {#each items as item, index}
      <SC.Item
        value={item.value}
        disabled={item.disabled}
        data-testid={item.testid}
        class="relative z-1 inline-flex h-full cursor-pointer items-center justify-center font-normal whitespace-nowrap data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40 data-[state=checked]:font-medium {sized.item} {sized.radius}"
      >
        {#if hasDividerBefore(index)}
          <span class="absolute inset-y-[3px] left-0 w-px bg-surface-950-50/10"></span>
        {/if}
        <SC.ItemText class="text-surface-950-50 {itemTextClass}">
          {#if label}{@render label(item)}{:else}{item.label ?? item.value}{/if}
        </SC.ItemText>
        <SC.ItemHiddenInput />
      </SC.Item>
    {/each}
  </SC.Control>
</SC>
