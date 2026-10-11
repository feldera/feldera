<script lang="ts">
  import { Combobox, Portal, useListCollection } from '@skeletonlabs/skeleton-svelte'
  import type { MetricOption } from 'profiler-lib'

  /**
   * Selects the metric that colors the diagram. The selector is at the right end of the tab bar,
   * and the metric names are wider than the input, so the list lines up with the right edge of
   * the input and grows to the left. Type in the input to filter the list.
   */
  let {
    metrics,
    value = $bindable(),
    class: className = '',
    title
  }: { metrics: MetricOption[]; value: string; class?: string; title?: string } = $props()

  /** The text that the user typed to filter the list. Empty shows all metrics. */
  let filter = $state('')
  const matching = $derived.by(() => {
    const needle = filter.trim().toLowerCase()
    return needle === ''
      ? metrics
      : metrics.filter((metric) => metric.label.toLowerCase().includes(needle))
  })
  const collection = $derived(
    useListCollection({
      items: matching,
      itemToValue: (metric) => metric.id,
      itemToString: (metric) => metric.label
    })
  )
</script>

<Combobox
  class={className}
  {collection}
  value={value === '' ? [] : [value]}
  onValueChange={(e) => {
    if (e.value[0] !== undefined) {
      value = e.value[0]
    }
  }}
  onInputValueChange={(e) => {
    // Only typed text filters. When the input shows the label of the selected metric, show all.
    filter = e.reason === 'input-change' ? e.inputValue : ''
  }}
  openOnClick
  positioning={{ placement: 'bottom-end', sameWidth: false }}
>
  <Combobox.Control>
    <Combobox.Input
      class="h-6 border-none bg-surface-100-900/50 py-0 pr-7 pl-2 text-[14px] ring-0"
      {title}
    />
    <Combobox.Trigger class="right-0.5 size-5 min-h-0 min-w-0 bg-transparent p-0">
      <span class="fd fd-chevron-down text-[16px] text-surface-600-400"></span>
    </Combobox.Trigger>
  </Combobox.Control>
  <Portal>
    <Combobox.Positioner>
      <!-- The positioner copies its z-index from the content (`z-2`). -->
      <Combobox.Content class="scrollbar z-2 max-h-96 gap-0 overflow-y-auto p-1 text-[14px]">
        {#each matching as metric (metric.id)}
          <Combobox.Item item={metric} class="py-0.5">
            <Combobox.ItemText>{metric.label}</Combobox.ItemText>
          </Combobox.Item>
        {:else}
          <li class="px-2 py-0.5 text-surface-600-400">No matching metrics</li>
        {/each}
      </Combobox.Content>
    </Combobox.Positioner>
  </Portal>
</Combobox>
