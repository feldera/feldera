<script lang="ts">
  import { SegmentedControl, type SegmentedItem } from 'common-ui'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'

  let { class: className = '' }: { class?: string } = $props()

  const darkMode = useDarkMode()

  type Mode = 'light' | 'dark'
  const items: SegmentedItem<Mode>[] = [
    { value: 'light', label: 'Light' },
    { value: 'dark', label: 'Dark' }
  ]
</script>

<div class="flex flex-col gap-2 {className}">
  Theme
  <SegmentedControl
    value={darkMode.current as Mode}
    onValueChange={(v) => (darkMode.current = v)}
    {items}
    itemTextClass="flex flex-nowrap items-center gap-3"
  >
    {#snippet label(item)}
      <span class="fd fd-{item.value === 'light' ? 'sun' : 'moon'} text-[16px]"></span>
      {item.label}
    {/snippet}
  </SegmentedControl>
</div>
