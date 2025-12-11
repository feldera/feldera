<script lang="ts">
  import { SegmentedControl } from '@skeletonlabs/skeleton-svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'

  const darkMode = useDarkMode()
</script>

<div class="flex flex-col gap-2">
  Theme
  <SegmentedControl
    value={darkMode.current}
    onValueChange={(e) => (darkMode.current = e.value as typeof darkMode.current)}
    class=""
  >
    <SegmentedControl.Label />
    <SegmentedControl.Control
      class="flex h-12 w-full items-stretch gap-2 overflow-hidden rounded preset-filled-surface-50-950 p-1"
    >
      <SegmentedControl.Indicator class="bg-white-dark shadow" />

      <SegmentedControl.Item value="light" class="z-1 btn h-auto cursor-pointer px-10 sm:px-12">
        <SegmentedControl.ItemText
          class="flex flex-nowrap items-center gap-3 text-surface-950-50 transition-none"
        >
          <span class="fd fd-sun text-[20px]"></span>
          Light</SegmentedControl.ItemText
        >
        <SegmentedControl.ItemHiddenInput />
      </SegmentedControl.Item>
      <SegmentedControl.Item value="dark" class="z-1 btn h-auto cursor-pointer px-10 sm:px-12">
        <SegmentedControl.ItemText
          class="flex flex-nowrap items-center gap-3 text-surface-950-50 transition-none"
        >
          <span class="fd fd-moon text-[20px]"></span>
          Dark</SegmentedControl.ItemText
        >
        <SegmentedControl.ItemHiddenInput />
      </SegmentedControl.Item>
    </SegmentedControl.Control>
  </SegmentedControl>
</div>
