<script lang="ts">
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'

  import { Segment } from '@skeletonlabs/skeleton-svelte'
  let darkMode = useDarkMode()
</script>

<div class="flex flex-col gap-2">
  Theme
  <Segment
    bind:value={darkMode.current}
    base="flex items-stretch overflow-hidden"
    background="preset-filled-surface-50-950 h-12 w-fit"
    indicatorBg="bg-white-dark shadow "
    indicatorText=""
    border="p-1"
    rounded="rounded"
    gap="gap-2"
  >
    <Segment.Item
      value="light"
      base="btn cursor-pointer z-[1] px-10 sm:px-12 h-auto"
      labelClasses="flex items-center flex-nowrap gap-3 transition-none"
    >
      <span class="fd fd-sun text-[20px]"></span>
      Light
    </Segment.Item>
    <Segment.Item
      value="dark"
      base="btn cursor-pointer z-[1] px-10 sm:px-12 h-auto"
      labelClasses="flex items-center flex-nowrap gap-3 transition-none"
    >
      <span class="fd fd-moon text-[20px]"></span>
      Dark
    </Segment.Item>
  </Segment>
</div>
