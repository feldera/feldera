<script lang="ts">
  import ReverseScrollList from './ReverseScrollList.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  const theme = useSkeletonTheme()

  let { rows }: { rows: string[] } = $props()
</script>

<div class="relative h-full bg-white pl-2 dark:bg-black">
  <ReverseScrollList items={rows}>
    {#snippet item(item)}
      <div class="whitespace-pre-wrap" style="font-family: {theme.config.monospaceFontFamily};">
        <!-- TODO: Re-enable line numbers when they get reported by backend -->
        <!-- <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
        {item}
      </div>
    {/snippet}
  </ReverseScrollList>
</div>
