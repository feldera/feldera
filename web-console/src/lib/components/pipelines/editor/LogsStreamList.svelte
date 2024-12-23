<script lang="ts">
  import ReverseScrollList from './ReverseScrollList.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import ANSIDecoratedText from '$lib/components/logs/ANSIDecoratedText.svelte'

  const theme = useSkeletonTheme()

  let { logs }: { logs: { rows: string[]; totalSkippedBytes: number } } = $props()
</script>

<div class="relative flex h-full flex-1 flex-col rounded sm:pt-4">
  {#if logs.totalSkippedBytes}
    <WarningBanner>
      Receiving logs faster than can be displayed. Skipping some logs to keep up, {humanSize(
        logs.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <ReverseScrollList items={logs.rows} class="bg-white-dark pl-2 scrollbar">
    {#snippet item(value)}
      <div class="whitespace-pre-wrap" style="font-family: {theme.config.monospaceFontFamily};">
        <!-- TODO: Re-enable line numbers when they get reported by backend -->
        <!-- <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
        <ANSIDecoratedText {value} />
      </div>
    {/snippet}
  </ReverseScrollList>
</div>
