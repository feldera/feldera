<script lang="ts">
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { scale } from 'svelte/transition'
  import { useReverseScrollContainer } from '$lib/compositions/common/useReverseScrollContainer.svelte'
  import '$lib/components/logs/styles.css'
  import { selectScope } from '$lib/compositions/common/selectScope'

  const theme = useSkeletonTheme()

  let { logs }: { logs: { rows: string[]; totalSkippedBytes: number } } = $props()

  const reverseScroll = useReverseScrollContainer()
</script>

<div class="relative flex h-full flex-1 flex-col rounded sm:pt-4">
  {#if logs.totalSkippedBytes}
    <WarningBanner>
      Receiving logs faster than can be displayed. Skipping some logs to keep up, {humanSize(
        logs.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <!-- <ReverseScrollList items={logs.rows} class="bg-white-dark pl-2 scrollbar">
    {#snippet item(value)}
      <div class="whitespace-pre-wrap" style="font-family: {theme.config.monospaceFontFamily};"> -->
  <!-- TODO: Re-enable line numbers when they get reported by backend -->
  <!-- <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
  <!-- <ANSIDecoratedText {value} />
      </div>
    {/snippet}
  </ReverseScrollList> -->
  <div
    role="textbox"
    use:reverseScroll.action
    use:selectScope
    class="bg-white-dark h-full w-full overflow-auto whitespace-pre-wrap pl-2 scrollbar"
    style="font-family: {theme.config.monospaceFontFamily}; user-select: contain;"
    tabindex={99}
  >
    {#each logs.rows as value}
      <div>
        {@html value}
      </div>
    {/each}
  </div>
  {#if !reverseScroll.stickToBottom}
    <button
      transition:scale={{ duration: 200 }}
      class="fd fd-arrow-down absolute bottom-4 right-4 rounded-full p-2 text-[20px] preset-filled-primary-500"
      onclick={() => {
        reverseScroll.stickToBottom = true
        reverseScroll.scrollToBottom()
      }}
      aria-label="Scroll to bottom"
    ></button>
  {/if}
</div>
