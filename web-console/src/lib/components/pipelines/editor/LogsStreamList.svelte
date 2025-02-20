<script lang="ts">
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { scale } from 'svelte/transition'
  import { useReverseScrollContainer } from '$lib/compositions/common/useReverseScrollContainer.svelte'
  import { selectScope, virtualSelect } from '$lib/compositions/common/userSelect'
  import { Virtualizer, type VirtualizerHandle } from 'virtua/svelte'
  import stripANSI from 'strip-ansi'
  import AnsiDecoratedText from '$lib/components/logs/ANSIDecoratedText.svelte'

  const theme = useSkeletonTheme()

  let { logs }: { logs: { rows: string[]; totalSkippedBytes: number; firstRowIndex: number } } =
    $props()

  const reverseScroll = useReverseScrollContainer()
  let virtualizer: VirtualizerHandle = $state()!
</script>

<div class="relative flex h-full flex-1 flex-col rounded">
  {#if logs.totalSkippedBytes}
    <WarningBanner>
      Receiving logs faster than can be displayed. Skipping some logs to keep up, {humanSize(
        logs.totalSkippedBytes
      )} in total.
    </WarningBanner>
  {/if}
  <!-- <ReverseScrollList items={logs.rows} class="bg-white-dark pl-2 scrollbar">
    {#snippet item(value, i)}
      <div class="whitespace-pre-wrap" style="font-family: {theme.config.monospaceFontFamily};"> -->
  <!-- TODO: Re-enable line numbers when they get reported by backend -->
  <!-- <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
  <!-- {@html value}
      </div>
    {/snippet}
  </ReverseScrollList> -->
  <div
    role="textbox"
    class="bg-white-dark h-full w-full overflow-y-auto whitespace-pre-wrap rounded pl-2 scrollbar"
    style="font-family: {theme.config.monospaceFontFamily}; user-select: contain;"
    tabindex={99}
    use:reverseScroll.action
    use:selectScope
    use:virtualSelect={{
      getRoot: (node) => node.firstElementChild!,
      getRootChildrenOffset: (root) => {
        const num = parseInt(
          root.children.item(0)!.children.item(0)!.getAttribute('data-rowindex')!
        )
        return num
      },
      getCopyContent(slice) {
        if (slice === 'all') {
          return logs.rows.map(stripANSI).join('')
        }
        const result = logs.rows.slice(slice.start.row, slice.end.row + 1).map(stripANSI)
        result[0] = result[0].slice(slice.start.col)
        result[result.length - 1] = result[result.length - 1].slice(
          0,
          slice.end.col - (slice.start.row === slice.end.row ? slice.start.col : 0)
        )
        return result.join('')
      }
    }}
  >
    <Virtualizer data={logs.rows} getKey={(_, i) => i + logs.firstRowIndex} bind:this={virtualizer}>
      {#snippet children(value, index)}
        <div data-rowindex={index}>
          <!-- <span class="select-none font-bold">{(index + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
          <AnsiDecoratedText {value}></AnsiDecoratedText>
        </div>
      {/snippet}
    </Virtualizer>
  </div>

  <!-- <SvelteVirtualList items={logs.rows} containerClass="scrollbar" viewportClass="virtual-list-viewport bg-white-dark " bufferSize={1000}>
    {#snippet renderItem(item, i)}
      <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span>
      {@html item}
    {/snippet}
  </SvelteVirtualList> -->
  <!-- <div
    role="textbox"
    use:reverseScroll.action
    use:selectScope
    class="bg-white-dark h-full w-full overflow-auto whitespace-pre-wrap pl-2 scrollbar"
    style="font-family: {theme.config.monospaceFontFamily}; user-select: contain;"
    tabindex={99}
  >
    {#each logs.rows as value, i}
    {@const x = console.log('row')}
      <div>
        {@html value}
      </div>
    {/each}
  </div> -->
  <!-- <div
    role="textbox"
    use:reverseScroll.action
    use:selectScope
    class="bg-white-dark h-full w-full overflow-auto whitespace-pre-wrap pl-2 scrollbar"
    style="font-family: {theme.config.monospaceFontFamily}; user-select: contain;"
    tabindex={99}
  >
    {@html logs.rows}
  </div> -->
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
