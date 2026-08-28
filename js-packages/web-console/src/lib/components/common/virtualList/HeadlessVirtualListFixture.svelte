<!--
  Harness for `HeadlessVirtualList`.

  The list has no scroll container of its own — the host supplies one through `listContainer`, the
  way `ChangeStream` does — so a spec cannot mount it bare.
-->
<script lang="ts">
  import HeadlessVirtualList from './HeadlessVirtualList.svelte'

  let {
    itemCount = 500,
    itemSize = 28,
    viewportHeight = 400
  }: { itemCount?: number; itemSize?: number; viewportHeight?: number } = $props()

  let container: HTMLDivElement | undefined = $state()

  export function getContainer() {
    return container!
  }
</script>

<HeadlessVirtualList {itemCount} {itemSize}>
  {#snippet listContainer(children, { height, onscroll, setClientHeight })}
    {@const _ = {
      set clientHeight(value: number) {
        setClientHeight(value)
      }
    }}
    <div
      bind:this={container}
      data-testid="virtual-list-viewport"
      style="height: {viewportHeight}px; overflow-y: auto;"
      {onscroll}
      bind:clientHeight={_.clientHeight}
    >
      <div style:height>
        {@render children()}
      </div>
    </div>
  {/snippet}
  {#snippet item({ index, style })}
    <div data-index={index} style="{style} height: {itemSize}px;">row {index}</div>
  {/snippet}
  {#snippet emptyItem()}
    <div style="display: none;"></div>
  {/snippet}
</HeadlessVirtualList>
