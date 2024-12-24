<script lang="ts" generics="Row">
  import List, {
    type ListContainer
  } from '$lib/components/common/virtualList/HeadlessVirtualList.svelte'
  import { untrack, type Snippet } from 'svelte'
  import { scale } from 'svelte/transition'
  let {
    items,
    itemSize,
    item: renderItem,
    emptyItem,
    listContainer,
    stickyIndices,
    class: _class = '',
    header,
    footer,
    marginTop
  }: {
    items: Row[]
    item: Snippet<[item: Row, style?: string, padding?: string, isSticky?: boolean]>
    emptyItem: Snippet
    listContainer?: Snippet<[Snippet, ListContainer, Snippet | undefined]>
    header?: Snippet
    footer?: Snippet
    itemSize: number
    stickyIndices?: number[]
    class?: string
    marginTop?: number
  } = $props()

  let ref = $state<ReturnType<typeof List>>(undefined!)

  const scrollToBottom = (lastOffset?: number) => {
    ref.scrollToBottom()
  }

  let stickToBottom = $state(true)

  let len = $derived(items.length)
  $effect(() => {
    len
    untrack(() => {
      if (stickToBottom) {
        scrollToBottom()
      }
    })
  })
</script>

<List
  bind:this={ref}
  {header}
  {footer}
  onscroll={(e) => {
    stickToBottom =
      Math.round(
        e?.currentTarget.scrollTop - e?.currentTarget.scrollHeight + e?.currentTarget.clientHeight
      ) >= -1
  }}
  itemCount={items.length}
  {listContainer}
  {itemSize}
  {stickyIndices}
  {marginTop}
  {emptyItem}
>
  {#snippet item({ index, style, padding, isSticky })}
    {#if items[index]}
      {@render renderItem(items[index], style, padding, isSticky)}
    {/if}
  {/snippet}
</List>

{#if !stickToBottom}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow-down absolute bottom-4 right-4 z-20 rounded-full p-2 text-[20px] preset-filled-primary-500"
    onclick={() => {
      stickToBottom = true
      scrollToBottom()
    }}
    aria-label="Scroll to bottom"
  ></button>
{/if}
