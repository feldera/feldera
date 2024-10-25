<script lang="ts" generics="Row">
  import { VList } from 'virtua/svelte'
  import { untrack, type Snippet } from 'svelte'
  import { scale } from 'svelte/transition'
  let { items, item: renderItem }: { items: Row[]; item: Snippet<[item: Row]> } = $props()

  let ref = $state<VList<Row>>(undefined!)
  const scrollToBottom = () => {
    ref.scrollTo(Number.MAX_SAFE_INTEGER)
    // ref.scrollToIndex(items.length)
  }

  let stickToBottom = $state(true)

  $effect.root(() => {
    setTimeout(() => {
      stickToBottom = true
      scrollToBottom()
    }, 100)
  })

  let len = $derived(items)
  $effect(() => {
    len
    untrack(() => {
      if (stickToBottom) {
        scrollToBottom()
      }
    })
  })
</script>

<VList
  bind:this={ref}
  data={items}
  let:item
  on:scroll={(e) => {
    stickToBottom = Math.round(e.detail - ref.getScrollSize() + ref.getViewportSize()) === 0
  }}
  class="h-full"
  getKey={(_, i) => i}
  shift>
  {@render renderItem(item)}
</VList>
{#if !stickToBottom}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow_downward preset-filled-primary-500 absolute bottom-4 right-4 rounded-full p-2 text-[24px]"
    onclick={() => {
      stickToBottom = true
      scrollToBottom()
    }}
    aria-label="Scroll to bottom"></button>
{/if}
