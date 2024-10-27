<script lang="ts" generics="Row">
  import { VList } from 'virtua/svelte'
  import { type Snippet } from 'svelte'
  import { scale } from 'svelte/transition'
  let {
    items,
    item: renderItem,
    class: _class = ''
  }: { items: Row[]; item: Snippet<[item: Row]>; class?: string } = $props()

  let ref = $state<VList<Row>>(undefined!)
  const scrollToBottom = () => {
    ref.scrollTo(Number.MAX_SAFE_INTEGER)
  }

  let stickToBottom = $state(true)

  {
    // TODO: this is not needed when it is fixed that the tabs are not mounted until opened
    const len = $derived(items.length)
    $effect(() => {
      if (stickToBottom && len) {
        scrollToBottom()
      }
    })
  }
</script>

<VList
  bind:this={ref}
  data={items}
  onscroll={(scrollTop) => {
    stickToBottom = Math.round(scrollTop - ref.getScrollSize() + ref.getViewportSize()) === 0
  }}
  class={_class}
  getKey={(_, i) => i}
  children={renderItem}
  shift
></VList>
{#if !stickToBottom}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow_downward absolute bottom-4 right-4 rounded-full p-2 text-[24px] preset-filled-primary-500"
    onclick={() => {
      stickToBottom = true
      scrollToBottom()
    }}
    aria-label="Scroll to bottom"
  ></button>
{/if}
