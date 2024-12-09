<script lang="ts" generics="Row">
  import { VList } from 'virtua/svelte'
  import { untrack, type Snippet } from 'svelte'
  import { scale } from 'svelte/transition'
  let {
    items,
    item: renderItem,
    class: _class = ''
  }: { items: Row[]; item: Snippet<[item: Row]>; class?: string } = $props()

  let handle1: number = 0
  let handle2: number = 0
  let handle3: NodeJS.Timeout
  let ref = $state<VList<Row>>(undefined!)
  const scrollToBottom = (lastOffset?: number) => {
    if (!ref) {
      return
    }
    ref.scrollTo(
      ref.getScrollSize() -
        (Math.round(ref.getScrollSize() - ref.getViewportSize()) <= 0 ? ref.getViewportSize() : 0)
    )
    handle1 = requestAnimationFrame(() => {
      if (!ref) {
        return
      }
      const offset = Math.round(ref.getScrollSize() - ref.getScrollOffset() - ref.getViewportSize())
      if (offset === lastOffset) {
        return
      }
      stickToBottom = true
      if (offset > 1) {
        scrollToBottom(offset)
      }
    })
  }

  let stickToBottom = $state(true)

  {
    $effect(() => {
      items
      untrack(() => {
        if (Math.round(ref.getScrollSize() - ref.getScrollOffset()) <= 0) {
          handle2 = requestAnimationFrame(scrollToBottom)
        }
      })
      if (stickToBottom && items.length) {
        handle3 = setTimeout(scrollToBottom)
      }
    })
  }
</script>

<VList
  bind:this={ref}
  data={items}
  onscroll={(scrollTop) => {
    // TODO: re-enable only for vertical scroll when it can be disambiguated from horizontal scroll
    // cancelAnimationFrame(handle1)
    // cancelAnimationFrame(handle2)
    // clearTimeout(handle3)
    stickToBottom = Math.round(scrollTop - ref.getScrollSize() + ref.getViewportSize()) >= 0
  }}
  class={_class}
  getKey={(_, i) => i}
  children={renderItem}
></VList>
{#if !stickToBottom}
  <button
    transition:scale={{ duration: 200 }}
    class="fd fd-arrow-down absolute bottom-4 right-4 rounded-full p-2 text-[20px] preset-filled-primary-500"
    onclick={() => {
      stickToBottom = true
      scrollToBottom()
    }}
    aria-label="Scroll to bottom"
  ></button>
{/if}
