<!--
  Harness for common-ui's `useReverseScrollContainer`: a scroll container whose content height the
  test sets directly. A virtualiser would keep resizing the content on its own as it measures rows,
  which is exactly what makes the timing under test hard to pin down; a single div resizes only when
  the test says so.
-->
<script lang="ts">
  import { useReverseScrollContainer } from 'common-ui'

  let contentHeight = $state(1000)

  const reverseScroll = useReverseScrollContainer({
    observeContentElement: (node) => node.firstElementChild!
  })

  // Instance exports: the test drives the resize and the jump away from the bottom itself, so it
  // owns the order of the two.
  export const scroll = reverseScroll
  export function setContentHeight(px: number) {
    contentHeight = px
  }
</script>

<div
  data-testid="reverse-scroll-container"
  style="height: 200px; overflow-y: auto;"
  use:reverseScroll.action
>
  <div style="height: {contentHeight}px;"></div>
</div>
