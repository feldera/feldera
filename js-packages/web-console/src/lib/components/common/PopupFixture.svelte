<!--
  Harness for `Popup`'s bindable `open` prop. A binding needs a parent on the other end
  of it, so the test drives this component: it opens the popup from outside the trigger
  the way `SupportBundlePopup` does after a file has been chosen, and reads back what
  the popup wrote when it closed itself.
-->
<script lang="ts">
  import Popup from './Popup.svelte'

  let isOpen = $state(false)

  // Instance exports: the test owns when the popup is opened from outside, and asks
  // afterwards what the binding holds.
  export function openFromOutside() {
    isOpen = true
  }
  function _isOpen() {
    return isOpen
  }

  export { _isOpen as isOpen }
</script>

<Popup bind:isOpen>
  {#snippet trigger(toggle, isOpen)}
    <button onclick={toggle} data-testid="btn-popup-trigger">
      {isOpen ? 'Close' : 'Open'}
    </button>
  {/snippet}
  {#snippet content(close)}
    <div data-testid="box-popup-content">
      <button onclick={close} data-testid="btn-popup-close">Done</button>
    </div>
  {/snippet}
</Popup>
<button data-testid="btn-outside">Elsewhere</button>
