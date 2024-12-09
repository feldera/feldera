<script lang="ts">
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
</script>

<Popup>
  {#snippet trigger(onclick)}
    <button
      {onclick}
      class="fd fd-menu btn-icon flex text-[24px] preset-tonal-surface"
      aria-label="Open extras popup"
    >
    </button>
  {/snippet}
  {#snippet content()}
    <div
      transition:fade={{ duration: 100 }}
      class="bg-white-black absolute right-0 z-30 flex max-h-[400px] w-[calc(100vw-100px)] max-w-[260px] flex-col rounded p-2 shadow-md"
    >
      <NavigationExtras inline></NavigationExtras>
    </div>
  {/snippet}
</Popup>
