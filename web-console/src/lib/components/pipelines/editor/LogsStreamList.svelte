<script lang="ts">
  import { scale } from 'svelte/transition'

  let { rows }: { rows: string[] } = $props()

  let ref = $state<HTMLElement>(undefined!)

  const invertScrollY = (node: HTMLElement) => {
    node.addEventListener(
      'wheel',
      function (e) {
        if (e.deltaY) {
          e.preventDefault()
          ;(e.currentTarget as any).scrollTop -= e.deltaY
        }
      },
      { passive: false }
    )
  }
  let x = $state(0)
</script>

<div class="relative h-full bg-white dark:bg-black">
  <div
    onscroll={(e) => (x = e.currentTarget.scrollTop)}
    class="h-full -scale-y-100 gap-4 overflow-y-auto p-2 font-mono"
    use:invertScrollY
    bind:this={ref}
  >
    <div class="mb-auto flex min-h-full -scale-y-100 flex-col">
      {#each rows as item}
        <div class="whitespace-pre-wrap">
          <!-- TODO: Re-enable line numbers when they get reported by backend -->
          <!-- <span class="select-none font-bold">{(i + 1).toFixed().padStart(5, ' ')}&nbsp;&nbsp;</span> -->
          {item}
        </div>
      {:else}
        <div class="text-surface-500">No logs emitted</div>
      {/each}
    </div>
  </div>
  {#if x !== 0}
    <button
      transition:scale={{ duration: 200 }}
      class="fd fd-arrow_downward absolute bottom-4 right-4 rounded-full p-2 text-[24px] preset-filled-primary-500"
      onclick={(e) => {
        ref.scrollTop = 0
      }}
    ></button>
  {/if}
</div>
