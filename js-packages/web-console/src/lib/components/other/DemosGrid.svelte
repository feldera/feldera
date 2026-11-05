<script lang="ts">
  import Footer from '#lib/components/layout/Footer.svelte'
  import DemoTile from '#lib/components/other/DemoTile.svelte'
  import type { Demo } from '#lib/services/pipelineManager.ts'

  // A null entry is a placeholder tile shown while the demos load.
  let { demos }: { demos: (Demo | null)[] } = $props()
</script>

<div class="scrollbar flex h-full flex-col justify-between overflow-y-auto pt-5">
  <div class="grid grid-cols-1 gap-4 px-2 sm:grid-cols-2 md:px-8 lg:grid-cols-3 2xl:grid-cols-5">
    {#each demos as demo}
      <DemoTile {demo} placement="demos"></DemoTile>
    {/each}
  </div>
  <Footer></Footer>
</div>
