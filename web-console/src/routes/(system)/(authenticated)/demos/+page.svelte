<script lang="ts">
  import { base } from '$app/paths'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import { Segment } from '@skeletonlabs/skeleton-svelte'
  import type { PageData } from './$types'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import Footer from '$lib/components/layout/Footer.svelte'
  import { nubLast } from '$lib/functions/common/array'

  let { data }: { data: PageData } = $props()
  let demosType = $state('All')
  const drawer = useDrawer('right')
  const breadcrumbs = $derived([
    ...(drawer.isMobileDrawer
      ? []
      : [
          {
            text: 'Home',
            href: `${base}/`
          }
        ]),
    {
      text: 'Use cases and tutorials',
      href: `${base}/demos/`
    }
  ])
  const types = $derived(['All', ...nubLast(data.demos.map((d) => d.type))])
  const plurals: Record<string, string> = {
    'Use Case': 'Use Cases',
    Tutorial: 'Tutorials',
    Example: 'Examples'
  }
</script>

<AppHeader>
  {#snippet afterStart()}
    <PipelineBreadcrumbs {breadcrumbs}></PipelineBreadcrumbs>
  {/snippet}
  {#snippet beforeEnd()}
    {#if drawer.isMobileDrawer}
      <button
        onclick={() => (drawer.value = !drawer.value)}
        class="fd fd-book-open btn-icon flex text-[20px] preset-tonal-surface"
        aria-label="Open extras drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <div class="relative">
        <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
        ></CreatePipelineButton>
      </div>
      <BookADemo class="preset-filled-primary-500">Book a demo</BookADemo>
    {/if}
  {/snippet}
</AppHeader>
<div class="px-2 pb-5 md:px-8">
  <Segment
    bind:value={demosType}
    background="preset-filled-surface-50-950"
    indicatorBg="bg-white-dark shadow"
    indicatorText=""
    border="p-1"
    rounded="rounded"
    gap="sm:gap-2"
  >
    {#each types as type}
      <Segment.Item value={type} base="btn cursor-pointer z-[1] px-5 sm:px-8 h-6 text-sm">
        {plurals[type] ?? type}
      </Segment.Item>
    {/each}
  </Segment>
</div>
<div class="flex h-full flex-col justify-between overflow-y-auto scrollbar">
  <div
    class="grid grid-cols-1 gap-x-6 gap-y-5 px-2 sm:grid-cols-2 md:px-8 lg:grid-cols-3 2xl:grid-cols-5"
  >
    {#each data.demos.filter((demo) => {
      return demosType === 'All' || demo.type === demosType
    }) as demo}
      <DemoTile {demo}></DemoTile>
    {/each}
  </div>
  <Footer></Footer>
</div>
