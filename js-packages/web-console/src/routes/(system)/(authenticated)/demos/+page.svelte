<script lang="ts">
  import { SegmentedControl } from '@skeletonlabs/skeleton-svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import Footer from '$lib/components/layout/Footer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { nubLast } from '$lib/functions/common/array'
  import { resolve } from '$lib/functions/svelte'
  import type { PageData } from './$types'

  const { data }: { data: PageData } = $props()
  let demosType = $state('All')
  const drawer = useAdaptiveDrawer('right')
  const breadcrumbs = $derived([
    ...(drawer.isMobileDrawer
      ? []
      : [
          {
            text: 'Home',
            href: resolve(`/`)
          }
        ]),
    {
      text: 'Use cases and tutorials',
      href: resolve(`/demos/`)
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
        class="fd fd-book-open btn-icon flex preset-tonal-surface text-[20px]"
        aria-label="Open extras drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <div class="relative">
        <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
        ></CreatePipelineButton>
      </div>
      <BookADemo class="btn preset-filled-primary-500">Book a demo</BookADemo>
    {/if}
  {/snippet}
</AppHeader>
<div class="px-2 pb-5 md:px-8">
  <SegmentedControl value={demosType} onValueChange={(e) => e.value && (demosType = e.value)}>
    <SegmentedControl.Label />
    <SegmentedControl.Control class="rounded preset-filled-surface-50-950 p-1 sm:gap-2">
      <SegmentedControl.Indicator class="bg-white-dark shadow" />
      {#each types as type}
        <SegmentedControl.Item value={type} class="z-1 btn h-6 cursor-pointer px-5 text-sm sm:px-8">
          <SegmentedControl.ItemText class="text-surface-950-50">
            {plurals[type] ?? type}
          </SegmentedControl.ItemText>
          <SegmentedControl.ItemHiddenInput />
        </SegmentedControl.Item>
      {/each}
    </SegmentedControl.Control>
  </SegmentedControl>
</div>
<div class="scrollbar flex h-full flex-col justify-between overflow-y-auto">
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
