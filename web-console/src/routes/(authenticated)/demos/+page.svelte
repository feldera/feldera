<script lang="ts">
  import { base } from '$app/paths'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import { Segment } from '@skeletonlabs/skeleton-svelte'
  import type { PageData } from './$types'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'

  let { data }: { data: PageData } = $props()
  const breadcrumbs = [
    {
      text: 'Home',
      href: `${base}/`
    },
    {
      text: 'Use cases and tutorials',
      href: `${base}/demos/`
    }
  ]
  const typeLabels = ['All', 'Use Cases', 'Tutorials', 'Examples'] as const
  const types = {
    All: undefined,
    'Use Cases': 'Use Case',
    Tutorials: 'Tutorial',
    Examples: 'Example'
  } as const
  let demosType = $state<(typeof typeLabels)[number]>('All')
</script>

<AppHeader>
  {#snippet afterStart()}
    <PipelineBreadcrumbs {breadcrumbs}></PipelineBreadcrumbs>
  {/snippet}
</AppHeader>
<div class="px-2 pb-5 md:px-8">
  <Segment
    bind:value={demosType}
    background="preset-filled-surface-50-950"
    indicatorBg="bg-white-black shadow"
    indicatorText=""
    border="p-1"
    rounded="rounded"
    gap="sm:gap-2"
  >
    {#each typeLabels as type}
      <Segment.Item value={type} base="btn cursor-pointer z-[1] sm:px-8 h-6 text-sm">
        {type}
      </Segment.Item>
    {/each}
  </Segment>
</div>
<div class="flex flex-col overflow-y-auto px-2 pb-4 scrollbar md:px-8">
  <div class="grid grid-cols-1 gap-x-6 gap-y-5 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-5">
    {#each data.demos.filter((demo) => {
      const target = types[demosType]
      return !target || demo.type === target
    }) as demo}
      <DemoTile {demo}></DemoTile>
    {/each}
  </div>
</div>
