<script lang="ts">
  import { nubLast } from 'common-lib/array'
  import { TabsPanel } from 'common-ui'
  import { createRawSnippet } from 'svelte'
  import AppHeader from '#lib/components/layout/AppHeader.svelte'
  import NavigationExtras from '#lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '#lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '#lib/components/other/BookADemo.svelte'
  import DemosGrid from '#lib/components/other/DemosGrid.svelte'
  import CreatePipelineButton from '#lib/components/pipelines/CreatePipelineButton.svelte'
  import { useAdaptiveDrawer } from '#lib/compositions/layout/useAdaptiveDrawer.svelte.ts'
  import { useDemos } from '#lib/compositions/useDemos.svelte.ts'
  import { resolve } from '#lib/functions/svelte.ts'

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
  const demos = useDemos()
  const types = $derived(['All', ...nubLast(demos.current.flatMap((d) => (d ? [d.type] : [])))])
  const plurals: Record<string, string> = {
    'Use Case': 'Use Cases',
    Tutorial: 'Tutorials',
    Example: 'Examples'
  }
  // Set the label as text, so a demo type from the server cannot inject markup.
  const textSnippet = (text: string) =>
    createRawSnippet(() => ({
      render: () => '<span></span>',
      setup: (span) => {
        span.textContent = text
      }
    }))
  // Every tab shows the same grid, filtered by the current tab.
  const tabs = $derived(
    types.map((type) => ({
      id: type,
      label: textSnippet(plurals[type] ?? type),
      panel: DemosGrid,
      keepAlive: false
    }))
  )
  const filteredDemos = $derived(
    demos.current.filter((demo) => !demo || demosType === 'All' || demo.type === demosType)
  )
</script>

<AppHeader>
  {#snippet afterStart()}
    <PipelineBreadcrumbs {breadcrumbs}></PipelineBreadcrumbs>
  {/snippet}
  {#snippet beforeEnd()}
    {#if drawer.isMobileDrawer}
      <button
        onclick={() => (drawer.value = !drawer.value)}
        class="fd fd-book-open btn-icon flex preset-tonal-surface text-[16px]"
        aria-label="Open the right navigation drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <div class="relative">
        <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
        ></CreatePipelineButton>
      </div>
      <BookADemo class="btn preset-filled-primary-500" placement="demos">Book a demo</BookADemo>
    {/if}
  {/snippet}
</AppHeader>
<TabsPanel
  {tabs}
  bind:currentTab={demosType}
  tabProps={{ demos: filteredDemos }}
  tabLabelVariant="underline"
  framed={false}
  headerClass="md:px-8"
/>
