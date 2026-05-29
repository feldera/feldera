<script lang="ts" module>
  import type { Component, Snippet as SvelteSnippet } from 'svelte'

  /**
   * Structural Snippet alias that drops the nominal '{@render ...} must be called with a Snippet'
   * brand from svelte's Snippet type. Without this, downstream packages that resolve their own
   * copy of `svelte` (different unique-symbol identities) get a type-incompatibility error when
   * passing locally-defined `{#snippet ...}` blocks into `TabSpec.label` / `tabBarEnd`.
   */
  type Snippet<T extends unknown[] = []> = (...params: T) => ReturnType<SvelteSnippet<T>>

  export type TabSpec<T extends Record<string, unknown>> = {
    id: string
    label: Snippet
    panel: Component<T>
    keepAlive: boolean
    tabBarEnd?: Snippet
  }

  /** Visual style of the tab strip:
   *  - `'tab'`     – default; underlined triggers with a sliding bottom indicator.
   *  - `'segment'` – pill-style strip driven by `SegmentedControl`. Use when the tabs
   *                  feel more like a view selector than a primary navigation. */
  export type TabLabelVariant = 'tab' | 'segment'
</script>

<script lang="ts" generics="T extends Record<string, unknown>  ">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import { SvelteSet } from 'svelte/reactivity'
  import SegmentedControl, { type SegmentedItem } from './SegmentedControl.svelte'

  let {
    tabs,
    tabProps,
    currentTab = $bindable(),
    tabContainer = defaultTabContainer,
    tabLabelVariant = 'tab',
    headerClass = ''
  }: {
    tabs: TabSpec<T>[]
    tabContainer?: typeof defaultTabContainer
    tabProps: T
    currentTab: string
    tabLabelVariant?: TabLabelVariant
    headerClass?: string
  } = $props()

  let visited = $state(new SvelteSet<string>())
  $effect.pre(() => {
    visited.add(currentTab)
  })
  const activeTabBarEnd = $derived(tabs.find((t) => t.id === currentTab)?.tabBarEnd)
  // Segment items only need a `value`; the visual label is rendered via the `segmentLabel`
  // snippet, which dispatches back to each TabSpec's own `label` snippet so a tab definition
  // stays the single source of truth regardless of the chosen variant.
  const segmentItems = $derived<SegmentedItem<string>[]>(tabs.map((t) => ({ value: t.id })))
</script>

{#snippet defaultTabContainer(tab: Snippet, hidden: boolean)}
  <div class="absolute flex h-full w-full flex-col" class:hidden>
    {@render tab()}
  </div>
{/snippet}

{#snippet segmentLabel(item: SegmentedItem<string>)}
  {@const tab = tabs.find((t) => t.id === item.value)}
  {#if tab}{@render tab.label()}{/if}
{/snippet}

{#snippet tabContent()}
  <!-- Content. `flex-1 min-h-0` claims the remaining vertical space below the header
       (without `min-h-0` flex children refuse to shrink below their content size, so
       any inner overflow would push the panel taller than the Pane). -->
  <div class="relative min-h-0 flex-1">
    {#each tabs as { id, panel: TabComponent, keepAlive }}
      {#snippet tab()}
        <TabComponent {...tabProps}></TabComponent>
      {/snippet}
      {#if keepAlive && visited.has(id)}
        {@render tabContainer(tab, currentTab !== id)}
      {:else if currentTab === id}
        {@render tabContainer(tab, false)}
      {/if}
    {/each}
  </div>
{/snippet}

{#if tabLabelVariant === 'segment'}
  <!-- Segment variant: the header is a SegmentedControl rather than a Tabs.List, so the
       skeleton `<Tabs>` machine is bypassed entirely — it only contributed ARIA roles and
       keyboard nav that the SegmentedControl already provides for its own machine. The
       content area below is rendered manually (it never relied on Tabs.Content). -->
  <div class="flex h-full flex-col space-y-0! rounded-container bg-surface-50-950 p-4">
    <div class="flex w-full min-w-0 flex-wrap items-center gap-2 mb-0 {headerClass}">
      <SegmentedControl
        value={currentTab}
        onValueChange={(v) => (currentTab = v)}
        items={segmentItems}
        label={segmentLabel}
      />
      {@render activeTabBarEnd?.()}
    </div>
    {@render tabContent()}
  </div>
{:else}
  <Tabs
    value={currentTab}
    onValueChange={(e) => (currentTab = e.value)}
    class="flex h-full flex-col space-y-0! rounded-container bg-surface-50-950 p-4"
  >
    <!-- Header. `flex-wrap` lets the active tabBarEnd contents (selectors, controls, inputs)
         drop to subsequent rows when the panel is narrowed, instead of forcing a horizontal
         minimum width on the surrounding Pane. `min-w-0` on the row + a row gap keeps the
         wrapped rows visually grouped. -->
    <Tabs.List class="flex w-full min-w-0 flex-wrap items-center gap-0 pb-0 mb-0 {headerClass}">
      {#each tabs as { id, label }}
        <Tabs.Trigger
          value={id}
          class="btn h-9 font-medium whitespace-nowrap {id === currentTab
            ? 'border-surface-950-50 '
            : 'rounded hover:bg-surface-100-900/50'}"
        >
          {@render label()}
        </Tabs.Trigger>
      {/each}
      {@render activeTabBarEnd?.()}
      <!-- Zag.js writes the active trigger's offset rect to the indicator as the CSS custom
           properties --left / --top / --width / --height, but for horizontal orientation it
           only inlines `left: var(--left)`. Without explicit width/height/top the bar falls
           to whatever static position the indicator's slot ends up at — when the header
           wraps, that's the bottom of the wrapped list, not the active trigger's row. Pin
           it to the active trigger's bottom edge so it follows the row across wrap breaks. -->
      <Tabs.Indicator
        class="h-0.5 top-[calc(var(--top)+var(--height)-2px)] w-[var(--width)] bg-surface-950-50"
      />
    </Tabs.List>
    {@render tabContent()}
  </Tabs>
{/if}
