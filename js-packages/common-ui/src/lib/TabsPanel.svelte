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
    /** Hover text of the tab trigger */
    title?: string
    panel: Component<T>
    keepAlive: boolean
    tabBarEnd?: Snippet
    /** When true, the trigger is rendered but cannot be selected. Use for tabs whose data the
     *  loaded profile does not carry. */
    disabled?: boolean
  }

  /** Visual style of the tab strip:
   *  - `'tab'`     – default; underlined triggers with a sliding bottom indicator.
   *  - `'segment'` – pill-style strip driven by `SegmentedControl`. Use when the tabs
   *                  feel more like a view selector than a primary navigation.
   *  - `'underline'` – grey labels with no hover fill; the selected tab is in the main
   *                  text colour with a 2px primary (black) bar on the header's line.
   *  - `'file'`    – the file tabs of a code editor, see `fileTabClass`. */
  export type TabLabelVariant = 'tab' | 'segment' | 'underline' | 'file'

  /** Classes of a file tab: a 40px cell of a grey `bg-toolbar` tab bar with a 1px bottom line.
   *  The current tab is white and 1px taller, so it covers the line and joins the content below.
   *  Used by the `'file'` variant. The pipeline code editor can use it for its own file tabs. */
  export const fileTabClass = (isCurrent: boolean) =>
    `flex h-10 items-center gap-2 rounded-none border-r border-line px-4 text-base font-normal whitespace-nowrap hover:filter-none ${
      isCurrent
        ? 'bg-white-dark -mb-px h-[41px] text-surface-950-50'
        : 'text-surface-600-400 hover:bg-transparent hover:text-surface-950-50'
    }`
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
    headerClass = '',
    framed = true
  }: {
    tabs: TabSpec<T>[]
    tabContainer?: typeof defaultTabContainer
    tabProps: T
    currentTab: string
    tabLabelVariant?: TabLabelVariant
    headerClass?: string
    /** Whether the root has the `panel` frame and its padding. Set it to `false` when the tabs
     *  fill a whole page. */
    framed?: boolean
  } = $props()

  let visited = $state(new SvelteSet<string>())
  $effect.pre(() => {
    visited.add(currentTab)
  })
  const activeTabBarEnd = $derived(tabs.find((t) => t.id === currentTab)?.tabBarEnd)
  const isUnderline = $derived(tabLabelVariant === 'underline')
  const isFile = $derived(tabLabelVariant === 'file')
  // Segment items only need a `value`; the visual label is rendered via the `segmentLabel`
  // snippet, which dispatches back to each TabSpec's own `label` snippet so a tab definition
  // stays the single source of truth regardless of the chosen variant.
  const segmentItems = $derived<SegmentedItem<string>[]>(
    tabs.map((t) => ({ value: t.id, title: t.title, disabled: t.disabled }))
  )
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
    {#each tabs as { id, panel: TabComponent, keepAlive } (id)}
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
  <div class="flex h-full flex-col space-y-0! {framed ? 'panel' : ''}">
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
    class="flex h-full flex-col space-y-0! {framed
      ? `panel ${isFile ? 'p-0' : 'px-2 pt-0 pb-2'}`
      : 'p-0'}"
  >
    <!-- Header. `flex-wrap` lets the active tabBarEnd contents (selectors, controls, inputs)
         drop to subsequent rows when the panel is narrowed, instead of forcing a horizontal
         minimum width on the surrounding Pane. `min-w-0` on the row + a row gap keeps the
         wrapped rows visually grouped. The panel has no top padding; instead each 24px
         `.tab` (see feldera-modern.css) keeps 12px above and below, so the tab labels and
         any 32px controls in the tabBarEnd centre in a 48px row whose bottom edge is the
         header's line. A tabBarEnd wrapper is at least
         as tall (`min-h-12`), so when it wraps onto a row of its own its controls centre
         there too; a bare 32px button takes 8px margins (`my-2`) to fill the same row.
         The `'file'` variant has a 40px row instead, with the same rules. -->
    <Tabs.List
      class="flex w-full min-w-0 flex-wrap items-center gap-0 pb-0 mb-0 {isFile
        ? `${framed ? 'rounded-t-[7px]' : ''} border-line bg-toolbar pr-5 [&>:not([role=tab],button)]:min-h-10 [&>button:not([role=tab])]:my-1`
        : '[&>:not([role=tab],button)]:min-h-12 [&>button:not([role=tab])]:my-2'} {isUnderline
        ? 'px-2'
        : ''} {headerClass}"
    >
      {#each tabs as { id, label, title, disabled }}
        <Tabs.Trigger
          value={id}
          {disabled}
          {title}
          class={isFile
            ? `${fileTabClass(currentTab === id)} ${framed ? 'first:rounded-tl-[7px]' : ''} data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40`
            : isUnderline
              ? 'relative mt-3 mb-2 flex h-6 items-center gap-2 self-end px-3 text-base whitespace-nowrap text-surface-600-400 hover:bg-transparent hover:text-surface-950-50 hover:filter-none data-[disabled]:cursor-not-allowed data-[disabled]:opacity-40 data-[selected]:text-surface-950-50 data-[selected]:after:absolute data-[selected]:after:inset-x-0 data-[selected]:after:-bottom-2 data-[selected]:after:h-0.5 data-[selected]:after:bg-chart-line'
              : 'tab'}
        >
          {@render label()}
        </Tabs.Trigger>
      {/each}
      {@render activeTabBarEnd?.()}
    </Tabs.List>
    {@render tabContent()}
  </Tabs>
{/if}
