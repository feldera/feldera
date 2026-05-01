<script lang="ts" generics="S extends BaseHealthStatus, T extends string">
  import { slide } from 'svelte/transition'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { formatDateTimeRange } from '$lib/functions/format'
  import type { BaseHealthStatus, HealthEventBucket } from '$lib/functions/pipelines/health'
  import type { Snippet } from '$lib/types/svelte'

  type Bucket = HealthEventBucket<S, T>

  let {
    previousEvents,
    unresolvedEvents,
    noIssues,
    onEventSelected,
    selectedEvents = null
  }: {
    previousEvents: Bucket[]
    unresolvedEvents: Bucket[]
    noIssues: Snippet
    onEventSelected?: (eventParts: Bucket) => void
    selectedEvents?: { tag: T; from: Date; to: Date } | null
  } = $props()

  let showUnresolved = $state(true)
  let containerElement: HTMLDivElement | undefined = $state()

  function getEventId(event: Bucket): string {
    return `event-${event.timestampFrom.getTime()}-${event.tag}`
  }

  function iconClassFor(type: BaseHealthStatus): string {
    switch (type) {
      case 'major_issue':
        return 'fd fd-circle-x text-error-500'
      case 'unhealthy':
        return 'fd fd-triangle-alert text-warning-500'
      case 'transitioning':
        return 'fd fd-circle-dot text-blue-500'
      case 'healthy':
      default:
        return 'fd fd-circle-check-big text-success-500'
    }
  }

  // Check if an event overlaps with the selected range for the same tag
  function eventMatchesSelection(
    event: Bucket,
    selection: { tag: T; from: Date; to: Date }
  ): boolean {
    return (
      selection.tag === event.tag &&
      ((event.timestampFrom.getTime() >= selection.from.getTime() &&
        event.timestampFrom.getTime() <= selection.to.getTime()) ||
        (event.timestampTo.getTime() <= selection.to.getTime() &&
          event.timestampTo.getTime() >= selection.from.getTime()))
    )
  }

  // Compute all selected events once
  const selectedEventBuckets = $derived.by(() => {
    if (!selectedEvents) {
      return new Set<Bucket>()
    }
    const selected = new Set<Bucket>()
    for (const event of [...unresolvedEvents, ...previousEvents]) {
      if (eventMatchesSelection(event, selectedEvents)) {
        selected.add(event)
      }
    }
    return selected
  })

  // Get the first selected event (for scrolling)
  const firstSelectedEvent = $derived(
    selectedEventBuckets.size > 0 ? [...selectedEventBuckets][0] : null
  )

  // Auto-scroll to first selected event when selection changes
  $effect(() => {
    if (firstSelectedEvent && containerElement) {
      const eventId = getEventId(firstSelectedEvent)
      const element = containerElement.querySelector(`#${CSS.escape(eventId)}`)
      if (element) {
        element.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
    }
  })

  export function scrollToEvent(event: Bucket) {
    const eventId = getEventId(event)
    const element = containerElement?.querySelector(`#${CSS.escape(eventId)}`)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }
  }

  // Navigation methods for incidents
  export function selectNextIncident(): boolean {
    if (!selectedEvents) {
      return false
    }

    const tag = selectedEvents.tag
    const allIncidents = [...unresolvedEvents, ...previousEvents].filter((e) => e.tag === tag)

    // Find current incident
    const currentIndex = allIncidents.findIndex(
      (e) =>
        e.timestampFrom.getTime() === selectedEvents.from.getTime() &&
        e.timestampTo.getTime() === selectedEvents.to.getTime()
    )

    if (currentIndex > 0) {
      onEventSelected?.(allIncidents[currentIndex - 1])
      return true
    }
    return false
  }

  export function selectPreviousIncident(): boolean {
    if (!selectedEvents) {
      return false
    }

    const tag = selectedEvents.tag
    const allIncidents = [...unresolvedEvents, ...previousEvents].filter((e) => e.tag === tag)

    // Find current incident
    const currentIndex = allIncidents.findIndex(
      (e) =>
        e.timestampFrom.getTime() === selectedEvents.from.getTime() &&
        e.timestampTo.getTime() === selectedEvents.to.getTime()
    )

    if (currentIndex >= 0 && currentIndex < allIncidents.length - 1) {
      onEventSelected?.(allIncidents[currentIndex + 1])
      return true
    }
    return false
  }

  // Compute navigation state once
  const navigationState = $derived.by((): 'start' | 'middle' | 'end' | null => {
    if (!selectedEvents) {
      return null
    }

    const tag = selectedEvents.tag
    const allIncidents = [...unresolvedEvents, ...previousEvents].filter((e) => e.tag === tag)

    const currentIndex = allIncidents.findIndex(
      (e) =>
        e.timestampFrom.getTime() === selectedEvents.from.getTime() &&
        e.timestampTo.getTime() === selectedEvents.to.getTime()
    )

    if (currentIndex < 0) {
      return null
    }

    const isAtEnd = currentIndex === 0
    const isAtStart = currentIndex === allIncidents.length - 1

    if (isAtStart && isAtEnd) {
      return null // Only one item
    }
    if (isAtStart) {
      return 'start'
    }
    if (isAtEnd) {
      return 'end'
    }
    return 'middle'
  })

  export const getNavigationState = () => navigationState
</script>

{#snippet eventItem(event: Bucket, iconClass: string = '')}
  <button
    id={getEventId(event)}
    class="flex flex-nowrap items-center gap-1 outline-none"
    onclick={() => onEventSelected?.(event)}
  >
    <span class="text-[24px] {iconClassFor(event.type)} {iconClass}"></span>
    <span class="line-clamp-1">{event.description}</span>
  </button>
{/snippet}

{#snippet eventGroup(events: Bucket, iconClass: string = '')}
  <div
    class="-m-1 flex flex-nowrap items-center gap-7 rounded p-1 py-2 {selectedEventBuckets.has(
      events
    )
      ? 'bg-primary-50-950/50'
      : ''}"
  >
    <span class="w-[180px] text-surface-600-400 sm:w-[320px]">
      {formatDateTimeRange(events.timestampFrom, events.timestampTo)}
    </span>
    <div class="flex flex-col gap-4">
      {@render eventItem(events, iconClass)}
    </div>
  </div>
{/snippet}

{#snippet dropdownHeader(open: boolean, toggle: () => void, title: Snippet, className = '')}
  <div
    class="flex cursor-pointer items-center gap-2 {className}"
    onclick={toggle}
    role="presentation"
  >
    <div
      class={'fd fd-chevron-down text-[20px] transition-transform ' + (open ? 'rotate-180' : '')}
    ></div>
    {@render title()}
  </div>
{/snippet}

<div class="relative -mx-2 scrollbar h-full overflow-y-auto px-2">
  <div bind:this={containerElement} class="absolute flex flex-col gap-3 pb-2">
    {#if unresolvedEvents.length > 0}
      <div class="flex flex-col gap-3">
        <InlineDropdown bind:open={showUnresolved}>
          {#snippet header(open, toggle)}
            {#snippet title()}
              <div class="w-full text-xl font-semibold">
                {#if unresolvedEvents.length > 1}
                  {unresolvedEvents.length} ongoing incidents
                {:else}
                  1 ongoing incident
                {/if}
              </div>
            {/snippet}
            {@render dropdownHeader(
              open,
              toggle,
              title,
              'bg-white-dark sticky top-0 -mx-1 px-1 py-1 flex-1'
            )}
          {/snippet}
          {#snippet content()}
            <div transition:slide={{ duration: 150 }} class="flex flex-col gap-3">
              {#each unresolvedEvents as events, i}
                {@render eventGroup(events)}
              {/each}
            </div>
          {/snippet}
        </InlineDropdown>
      </div>
    {/if}

    <div class="flex flex-col gap-3">
      {#if previousEvents.length}
        <span class="bg-white-dark sticky top-0 -mx-1 px-1 py-1 text-xl font-semibold">
          Previous incidents
        </span>

        <div class="flex flex-col gap-3">
          {#each previousEvents as events}
            {@render eventGroup(events, 'text-surface-900-100')}
          {/each}
        </div>
      {:else if !unresolvedEvents.length}
        {@render noIssues()}
      {/if}
    </div>
  </div>
</div>
