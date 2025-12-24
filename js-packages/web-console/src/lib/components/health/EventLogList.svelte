<script lang="ts">
  import { slide } from 'svelte/transition'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { formatDateTime } from '$lib/functions/format'
  import type { HealthEventParts } from '$lib/functions/pipelines/health'
  import type { Snippet } from '$lib/types/svelte'

  let {
    previousEvents,
    unresolvedEvents,
    noIssues,
    onEventSelected
  }: {
    previousEvents: HealthEventParts[]
    unresolvedEvents: HealthEventParts[]
    noIssues: Snippet
    onEventSelected?: (eventParts: HealthEventParts) => void
  } = $props()

  let showUnresolved = $state(true)
  let containerElement: HTMLDivElement | undefined = $state()

  function getEventId(event: HealthEventParts): string {
    return `event-${event.timestampFrom.getTime()}-${event.tag}`
  }

  export function scrollToEvent(event: HealthEventParts) {
    const eventId = getEventId(event)
    const element = containerElement?.querySelector(`#${CSS.escape(eventId)}`)
    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }
  }

  // const startOfHour = (events: HealthEventParts[]) => {
  //   const MS_PER_HOUR = 60 * 60 * 1000
  //   const ms = events[0].timestampFrom.getTime()
  //   // Floor to UTC hour boundary
  //   const hourStartMs = Math.floor(ms / MS_PER_HOUR) * MS_PER_HOUR
  //   return new Date(hourStartMs)
  // }
</script>

{#snippet eventItem(event: HealthEventParts, iconClass: string = '')}
  <button
    id={getEventId(event)}
    class="flex flex-nowrap items-center gap-1"
    onclick={() => onEventSelected?.(event)}
  >
    <span
      class="text-[24px] {event.type === 'unhealthy'
        ? 'fd fd-triangle-alert text-warning-500'
        : 'fd fd-circle-x text-error-500'} {iconClass}"
    ></span>
    <span class="line-clamp-1">{event.description}</span>
  </button>
{/snippet}

{#snippet eventGroup(events: HealthEventParts, iconClass: string = '')}
  <div class="flex flex-nowrap gap-7">
    <span class="w-[180px] text-surface-800-200 sm:w-[320px]">
      <!-- {events[0].timestampFrom.toLocaleString(undefined, {
        dateStyle: 'medium'
      })} - {events.at(-1)!.timestampTo.toLocaleString(undefined, {
        dateStyle: 'medium'
      })} -->
      {formatDateTime(events.timestampFrom)} - {formatDateTime(events.timestampTo)}
    </span>
    <div class="flex flex-col gap-4">
      <!-- {#each events as event} -->
      {@render eventItem(events, iconClass)}
      <!-- {/each} -->
    </div>
  </div>
{/snippet}

{#snippet dropdownHeader(open: boolean, toggle: () => void, title: Snippet)}
  <div class="flex w-fit cursor-pointer items-center gap-2" onclick={toggle} role="presentation">
    <div
      class={'fd fd-chevron-down text-[20px] transition-transform ' + (open ? 'rotate-180' : '')}
    ></div>
    {@render title()}
  </div>
{/snippet}

<div bind:this={containerElement} class="flex max-h-[calc(100vh-300px)] flex-col gap-3 overflow-y-auto">
  {#if unresolvedEvents.length > 0}
    <div class="flex flex-col gap-3">
      <InlineDropdown bind:open={showUnresolved}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            <span class="text-xl font-semibold">
              {#if unresolvedEvents.length > 1}
                {unresolvedEvents.length} unresolved issues
              {:else}
                1 unresolved issue
              {/if}
            </span>
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }} class="flex flex-col gap-5">
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
      <span class="text-xl font-semibold"> Previous incidents </span>

      <div class="flex flex-col gap-5">
        {#each previousEvents as events, i}
          {@render eventGroup(events, 'text-surface-900-100')}
        {/each}
      </div>
    {:else if !unresolvedEvents.length}
      {@render noIssues()}
    {/if}
  </div>
</div>
