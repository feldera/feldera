<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventList from '$lib/components/health/HealthEventList.svelte'
  import StatusTimeline, { type TimelineGroup } from '$lib/components/health/StatusTimeline.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import { ceilToHour } from '$lib/functions/common/date'
  import {
    groupHealthEvents,
    type HealthEventBucket,
    unpackCombinedEvent
  } from '$lib/functions/pipelines/health'
  import { resolve } from '$lib/functions/svelte'
  import type { ClusterMonitorEventSelectedInfo } from '$lib/services/manager'

  let {}: {} = $props()

  type EventTag = 'api' | 'compiler' | 'runner'

  const componentLabels: Record<EventTag, string> = {
    api: 'API server',
    compiler: 'Compiler server',
    runner: 'Runner'
  }

  let api = usePipelineManager()
  let events: ClusterMonitorEventSelectedInfo[] | null = $state(null)
  let refreshEvents = async () => {
    events = (await api.getClusterEvents()) as typeof events
  }

  const rawClusterEvents = $derived.by(() =>
    events?./*filter(e => !e.all_healthy).*/ flatMap(unpackCombinedEvent)
  )

  const healthWindowHours = 72

  const firstTimestamp = (events: ClusterMonitorEventSelectedInfo[]) =>
    new Date(lastTimestamp(events).getTime() - healthWindowHours * 60 * 60 * 1000)
  const lastTimestamp = (events: ClusterMonitorEventSelectedInfo[]) =>
    ceilToHour(new Date(events.at(0)!.recorded_at))

  // Group events for EventLogList
  const groupedClusterEvents = $derived.by(() =>
    events && rawClusterEvents ? groupHealthEvents(rawClusterEvents, 60 * 60 * 1000) : undefined
  )

  const splitClusterEvents = $derived.by(() => {
    const [unresolved, previous] = partition(groupedClusterEvents ?? [], (es) => es.active)
    return { unresolved, previous }
  })

  $effect(() => {
    refreshEvents()
  })

  const drawer = useAdaptiveDrawer('right')
  const breadcrumbs = $derived([
    {
      text: 'Home',
      href: resolve(`/`)
    },
    {
      text: 'Health',
      href: resolve(`/health/`)
    }
  ])

  let selectedEvent: HealthEventBucket | null = $state(null)

  // Track selected time range (unified for both timeline bars and event list)
  let selectedEventTimestamp = $state<{ tag: EventTag; from: Date; to: Date } | null>(null)

  // Open drawer with all events (including healthy) from the clicked timeline bar for a specific component
  function handleBarClick(tag: EventTag, group: TimelineGroup) {
    if (!rawClusterEvents) {
      return
    }

    // Find all events within the time range for this tag (including healthy events)
    const eventsInRange = rawClusterEvents.filter((e) => {
      const eventTime = e.timestamp.getTime()
      return e.tag === tag && eventTime >= group.startTime && eventTime < group.endTime
    })

    if (eventsInRange.length === 0) {
      return
    }

    // Create a HealthEventBucket from all events in the time range
    const bucket: HealthEventBucket = {
      timestampFrom: new Date(group.startTime),
      timestampTo: new Date(group.endTime),
      type: group.status,
      description: `${componentLabels[tag]} events`,
      tag,
      active: false,
      title: `${componentLabels[tag]} status history`,
      events: eventsInRange.map((e) => ({
        id: e.id,
        timestamp: e.timestamp,
        status: e.type
      }))
    }

    // Update selected time range for this component
    selectedEventTimestamp = {
      tag,
      from: new Date(group.startTime),
      to: new Date(group.endTime)
    }

    selectedEvent = bucket
  }

  // Open drawer when an event is selected from EventLogList
  function handleEventSelected(eventBucket: HealthEventBucket) {
    // Update selected event time range
    selectedEventTimestamp = {
      tag: eventBucket.tag,
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }

    selectedEvent = eventBucket
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
        aria-label="Open the right navigation drawer"
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

<div class="flex h-full flex-nowrap px-2 pb-5 md:px-8">
  <div class="flex h-full flex-1 flex-col gap-8 rounded-container">
    {#if events && rawClusterEvents}
      <!-- Status Timelines for each component -->
      <div class="flex flex-col gap-2">
        {#each Object.entries(componentLabels) as [tag, label], i}
          <StatusTimeline
            {label}
            events={rawClusterEvents.filter((e) => e.tag === tag)}
            startAt={firstTimestamp(events)}
            endAt={lastTimestamp(events)}
            unitDurationMs={60 * 60 * 1000}
            class="flex flex-col gap-2"
            onBarClick={(group) => handleBarClick(tag as EventTag, group)}
            legend={i === 2}
            selectedBars={selectedEventTimestamp?.tag === tag
              ? { from: selectedEventTimestamp.from, to: selectedEventTimestamp.to }
              : null}
          ></StatusTimeline>
        {/each}
      </div>
    {:else}
      <Progress value={null}></Progress>
    {/if}
    <EventLogList
      previousEvents={splitClusterEvents.previous}
      unresolvedEvents={splitClusterEvents.unresolved}
      onEventSelected={handleEventSelected}
      selectedEvents={selectedEventTimestamp}
    >
      {#snippet noIssues()}
        <span>The cluster experienced no issues in the observed period.</span>
      {/snippet}
    </EventLogList>
  </div>
  <!-- TODO: Create a responsive inline drawer - that takes up full width on smaller screens -->
  <InlineDrawer open={!!selectedEvent} side="right" width="w-[500px]">
    {#if selectedEvent}
      <HealthEventList
        eventParts={selectedEvent}
        onClose={() => {
          selectedEvent = null
          selectedEventTimestamp = null
        }}
      ></HealthEventList>
    {/if}
  </InlineDrawer>
</div>
