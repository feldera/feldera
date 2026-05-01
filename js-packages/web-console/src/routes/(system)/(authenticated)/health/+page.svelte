<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { match } from 'ts-pattern'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventList from '$lib/components/health/HealthEventList.svelte'
  import StatusTimeline, { type TimelineGroup } from '$lib/components/health/StatusTimeline.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { newDate } from '$lib/compositions/serverTime'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import { ceilToHour, dateMax } from '$lib/functions/common/date'
  import {
    type ClusterBucket,
    type ClusterEventType,
    groupHealthEvents,
    unpackCombinedEvent
  } from '$lib/functions/pipelines/health'
  import { resolve } from '$lib/functions/svelte'
  import type { ClusterMonitorEventSelectedInfo } from '$lib/services/manager'

  type EventTag = 'api' | 'compiler' | 'runner'

  const componentLabels: Record<EventTag, string> = {
    api: 'API server',
    compiler: 'Compiler server',
    runner: 'Runner'
  }

  let api = usePipelineManager()
  let events: ClusterMonitorEventSelectedInfo[] | null = $state(null)
  let refreshEvents = async () => {
    events = await api.getClusterEvents()
  }

  const loadClusterEventDetail = async (eventId: string, bucket: ClusterBucket) => {
    const e = await api.getClusterEvent(eventId)
    if (!e) {
      return null
    }
    const description = match(bucket.tag)
      .with('api', () => (e.api_self_info || '') + '\n' + (e.api_resources_info || ''))
      .with(
        'compiler',
        () => (e.compiler_self_info || '') + '\n' + (e.compiler_resources_info || '')
      )
      .with('runner', () => (e.runner_self_info || '') + '\n' + (e.runner_resources_info || ''))
      .exhaustive()
    return { timestamp: new Date(e.recorded_at), description }
  }

  const rawClusterEvents = $derived.by(() => events?.flatMap(unpackCombinedEvent) ?? [])

  const healthWindowHours = 72

  const lastTimestamp = (events: ClusterMonitorEventSelectedInfo[] | null) =>
    ceilToHour(events?.length ? dateMax(new Date(events.at(0)!.recorded_at), newDate()) : newDate())
  const firstTimestamp = (events: ClusterMonitorEventSelectedInfo[] | null) =>
    new Date(lastTimestamp(events).getTime() - healthWindowHours * 60 * 60 * 1000)

  // Group events for EventLogList
  const groupedClusterEvents = $derived.by(() =>
    rawClusterEvents.length ? groupHealthEvents(rawClusterEvents, 60 * 60 * 1000) : []
  )

  const splitClusterEvents = $derived.by(() => {
    const [unresolved, previous] = partition(groupedClusterEvents, (es) => es.active)
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

  let selectedEvent: ClusterBucket | null = $state(null)

  // Track selected time range (unified for both timeline bars and event list)
  let selectedEventTimestamp = $state<{ tag: EventTag; from: Date; to: Date } | null>(null)

  // Component refs for navigation
  let eventLogListRef: EventLogList<ClusterEventType, EventTag> | undefined = $state()
  let timelineRefs: { [key in EventTag]?: StatusTimeline<ClusterEventType> } = $state({})

  // Track which component has the active selection
  type ActiveComponent = { type: 'timeline'; tag: EventTag } | { type: 'incident' }
  let activeComponent: ActiveComponent | null = $state(null)

  // Open drawer with all events (including healthy) from the clicked timeline bar for a specific component
  function handleBarClick(tag: EventTag, group: TimelineGroup<ClusterEventType>) {
    activeComponent = { type: 'timeline', tag }
    if (!rawClusterEvents.length) {
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

    // Create a ClusterBucket from all events in the time range
    const bucket: ClusterBucket = {
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
  function handleEventSelected(eventBucket: ClusterBucket) {
    activeComponent = { type: 'incident' }
    // Update selected event time range
    selectedEventTimestamp = {
      tag: eventBucket.tag,
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }

    selectedEvent = eventBucket
  }

  // Navigate to previous event by delegating to the active component
  function navigatePrevious() {
    if (!activeComponent) {
      return
    }

    if (activeComponent.type === 'timeline') {
      const timeline = timelineRefs[activeComponent.tag]
      timeline?.selectPreviousBucket()
    } else {
      eventLogListRef?.selectPreviousIncident()
    }
  }

  // Navigate to next event by delegating to the active component
  function navigateNext() {
    if (!activeComponent) {
      return
    }

    if (activeComponent.type === 'timeline') {
      const timeline = timelineRefs[activeComponent.tag]
      timeline?.selectNextBucket()
    } else {
      eventLogListRef?.selectNextIncident()
    }
  }

  // Get navigation state from the active component
  const activeNavigationState = $derived.by(() => {
    if (!activeComponent) {
      return null
    }

    if (activeComponent.type === 'timeline') {
      const timeline = timelineRefs[activeComponent.tag]
      return timeline?.getNavigationState() ?? null
    } else {
      return eventLogListRef?.getNavigationState() ?? null
    }
  })

  // Derive navigation capabilities from the single navigation state
  const canNavigatePrevious = $derived(activeNavigationState && activeNavigationState !== 'start')

  const canNavigateNext = $derived(activeNavigationState && activeNavigationState !== 'end')

  const onCloseDrawer = () => {
    selectedEvent = null
    selectedEventTimestamp = null
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
    <!-- Status Timelines for each Feldera service -->
    <div class="flex flex-col gap-2">
      {#each Object.entries(componentLabels) as [tag, label], i}
        <StatusTimeline
          bind:this={timelineRefs[tag as EventTag]}
          {label}
          events={rawClusterEvents.filter((e) => e.tag === tag)}
          startAt={firstTimestamp(events)}
          endAt={lastTimestamp(events)}
          unitDurationMs={60 * 60 * 1000}
          class="flex flex-col gap-2"
          onBarClick={(group) => handleBarClick(tag as EventTag, group)}
          legend={i === 2 ? (['healthy', 'unhealthy', 'major_issue'] as ClusterEventType[]) : []}
          selectedBars={selectedEventTimestamp?.tag === tag
            ? { from: selectedEventTimestamp.from, to: selectedEventTimestamp.to }
            : null}
        ></StatusTimeline>
      {/each}
    </div>
    {#if !events}
      <Progress class="h-1" value={null} max={100}>
        <Progress.Track>
          <Progress.Range class="bg-primary-500" />
        </Progress.Track>
      </Progress>
    {/if}
    <EventLogList
      bind:this={eventLogListRef}
      previousEvents={splitClusterEvents.previous}
      unresolvedEvents={splitClusterEvents.unresolved}
      onEventSelected={handleEventSelected}
      selectedEvents={selectedEventTimestamp}
    >
      {#snippet noIssues()}
        {#if events}
          <span>The cluster experienced no issues in the observed period.</span>
        {/if}
      {/snippet}
    </EventLogList>
  </div>
  <!-- TODO: Create a responsive inline drawer - that takes up full width on smaller screens -->
  <Drawer open={!!selectedEvent} side="right" width="w-[500px]" onClose={onCloseDrawer}>
    {#if selectedEvent}
      <HealthEventList
        eventParts={selectedEvent}
        loadEventDetail={loadClusterEventDetail}
        onClose={onCloseDrawer}
        onNavigatePrevious={canNavigatePrevious ? navigatePrevious : undefined}
        onNavigateNext={canNavigateNext ? navigateNext : undefined}
      ></HealthEventList>
    {/if}
  </Drawer>
</div>
