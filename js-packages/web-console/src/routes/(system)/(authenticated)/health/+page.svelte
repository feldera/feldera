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
  import {
    type ClusterEventTag,
    createBucketFromTimelineGroup,
    formatClusterEventDetail,
    groupHealthEvents,
    type HealthEventBucket,
    healthTimeWindowEnd,
    healthTimeWindowStart,
    unpackCombinedEvent
  } from '$lib/functions/pipelines/health'
  import { resolve } from '$lib/functions/svelte'
  import type { ClusterMonitorEventSelectedInfo } from '$lib/services/manager'

  let {}: {} = $props()

  const componentLabels: Record<ClusterEventTag, string> = {
    api: 'API server',
    compiler: 'Compiler server',
    runner: 'Runner'
  }

  let api = usePipelineManager()
  let events: ClusterMonitorEventSelectedInfo[] | null = $state(null)
  let refreshEvents = async () => {
    events = await api.getClusterEvents()
  }

  const rawClusterEvents = $derived.by(() => events?.flatMap(unpackCombinedEvent) ?? [])

  const healthWindowHours = 72

  const endAt = $derived(healthTimeWindowEnd(events))
  const startAt = $derived(healthTimeWindowStart(endAt, healthWindowHours))

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

  let selectedEvent: HealthEventBucket<ClusterEventTag> | null = $state(null)

  // Track selected time range (unified for both timeline bars and event list)
  let selectedEventTimestamp = $state<{ tag: ClusterEventTag; from: Date; to: Date } | null>(null)

  // Component refs for navigation
  let eventLogListRef: EventLogList | undefined = $state()
  let timelineRefs: { [key in ClusterEventTag]?: StatusTimeline } = $state({})

  // Track which component has the active selection
  type ActiveComponent = { type: 'timeline'; tag: ClusterEventTag } | { type: 'incident' }
  let activeComponent: ActiveComponent | null = $state(null)

  // Open drawer with all events (including healthy) from the clicked timeline bar for a specific component
  function handleBarClick(tag: ClusterEventTag, group: TimelineGroup) {
    activeComponent = { type: 'timeline', tag }
    const bucket = createBucketFromTimelineGroup(rawClusterEvents, tag, group, componentLabels[tag])
    if (!bucket) return

    selectedEventTimestamp = {
      tag,
      from: new Date(group.startTime),
      to: new Date(group.endTime)
    }
    selectedEvent = bucket
  }

  // Open drawer when an event is selected from EventLogList
  function handleEventSelected(eventBucket: HealthEventBucket) {
    activeComponent = { type: 'incident' }
    selectedEventTimestamp = {
      tag: eventBucket.tag as ClusterEventTag,
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }
    selectedEvent = eventBucket as HealthEventBucket<ClusterEventTag>
  }

  // Navigate to previous event by delegating to the active component
  function navigatePrevious() {
    if (!activeComponent) return

    if (activeComponent.type === 'timeline') {
      const timeline = timelineRefs[activeComponent.tag]
      timeline?.selectPreviousBucket()
    } else {
      eventLogListRef?.selectPreviousIncident()
    }
  }

  // Navigate to next event by delegating to the active component
  function navigateNext() {
    if (!activeComponent) return

    if (activeComponent.type === 'timeline') {
      const timeline = timelineRefs[activeComponent.tag]
      timeline?.selectNextBucket()
    } else {
      eventLogListRef?.selectNextIncident()
    }
  }

  // Get navigation state from the active component
  const activeNavigationState = $derived.by(() => {
    if (!activeComponent) return null

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

  // Load cluster event details for the drawer
  function loadClusterEventDetail(
    eventId: string
  ): Promise<{ timestamp: Date; description: string } | null> {
    if (!selectedEvent) return Promise.resolve(null)
    const tag = selectedEvent.tag
    return api.getClusterEvent(eventId).then((e) => {
      if (!e) return null
      return formatClusterEventDetail(e, tag)
    })
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
          bind:this={timelineRefs[tag as ClusterEventTag]}
          {label}
          events={rawClusterEvents.filter((e) => e.tag === tag)}
          {startAt}
          {endAt}
          unitDurationMs={60 * 60 * 1000}
          class="flex flex-col gap-2"
          onBarClick={(group) => handleBarClick(tag as ClusterEventTag, group)}
          legend={i === 2}
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
  <InlineDrawer open={!!selectedEvent} side="right" width="w-[500px]">
    {#if selectedEvent}
      <HealthEventList
        eventParts={selectedEvent}
        onClose={() => {
          selectedEvent = null
          selectedEventTimestamp = null
        }}
        onNavigatePrevious={canNavigatePrevious ? navigatePrevious : undefined}
        onNavigateNext={canNavigateNext ? navigateNext : undefined}
        loadEventDetail={loadClusterEventDetail}
      ></HealthEventList>
    {/if}
  </InlineDrawer>
</div>
