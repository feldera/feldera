<script lang="ts">
  import { Progress, Tabs } from '@skeletonlabs/skeleton-svelte'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventDetails from '$lib/components/health/HealthEventDetails.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import StatusTimeline, {
    type EventType,
    type TimelineGroup
  } from '$lib/components/StatusTimeline.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { useContextDrawer } from '$lib/compositions/layout/useContextDrawer.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import { ceilToHour } from '$lib/functions/common/date'
  import {
    groupHealthEvents,
    type HealthEventParts,
    unpackCombinedEvent
  } from '$lib/functions/pipelines/health'
  import { resolve } from '$lib/functions/svelte'
  import type { ClusterMonitorEventSelectedInfo, MonitorStatus } from '$lib/services/manager'

  let {}: {} = $props()

  let tabs = ['all', 'api', 'compiler', 'runner'] as const
  let tabLabels = {
    all: 'All',
    api: 'API',
    runner: 'Kubernetes runner',
    compiler: 'Compiler'
  }
  let tabText = {
    api: 'API',
    runner: 'runner',
    compiler: 'compiler'
  }
  let currentTab: (typeof tabs)[number] = $state('all')

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

  const groupedClusterEvents = $derived.by(() =>
    events && rawClusterEvents
      ? groupHealthEvents(rawClusterEvents).filter(
          (es) => currentTab === 'all' || es.tag === currentTab
        )
      : undefined
  )

  const contextDrawer = useContextDrawer()

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

  let selectedEvent: HealthEventParts | null = $state(null)
  let eventLogListRef: EventLogList | undefined = $state()

  $effect(() => {
    if (selectedEvent) {
      if (!contextDrawer.content) {
        contextDrawer.content = eventDetails
      }
    } else {
      contextDrawer.content = null
    }
  })

  // Scroll to the first incident that belongs to the clicked timeline bar
  function handleBarClick(group: TimelineGroup) {
    if (!groupedClusterEvents) {
      return
    }
    // Find the event that overlaps with the clicked time range
    const matchingEvent = groupedClusterEvents.find((event) => {
      const eventStart = event.timestampFrom.getTime()
      const eventEnd = event.timestampTo.getTime()
      // Check if there's any overlap between the group and event time ranges
      return eventStart <= group.endTime && eventEnd >= group.startTime
    })

    if (matchingEvent && eventLogListRef) {
      eventLogListRef.scrollToEvent(matchingEvent)
    }
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

<div class="h-full px-2 pb-5 md:px-8">
  <Tabs
    value={currentTab}
    onValueChange={(e) => (currentTab = e.value as typeof currentTab)}
    class="flex h-full flex-1 flex-col gap-8 space-y-0! rounded-container p-4"
  >
    <Tabs.List class="flex w-full flex-wrap-reverse gap-0 pb-0 text-nowrap lg:flex-nowrap">
      {#each tabs as tabName}
        <Tabs.Trigger
          value={tabName}
          class="btn h-9 font-medium {tabName === currentTab
            ? 'border-surface-950-50 '
            : 'rounded hover:bg-surface-100-900/50'}"
        >
          {tabLabels[tabName]}
        </Tabs.Trigger>
      {/each}
      <Tabs.Indicator />
    </Tabs.List>
    {#if events && rawClusterEvents}
      <!-- TODO: Find a way to avoid filtering two times: here and in `splitClusterEvents` -->
      <StatusTimeline
        events={rawClusterEvents.filter((es) => currentTab === 'all' || es.tag === currentTab)}
        startAt={firstTimestamp(events)}
        endAt={lastTimestamp(events)}
        unitDurationMs={60 * 60 * 1000}
        class="-mt-4! flex flex-col gap-2"
        onBarClick={handleBarClick}
      ></StatusTimeline>
    {:else}
      <Progress value={null}></Progress>
    {/if}
    <EventLogList
      bind:this={eventLogListRef}
      previousEvents={splitClusterEvents.previous}
      unresolvedEvents={splitClusterEvents.unresolved}
      onEventSelected={(eventParts) => {
        selectedEvent = eventParts
      }}
    >
      {#snippet noIssues()}
        <span>
          {#if currentTab === 'all'}
            The cluster experienced no issues in the observed period.
          {:else}
            The cluster experienced no issues with the {tabText[currentTab]} in the observed period.
          {/if}
        </span>
      {/snippet}
    </EventLogList>
  </Tabs>
</div>

{#snippet eventDetails()}
  {#if selectedEvent}
    <HealthEventDetails eventParts={selectedEvent} onClose={() => (selectedEvent = null)}
    ></HealthEventDetails>
  {/if}
{/snippet}
