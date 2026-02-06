<script lang="ts" module>
  export { label as Label }
</script>

<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventList from '$lib/components/health/HealthEventList.svelte'
  import StatusTimeline, { type TimelineGroup } from '$lib/components/health/StatusTimeline.svelte'
  import InlineDrawer from '$lib/components/layout/InlineDrawer.svelte'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import {
    createBucketFromTimelineGroup,
    formatPipelineEventDetail,
    groupHealthEvents,
    healthTimeWindowEnd,
    healthTimeWindowStart,
    type HealthEventBucket,
    type PipelineEventTag,
    pipelineTagTitles,
    unpackPipelineEvent
  } from '$lib/functions/pipelines/health'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMonitorEventSelectedInfo } from '$lib/services/manager'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  const pipelineName = $derived(pipeline.current.name)

  const api = usePipelineManager()
  let events: PipelineMonitorEventSelectedInfo[] | null = $state(null)

  useInterval(async () => {
    events = await api.getPipelineEvents(pipelineName)
  }, 60_000)

  const rawPipelineEvents = $derived.by(() => events?.map(unpackPipelineEvent) ?? [])

  const healthWindowHours = 120

  const endAt = $derived(healthTimeWindowEnd(events))
  const startAt = $derived(healthTimeWindowStart(endAt, healthWindowHours))

  const groupedPipelineEvents = $derived.by(() =>
    rawPipelineEvents.length
      ? groupHealthEvents(rawPipelineEvents, 60 * 60 * 1000, pipelineTagTitles)
      : []
  )

  const splitPipelineEvents = $derived.by(() => {
    const [unresolved, previous] = partition(groupedPipelineEvents, (es) => es.active)
    return { unresolved, previous }
  })

  let selectedEvent: HealthEventBucket<PipelineEventTag> | null = $state(null)
  let selectedEventTimestamp = $state<{ tag: PipelineEventTag; from: Date; to: Date } | null>(null)

  let eventLogListRef: EventLogList | undefined = $state()
  let timelineRef: StatusTimeline | undefined = $state()

  type ActiveComponent = { type: 'timeline' } | { type: 'incident' }
  let activeComponent: ActiveComponent | null = $state(null)

  function handleBarClick(group: TimelineGroup) {
    activeComponent = { type: 'timeline' }
    const bucket = createBucketFromTimelineGroup(rawPipelineEvents, 'pipeline', group, 'Pipeline')
    if (!bucket) return

    selectedEventTimestamp = {
      tag: 'pipeline',
      from: new Date(group.startTime),
      to: new Date(group.endTime)
    }
    selectedEvent = bucket
  }

  function handleEventSelected(eventBucket: HealthEventBucket) {
    activeComponent = { type: 'incident' }
    selectedEventTimestamp = {
      tag: 'pipeline',
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }
    selectedEvent = eventBucket as HealthEventBucket<PipelineEventTag>
  }

  function navigatePrevious() {
    if (!activeComponent) return
    if (activeComponent.type === 'timeline') {
      timelineRef?.selectPreviousBucket()
    } else {
      eventLogListRef?.selectPreviousIncident()
    }
  }

  function navigateNext() {
    if (!activeComponent) return
    if (activeComponent.type === 'timeline') {
      timelineRef?.selectNextBucket()
    } else {
      eventLogListRef?.selectNextIncident()
    }
  }

  const activeNavigationState = $derived.by(() => {
    if (!activeComponent) return null
    if (activeComponent.type === 'timeline') {
      return timelineRef?.getNavigationState() ?? null
    } else {
      return eventLogListRef?.getNavigationState() ?? null
    }
  })

  const canNavigatePrevious = $derived(activeNavigationState && activeNavigationState !== 'start')
  const canNavigateNext = $derived(activeNavigationState && activeNavigationState !== 'end')

  function loadPipelineEventDetail(
    eventId: string
  ): Promise<{ timestamp: Date; description: string } | null> {
    return api.getPipelineEvent(pipelineName, eventId).then((e) => {
      if (!e) return null
      return formatPipelineEventDetail(e)
    })
  }
</script>

{#snippet label()}
  <span class=""> Health </span>
{/snippet}

<div class="bg-white-dark flex h-full flex-nowrap rounded px-2 pt-2">
  <div class="flex h-full flex-1 flex-col gap-2">
    <StatusTimeline
      bind:this={timelineRef}
      events={rawPipelineEvents}
      {startAt}
      {endAt}
      unitDurationMs={60 * 60 * 1000}
      class="flex flex-col gap-2"
      onBarClick={handleBarClick}
      legend
      selectedBars={selectedEventTimestamp
        ? { from: selectedEventTimestamp.from, to: selectedEventTimestamp.to }
        : null}
    ></StatusTimeline>
    {#if !events}
      <Progress class="h-1" value={null} max={100}>
        <Progress.Track>
          <Progress.Range class="bg-primary-500" />
        </Progress.Track>
      </Progress>
    {/if}
    <EventLogList
      bind:this={eventLogListRef}
      previousEvents={splitPipelineEvents.previous}
      unresolvedEvents={splitPipelineEvents.unresolved}
      onEventSelected={handleEventSelected}
      selectedEvents={selectedEventTimestamp}
    >
      {#snippet noIssues()}
        {#if events}
          <span>The pipeline experienced no issues in the observed period.</span>
        {/if}
      {/snippet}
    </EventLogList>
  </div>
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
        loadEventDetail={loadPipelineEventDetail}
      ></HealthEventList>
    {/if}
  </InlineDrawer>
</div>
