<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventList from '$lib/components/health/HealthEventList.svelte'
  import StatusTimeline, {
    type TimelineEvent,
    type TimelineGroup
  } from '$lib/components/health/StatusTimeline.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import { newDate } from '$lib/compositions/serverTime'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import { ceilToHour, dateMax } from '$lib/functions/common/date'
  import {
    classifyPipelineEvents,
    formatPipelineEventDescription,
    groupPipelineEvents,
    type PipelineEventTag,
    type PipelineHealthBucket,
    type PipelineHealthStatus
  } from '$lib/functions/pipelines/pipelineHealth'
  import type { PipelineMonitorEventSelectedInfo } from '$lib/services/manager'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  let {
    pipeline,
    deleted = false
  }: {
    pipeline: { current: ExtendedPipeline }
    deleted?: boolean
  } = $props()

  const pipelineName = $derived(pipeline.current.name)
  const api = usePipelineManager()

  let events: PipelineMonitorEventSelectedInfo[] | null = $state(null)

  async function refreshEvents(name: string) {
    events = null
    const fetched = await api.getPipelineEvents(name)
    if (name === pipelineName) {
      events = fetched
    }
  }

  $effect(() => {
    refreshEvents(pipelineName)
  })

  const classifiedEvents = $derived.by(() => classifyPipelineEvents(events ?? []))

  /**
   * Events with a `null` classification ("no data") never reach the timeline —
   * the bar for those buckets is rendered as the default "no data" gray.
   */
  const timelineEvents = $derived.by((): TimelineEvent<PipelineHealthStatus>[] =>
    classifiedEvents
      .filter((e): e is typeof e & { type: PipelineHealthStatus } => e.type !== null)
      .map((e) => ({
        timestamp: e.timestamp,
        type: e.type,
        description: ''
      }))
  )

  const healthWindowHours = 72

  const lastTimestamp = (es: PipelineMonitorEventSelectedInfo[] | null) =>
    ceilToHour(es?.length ? dateMax(new Date(es[0].recorded_at), newDate()) : newDate())
  const firstTimestamp = (es: PipelineMonitorEventSelectedInfo[] | null) =>
    new Date(lastTimestamp(es).getTime() - healthWindowHours * 60 * 60 * 1000)

  const groupedEvents = $derived.by(() => groupPipelineEvents(classifiedEvents))

  const splitEvents = $derived.by(() => {
    const [unresolved, previous] = partition(groupedEvents, (e) => e.active)
    return { unresolved, previous }
  })

  let selectedEvent: PipelineHealthBucket | null = $state(null)
  let selectedEventTimestamp = $state<{
    tag: PipelineEventTag
    from: Date
    to: Date
  } | null>(null)

  let eventLogListRef: EventLogList<PipelineHealthStatus, PipelineEventTag> | undefined = $state()
  let timelineRef: StatusTimeline<PipelineHealthStatus> | undefined = $state()

  type ActiveComponent = { type: 'timeline' } | { type: 'incident' }
  let activeComponent: ActiveComponent | null = $state(null)

  const loadPipelineEventDetail = async (eventId: string) => {
    const e = await api.getPipelineEvent(pipelineName, eventId)
    if (!e) {
      return null
    }
    return {
      timestamp: new Date(e.recorded_at),
      description: formatPipelineEventDescription(e)
    }
  }

  function handleBarClick(group: TimelineGroup<PipelineHealthStatus>) {
    activeComponent = { type: 'timeline' }
    if (!timelineEvents.length) {
      return
    }

    const eventsInRange = classifiedEvents.filter(
      (e): e is typeof e & { type: PipelineHealthStatus } => {
        if (e.type === null) {
          return false
        }
        const t = e.timestamp.getTime()
        return t >= group.startTime && t < group.endTime
      }
    )

    if (eventsInRange.length === 0) {
      return
    }

    const bucket: PipelineHealthBucket = {
      timestampFrom: new Date(group.startTime),
      timestampTo: new Date(group.endTime),
      type: group.status,
      description: 'Pipeline events',
      tag: 'pipeline',
      active: false,
      title: 'Pipeline status history',
      events: eventsInRange.map((e) => ({
        id: e.id,
        timestamp: e.timestamp,
        status: e.type
      }))
    }

    selectedEventTimestamp = {
      tag: 'pipeline',
      from: new Date(group.startTime),
      to: new Date(group.endTime)
    }
    selectedEvent = bucket
  }

  function handleEventSelected(eventBucket: PipelineHealthBucket) {
    activeComponent = { type: 'incident' }
    selectedEventTimestamp = {
      tag: eventBucket.tag,
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }
    selectedEvent = eventBucket
  }

  function navigatePrevious() {
    if (!activeComponent) {
      return
    }
    if (activeComponent.type === 'timeline') {
      timelineRef?.selectPreviousBucket()
    } else {
      eventLogListRef?.selectPreviousIncident()
    }
  }

  function navigateNext() {
    if (!activeComponent) {
      return
    }
    if (activeComponent.type === 'timeline') {
      timelineRef?.selectNextBucket()
    } else {
      eventLogListRef?.selectNextIncident()
    }
  }

  const activeNavigationState = $derived.by(() => {
    if (!activeComponent) {
      return null
    }
    if (activeComponent.type === 'timeline') {
      return timelineRef?.getNavigationState() ?? null
    }
    return eventLogListRef?.getNavigationState() ?? null
  })

  const canNavigatePrevious = $derived(activeNavigationState && activeNavigationState !== 'start')
  const canNavigateNext = $derived(activeNavigationState && activeNavigationState !== 'end')

  function closeDrawer() {
    selectedEvent = null
    selectedEventTimestamp = null
  }
</script>

<div data-testid="box-pipeline-health" class="flex h-full flex-nowrap overflow-hidden">
  <div class="flex h-full flex-1 flex-col gap-4 overflow-hidden">
    <StatusTimeline
      bind:this={timelineRef}
      label="Pipeline status"
      events={timelineEvents}
      startAt={firstTimestamp(events)}
      endAt={lastTimestamp(events)}
      unitDurationMs={60 * 60 * 1000}
      class="flex flex-col gap-2"
      onBarClick={handleBarClick}
      legend={['healthy', 'transitioning', 'unhealthy', 'major_issue']}
      selectedBars={selectedEventTimestamp
        ? { from: selectedEventTimestamp.from, to: selectedEventTimestamp.to }
        : null}
    ></StatusTimeline>

    {#if !events && !deleted}
      <Progress class="h-1" value={null} max={100}>
        <Progress.Track>
          <Progress.Range class="bg-primary-500" />
        </Progress.Track>
      </Progress>
    {/if}

    <div class="flex-1 overflow-hidden">
      <EventLogList
        bind:this={eventLogListRef}
        previousEvents={splitEvents.previous}
        unresolvedEvents={splitEvents.unresolved}
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
  </div>

  <Drawer
    open={!!selectedEvent}
    side="right"
    width="w-[500px]"
    inlineClass="rounded pt-4 pl-4"
    onClose={closeDrawer}
  >
    {#if selectedEvent}
      <HealthEventList
        eventParts={selectedEvent}
        loadEventDetail={loadPipelineEventDetail}
        onClose={closeDrawer}
        onNavigatePrevious={canNavigatePrevious ? navigatePrevious : undefined}
        onNavigateNext={canNavigateNext ? navigateNext : undefined}
      ></HealthEventList>
    {/if}
  </Drawer>
</div>
