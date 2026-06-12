<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { match } from 'ts-pattern'
  import EventLogList from '$lib/components/health/EventLogList.svelte'
  import HealthEventList from '$lib/components/health/HealthEventList.svelte'
  import StatusTimeline, {
    type TimelineEvent,
    type TimelineGroup
  } from '$lib/components/health/StatusTimeline.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { ServerDate } from '$lib/compositions/serverTime'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { partition } from '$lib/functions/common/array'
  import { ceilToHour, dateMax } from '$lib/functions/common/date'
  import {
    categorizePipelineEvents,
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
  let lastUpdated = $state<Date | null>(null)

  async function refreshEvents(name: string) {
    const fetched = await api.getPipelineEvents(name)
    if (name === pipelineName) {
      events = fetched
      lastUpdated = new Date()
    }
  }

  const pipelineStatus = (type: PipelineHealthStatus) =>
    match(type)
      .with('major_issue', () => ({
        iconClass: 'fd fd-circle-x text-error-500',
        statusColor: 'bg-red-500',
        severity: 3,
        barColor: (h: boolean) => (h ? 'fill-red-600' : 'fill-red-500'),
        statusStyle: { bg: 'bg-red-500', text: 'text-red-500', label: 'Major Issue' }
      }))
      .with('unhealthy', () => ({
        iconClass: 'fd fd-triangle-alert text-warning-500',
        statusColor: 'bg-yellow-500',
        severity: 2,
        barColor: (h: boolean) => (h ? 'fill-yellow-600' : 'fill-yellow-500'),
        statusStyle: { bg: 'bg-yellow-500', text: 'text-yellow-500', label: 'Service degradation' }
      }))
      .with('transitioning', () => ({
        iconClass: 'fd fd-circle-dot text-blue-500',
        statusColor: 'bg-blue-500',
        severity: 1,
        barColor: (h: boolean) => (h ? 'fill-blue-600' : 'fill-blue-500'),
        statusStyle: { bg: 'bg-blue-500', text: 'text-blue-500', label: 'Transitioning' }
      }))
      .with('idle', () => ({
        iconClass: 'fd fd-circle-minus text-surface-400-600',
        statusColor: 'bg-tertiary-100-900',
        severity: -1,
        barColor: (h: boolean) => (h ? 'fill-tertiary-100-900' : 'fill-tertiary-100-900'),
        statusStyle: { bg: 'bg-tertiary-100-900', text: 'text-tertiary-400-600', label: 'Idle' }
      }))
      .with('healthy', () => ({
        iconClass: 'fd fd-circle-check-big text-success-500',
        statusColor: 'bg-green-500',
        severity: 0,
        barColor: (h: boolean) => (h ? 'fill-green-600' : 'fill-green-500'),
        statusStyle: { bg: 'bg-green-500', text: 'text-green-500', label: 'Operational' }
      }))
      .exhaustive()

  // useInterval's $state(f()) is the initial fetch; subsequent ticks are periodic polls.
  useInterval(() => refreshEvents(pipelineName), 30_000)

  // Fires only on pipeline name changes. The first run is skipped because useInterval
  // already fired the initial fetch via $state(f()) before any effects run.
  let nameEffectInitialized = false
  $effect(() => {
    const name = pipelineName
    if (!nameEffectInitialized) {
      nameEffectInitialized = true
      return
    }
    events = null
    lastUpdated = null
    refreshEvents(name)
  })

  const categorizedEvents = $derived.by(() => categorizePipelineEvents(events ?? []))

  /**
   * Events with a `null` classification ("no data") never reach the timeline —
   * the bar for those buckets is rendered as the default "no data" gray.
   */
  const timelineEvents = $derived.by((): TimelineEvent<PipelineHealthStatus>[] =>
    categorizedEvents
      .filter((e): e is typeof e & { type: PipelineHealthStatus } => e.type !== null)
      .map((e) => ({
        timestamp: e.timestamp,
        type: e.type,
        description: ''
      }))
  )

  const healthWindowHours = 72

  const lastTimestamp = (es: PipelineMonitorEventSelectedInfo[] | null) =>
    ceilToHour(
      es?.length ? dateMax(new Date(es[0].recorded_at), new ServerDate()) : new ServerDate()
    )
  const firstTimestamp = (es: PipelineMonitorEventSelectedInfo[] | null) =>
    new Date(lastTimestamp(es).getTime() - healthWindowHours * 60 * 60 * 1000)

  const groupedEvents = $derived.by(() => groupPipelineEvents(categorizedEvents))

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

    const eventsInRange = categorizedEvents.filter(
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
  <div class="bg-white-dark flex h-full flex-1 flex-col gap-4 overflow-hidden rounded p-2">
    <StatusTimeline
      bind:this={timelineRef}
      label="Pipeline status"
      events={timelineEvents}
      startAt={firstTimestamp(events)}
      endAt={lastTimestamp(events)}
      unitDurationMs={60 * 60 * 1000}
      class="flex flex-col gap-2"
      onBarClick={handleBarClick}
      legend={['idle', 'transitioning', 'healthy', 'unhealthy', 'major_issue']}
      updatedAt={lastUpdated}
      getSeverity={(type) => pipelineStatus(type).severity}
      getBarColor={(type, h) => pipelineStatus(type).barColor(h)}
      getStatusStyle={(type) => pipelineStatus(type).statusStyle}
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
        getIconClass={(type) => pipelineStatus(type).iconClass}
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
        getStatusColor={(type) => pipelineStatus(type).statusColor}
      ></HealthEventList>
    {/if}
  </Drawer>
</div>
