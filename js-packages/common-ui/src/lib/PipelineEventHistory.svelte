<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { partition } from 'common-lib/array'
  import { ceilToHour, dateMax } from 'common-lib/date'
  import {
    categorizePipelineEvents,
    formatPipelineEventDescription,
    groupPipelineEvents,
    type PipelineEventTag,
    type PipelineHealthBucket,
    type PipelineHealthStatus,
    type PipelineMonitorEvent
  } from './pipelineHealth'
  import Drawer from './Drawer.svelte'
  import EventLogList from './EventLogList.svelte'
  import HealthEventList from './HealthEventList.svelte'
  import StatusTimeline, { type TimelineEvent, type TimelineGroup } from './StatusTimeline.svelte'

  interface Props {
    /** Pipeline monitor events, newest first, or `null` before they are loaded. */
    events: PipelineMonitorEvent[] | null
    /** Shows a progress bar above the incident list. */
    loading: boolean
    /** The end of the observed period. This is the current time for a live pipeline, or the time
     *  when the support bundle was collected. */
    now: Date
    /** Text after the status in the timeline header, e.g. "updated 5m ago". */
    updatedText?: string | null
    /** Returns one event with all its detail fields. By default, finds the event in `events`. */
    loadEvent?: (eventId: string) => Promise<PipelineMonitorEvent | null | undefined>
    /** localStorage key to persist the size of the details drawer. */
    localStorageKey?: string
  }

  let {
    events,
    loading,
    now,
    updatedText = null,
    loadEvent = async (eventId) => events?.find((e) => e.event_id === eventId),
    localStorageKey
  }: Props = $props()

  const categorizedEvents = $derived(categorizePipelineEvents(events ?? []))

  // The timeline gets only the classified events, so a bar with only unclassified events shows
  // the "no data" color.
  const timelineEvents = $derived(
    categorizedEvents
      .filter((e): e is typeof e & { type: PipelineHealthStatus } => e.type !== null)
      .map(
        (e): TimelineEvent<PipelineHealthStatus> => ({
          timestamp: e.timestamp,
          type: e.type,
          description: ''
        })
      )
  )

  const healthWindowHours = 72
  // The newest event can be ahead of `now` when the browser clock is behind the server clock.
  // The last bar covers a full hour.
  const endAt = $derived(
    ceilToHour(events?.length ? dateMax(new Date(events[0].recorded_at), now) : now)
  )
  const startAt = $derived(new Date(endAt.getTime() - healthWindowHours * 60 * 60 * 1000))

  const splitEvents = $derived.by(() => {
    const [unresolved, previous] = partition(
      groupPipelineEvents(categorizedEvents),
      (e) => e.active
    )
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

  // The component whose selection the details drawer shows, so that the drawer's arrows step
  // through timeline bars or through incidents.
  let activeComponent: 'timeline' | 'incident' | null = $state(null)

  const loadEventDetail = async (eventId: string) => {
    const e = await loadEvent(eventId)
    if (!e) {
      return null
    }
    return {
      timestamp: new Date(e.recorded_at),
      description: formatPipelineEventDescription(e)
    }
  }

  function handleBarClick(group: TimelineGroup<PipelineHealthStatus>) {
    activeComponent = 'timeline'
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

    selectedEventTimestamp = {
      tag: 'pipeline',
      from: new Date(group.startTime),
      to: new Date(group.endTime)
    }
    selectedEvent = {
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
  }

  function handleEventSelected(eventBucket: PipelineHealthBucket) {
    activeComponent = 'incident'
    selectedEventTimestamp = {
      tag: eventBucket.tag,
      from: eventBucket.timestampFrom,
      to: eventBucket.timestampTo
    }
    selectedEvent = eventBucket
  }

  function navigatePrevious() {
    if (activeComponent === 'timeline') {
      timelineRef?.selectPreviousBucket()
    } else if (activeComponent === 'incident') {
      eventLogListRef?.selectPreviousIncident()
    }
  }

  function navigateNext() {
    if (activeComponent === 'timeline') {
      timelineRef?.selectNextBucket()
    } else if (activeComponent === 'incident') {
      eventLogListRef?.selectNextIncident()
    }
  }

  const activeNavigationState = $derived(
    activeComponent === 'timeline'
      ? (timelineRef?.getNavigationState() ?? null)
      : activeComponent === 'incident'
        ? (eventLogListRef?.getNavigationState() ?? null)
        : null
  )

  const canNavigatePrevious = $derived(activeNavigationState && activeNavigationState !== 'start')
  const canNavigateNext = $derived(activeNavigationState && activeNavigationState !== 'end')

  function closeDrawer() {
    selectedEvent = null
    selectedEventTimestamp = null
  }
</script>

<Drawer
  open={!!selectedEvent}
  side="right"
  inlineClass="rounded pt-4 pl-4"
  onClose={closeDrawer}
  {localStorageKey}
>
  {#snippet main()}
    <div class="bg-white-dark flex h-full flex-1 flex-col gap-4 overflow-hidden rounded p-2">
      <StatusTimeline
        bind:this={timelineRef}
        label="Pipeline status"
        events={timelineEvents}
        {startAt}
        {endAt}
        unitDurationMs={60 * 60 * 1000}
        class="flex flex-col gap-2"
        onBarClick={handleBarClick}
        legend={['idle', 'transitioning', 'healthy', 'unhealthy', 'major_issue']}
        {updatedText}
        selectedBars={selectedEventTimestamp
          ? { from: selectedEventTimestamp.from, to: selectedEventTimestamp.to }
          : null}
      ></StatusTimeline>

      {#if loading}
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
  {/snippet}
  {#if selectedEvent}
    <HealthEventList
      eventParts={selectedEvent}
      {loadEventDetail}
      onClose={closeDrawer}
      onNavigatePrevious={canNavigatePrevious ? navigatePrevious : undefined}
      onNavigateNext={canNavigateNext ? navigateNext : undefined}
    ></HealthEventList>
  {/if}
</Drawer>
