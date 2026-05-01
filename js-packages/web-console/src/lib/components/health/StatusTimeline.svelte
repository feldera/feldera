<script lang="ts" module>
  /**
   * Statuses a status timeline can render. "No data" is represented by the
   * absence of an event for a bucket, not as a value of this union.
   */
  export type EventType = 'healthy' | 'unhealthy' | 'major_issue' | 'transitioning'

  export interface TimelineEvent<S extends EventType = EventType> {
    timestamp: Date
    type: S
    description: string
  }

  export interface TimelineGroup<S extends EventType = EventType> {
    startTime: number
    endTime: number
    status: S
    events: TimelineEvent<S>[]
  }
</script>

<script lang="ts" generics="S extends EventType">
  import { formatDateTime, formatDateTimeRange } from '$lib/functions/format'

  interface Props {
    /** The text label for the timeline */
    label: string
    /** The list of events to display */
    events: TimelineEvent<S>[]
    /** The timestamp of the beginning of the timeline */
    startAt: Date
    /** The timestamp of the end of the timeline */
    endAt: Date
    /** The duration of a single timeline item in milliseconds */
    unitDurationMs: number
    /** Container element CSS class */
    class?: string
    /** Callback of a timeline item being clicked */
    onBarClick?: (group: TimelineGroup<S>) => void
    /** Statuses to show in the legend. Pass an empty array to hide it. */
    legend: S[]
    /** Selected bar timestamp range */
    selectedBars?: { from: Date; to: Date } | null
  }

  let {
    label,
    events,
    startAt,
    endAt,
    unitDurationMs,
    class: className = '',
    onBarClick,
    legend,
    selectedBars = null
  }: Props = $props()

  // Timeline display configuration
  const TIMELINE_HEIGHT = 24
  const TOP_PADDING = 8 // Reserved space for bar expansion
  const BAR_TO_GAP_RATIO = 4 // bars are 2x wider than gaps

  // Group events into time buckets
  const groups = $derived.by(() => {
    const startTime = startAt.getTime()
    const endTime = endAt.getTime()
    const numGroups = Math.ceil((endTime - startTime) / unitDurationMs)

    const groupsArray: (TimelineGroup<S> | null)[] = new Array(numGroups).fill(null)
    const groupsMap = new Map<number, TimelineGroup<S>>()

    // Place events into appropriate groups
    for (const event of events) {
      const eventTime = event.timestamp.getTime()
      if (eventTime < startTime || eventTime > endTime) {
        continue
      }

      const groupIndex = Math.floor((eventTime - startTime) / unitDurationMs)
      let group = groupsMap.get(groupIndex)

      if (!group) {
        const groupStart = startTime + groupIndex * unitDurationMs
        const groupEnd = Math.min(groupStart + unitDurationMs, endTime)
        group = {
          startTime: groupStart,
          endTime: groupEnd,
          status: 'healthy' as S,
          events: []
        }
        groupsMap.set(groupIndex, group)
        groupsArray[groupIndex] = group
      }

      group.events.push(event)
      // Update group status based on event severity (highest severity wins)
      group.status = pickWorseStatus(group.status, event.type)
    }

    return groupsArray
  })

  // Calculate bar dimensions
  const barWidth = $derived.by(() => {
    const totalGroups = groups.length
    if (totalGroups === 0) return 0
    const barWidthCalc = (100 * BAR_TO_GAP_RATIO) / (totalGroups * (BAR_TO_GAP_RATIO + 1) - 1)
    return barWidthCalc
  })

  const gapWidth = $derived(barWidth / BAR_TO_GAP_RATIO)

  // Higher number = worse status. Used by both bar coloring and group rollup.
  const STATUS_SEVERITY: Record<EventType, number> = {
    healthy: 0,
    transitioning: 1,
    unhealthy: 2,
    major_issue: 3
  }

  function pickWorseStatus(a: S, b: S): S {
    return STATUS_SEVERITY[b] > STATUS_SEVERITY[a] ? b : a
  }

  function getBarColor(status: EventType, index: number): string {
    // If this bar is hovered or persistently selected, use darker shade
    const isHighlighted = hoveredBarIndex === index || selectedBarIndices.includes(index)

    switch (status) {
      case 'major_issue':
        return isHighlighted ? 'fill-red-600' : 'fill-red-500'
      case 'unhealthy':
        return isHighlighted ? 'fill-yellow-600' : 'fill-yellow-500'
      case 'transitioning':
        return isHighlighted ? 'fill-blue-600' : 'fill-blue-500'
      case 'healthy':
      default:
        return isHighlighted ? 'fill-green-600' : 'fill-green-500'
    }
  }

  function getStyle(status: EventType | 'no_data') {
    switch (status) {
      case 'major_issue':
        return {
          bg: 'bg-red-500',
          text: 'text-red-500',
          label: 'Major Issue'
        }
      case 'unhealthy':
        return {
          bg: 'bg-yellow-500',
          text: 'text-yellow-500',
          label: 'Service degradation'
        }
      case 'transitioning':
        return {
          bg: 'bg-blue-500',
          text: 'text-blue-500',
          label: 'Transitioning'
        }
      case 'no_data':
        return {
          bg: 'bg-surface-300-700',
          text: 'text-surface-300-700',
          label: 'No data'
        }
      case 'healthy':
      default:
        return {
          bg: 'bg-green-500',
          text: 'text-green-500',
          label: 'Operational'
        }
    }
  }

  const healthStatus = $derived(events.at(0)?.type)

  function getBarX(index: number): number {
    return index * (barWidth + gapWidth)
  }

  function getBarId(index: number): string {
    return `timeline-bar-${index}`
  }

  // Track hovered bar (for temporary hover effect)
  let hoveredBarIndex = $state<number | null>(null)

  // Calculate indices of selected bars based on selectedBars prop
  const selectedBarIndices = $derived.by(() => {
    if (!selectedBars) return []
    const startTime = startAt.getTime()
    const fromTime = selectedBars.from.getTime()
    const toTime = selectedBars.to.getTime()

    // Calculate start and end indices
    const startIndex = Math.floor((fromTime - startTime) / unitDurationMs)
    const endIndex = Math.ceil((toTime - startTime) / unitDurationMs)

    // Return array of indices in range
    const indices: number[] = []
    for (let i = startIndex; i < endIndex && i < groups.length; i++) {
      if (i >= 0) {
        indices.push(i)
      }
    }
    return indices
  })

  function handleBarHover(index: number) {
    hoveredBarIndex = index
  }

  function handleBarLeave() {
    hoveredBarIndex = null
  }

  function getBarHeight(index: number, clickable: boolean): number {
    const isActive = hoveredBarIndex === index || selectedBarIndices.includes(index)
    return isActive && clickable ? TIMELINE_HEIGHT + TOP_PADDING : TIMELINE_HEIGHT
  }

  function getBarY(index: number): number {
    // Normal bars start at TOP_PADDING, active bars start at 0
    const isActive = hoveredBarIndex === index || selectedBarIndices.includes(index)
    return isActive ? 0 : TOP_PADDING
  }

  const TOTAL_SVG_HEIGHT = TIMELINE_HEIGHT + TOP_PADDING

  // Compute the displayed time range based on highlighted or selected bars
  const highlightedTimeRange = $derived.by(() => {
    const startTime = startAt.getTime()
    const endTime = endAt.getTime()

    // Priority 1: Use hovered bar
    if (hoveredBarIndex !== null) {
      const rangeStart = startTime + hoveredBarIndex * unitDurationMs
      const rangeEnd = Math.min(rangeStart + unitDurationMs, endTime)
      return { from: rangeStart, to: rangeEnd }
    }

    // Priority 2: Use selected bar indices
    if (selectedBarIndices.length > 0) {
      const firstIndex = selectedBarIndices[0]
      const lastIndex = selectedBarIndices[selectedBarIndices.length - 1]
      const rangeStart = startTime + firstIndex * unitDurationMs
      const rangeEnd = Math.min(startTime + (lastIndex + 1) * unitDurationMs, endTime)
      return { from: rangeStart, to: rangeEnd }
    }

    // Default: No specific range selected
    return null
  })

  // Navigation methods for timeline buckets
  export function selectPreviousBucket(): boolean {
    if (!selectedBars || !onBarClick) return false

    const startTime = startAt.getTime()
    const currentIndex = Math.floor((selectedBars.from.getTime() - startTime) / unitDurationMs)

    // Find previous bucket with events
    for (let i = currentIndex - 1; i >= 0; i--) {
      const group = groups[i]
      if (group !== null) {
        onBarClick(group)
        return true
      }
    }
    return false
  }

  export function selectNextBucket(): boolean {
    if (!selectedBars || !onBarClick) return false

    const startTime = startAt.getTime()
    const currentIndex = Math.floor((selectedBars.from.getTime() - startTime) / unitDurationMs)

    // Find next bucket with events
    for (let i = currentIndex + 1; i < groups.length; i++) {
      const group = groups[i]
      if (group !== null) {
        onBarClick(group)
        return true
      }
    }
    return false
  }

  // Compute navigation state once
  const navigationState = $derived.by((): 'start' | 'middle' | 'end' | null => {
    if (!selectedBars) return null

    const startTime = startAt.getTime()
    const currentIndex = Math.floor((selectedBars.from.getTime() - startTime) / unitDurationMs)

    // Find if there's a previous bucket with events
    let hasPrevious = false
    for (let i = currentIndex - 1; i >= 0; i--) {
      if (groups[i] !== null) {
        hasPrevious = true
        break
      }
    }

    // Find if there's a next bucket with events
    let hasNext = false
    for (let i = currentIndex + 1; i < groups.length; i++) {
      if (groups[i] !== null) {
        hasNext = true
        break
      }
    }

    if (!hasPrevious && !hasNext) return null // Only one bucket
    if (!hasPrevious) return 'start'
    if (!hasNext) return 'end'
    return 'middle'
  })

  export const getNavigationState = () => navigationState
</script>

{#snippet timelineBar(
  index: number,
  colorClass: string,
  clickable: boolean,
  group: TimelineGroup<S> | null
)}
  <g
    id={getBarId(index)}
    role="button"
    tabindex="0"
    class="{clickable ? 'cursor-pointer' : ''} outline-none focus:outline-none"
    onmouseenter={() => handleBarHover(index)}
    onmouseleave={handleBarLeave}
    onclick={group && clickable ? () => onBarClick?.(group) : undefined}
    onkeydown={group && clickable
      ? (e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            onBarClick?.(group)
          }
        }
      : undefined}
  >
    <!-- Invisible interaction rect that includes the gap (right padding) -->
    <rect
      x="{getBarX(index)}%"
      y={getBarY(index)}
      width="{barWidth + gapWidth}%"
      height={getBarHeight(index, true)}
      class="fill-transparent"
    />
    <!-- Visible colored rect (visual appearance) -->
    <rect
      x="{getBarX(index)}%"
      y={getBarY(index)}
      width="{barWidth}%"
      height={getBarHeight(index, clickable)}
      class="{colorClass} pointer-events-none transition-all"
    />
  </g>
{/snippet}

<div class="flex flex-col">
  <div class="">
    {label}
    {#if healthStatus}
      {@const style = getStyle(healthStatus)} - <span class={style.text}>{style.label}</span>
    {/if}
  </div>
  <div class="relative {className}">
    <svg
      width="100%"
      height={TOTAL_SVG_HEIGHT}
      class=""
      viewBox="0 0 100 {TOTAL_SVG_HEIGHT}"
      preserveAspectRatio="none"
    >
      {#each groups as group, index}
        {#if group !== null}
          {@render timelineBar(index, getBarColor(group.status, index), true, group)}
        {:else}
          {@render timelineBar(index, 'fill-surface-300-700', false, null)}
        {/if}
      {/each}
    </svg>

    <div class="flex flex-wrap justify-between">
      <div class="text-nowrap text-surface-600-400">
        {#if highlightedTimeRange}
          {formatDateTimeRange(
            { ms: highlightedTimeRange.from },
            {
              ms: highlightedTimeRange.to
            }
          )}
        {:else}
          {formatDateTime(startAt)} - {formatDateTime(endAt)}
        {/if}
      </div>
      {#if legend.length}
        <div class="flex items-center gap-4 text-xs">
          {#each ['no_data' as const, ...legend] as status}
            {@const style = getStyle(status)}
            <div class="flex items-center gap-1.5">
              <div class="h-3 w-3 rounded-sm {style.bg}"></div>
              <span class="text-surface-600-300">{style.label}</span>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  </div>
</div>
