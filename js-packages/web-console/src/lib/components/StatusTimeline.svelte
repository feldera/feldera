<script lang="ts">
  import { nonNull } from '$lib/functions/common/function'
  import { formatDateTime } from '$lib/functions/format'

  export type EventType = 'healthy' | 'unhealthy' | 'major_issue'

  interface TimelineEvent {
    timestamp: Date
    type: EventType
    description: string
  }

  export interface TimelineGroup {
    startTime: number
    endTime: number
    status: EventType
    events: TimelineEvent[]
  }

  interface Props {
    /**
     * The list of events to display
     */
    events: TimelineEvent[]
    /**
     * The timestamp of the beginning of the timeline
     */
    startAt: Date
    /**
     * The timestamp of the end of the timeline
     */
    endAt: Date
    /**
     * The duration of a single timeline item in milliseconds
     */
    unitDurationMs: number
    /**
     * Container element CSS class
     */
    class?: string
    /**
     * Callback of a timeline item being clicked, which includes information about all events that are relevant to the item
     */
    onBarClick?: (group: TimelineGroup) => void
  }

  let {
    events,
    startAt,
    endAt,
    unitDurationMs,
    class: className = '',
    onBarClick
  }: Props = $props()

  // Timeline display configuration
  const TIMELINE_HEIGHT = 24
  const TOP_PADDING = 16 // Reserved space for bar expansion
  const BAR_TO_GAP_RATIO = 4 // bars are 2x wider than gaps

  // Group events into time buckets
  const groups = $derived.by(() => {
    const startTime = startAt.getTime()
    const endTime = endAt.getTime()
    const numGroups = Math.ceil((endTime - startTime) / unitDurationMs)

    const groupsArray: (TimelineGroup | null)[] = new Array(numGroups).fill(null)
    const groupsMap = new Map<number, TimelineGroup>()

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
          status: 'healthy',
          events: []
        }
        groupsMap.set(groupIndex, group)
        groupsArray[groupIndex] = group
      }

      group.events.push(event)
      // Update group status based on event severity
      if (event.type === 'major_issue') {
        group.status = 'major_issue'
      } else if (event.type === 'unhealthy' && group.status !== 'major_issue') {
        group.status = 'unhealthy'
      }
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

  function getBarColor(status: TimelineGroup['status'], index: number): string {
    // If this bar is selected, use darker shade
    const isSelected = selectedBarIndex === index

    switch (status) {
      case 'major_issue':
        return isSelected ? 'fill-red-600' : 'fill-red-500'
      case 'unhealthy':
        return isSelected ? 'fill-yellow-600' : 'fill-yellow-500'
      case 'healthy':
      default:
        return isSelected ? 'fill-green-600' : 'fill-green-500'
    }
  }

  function getBarX(index: number): number {
    return index * (barWidth + gapWidth)
  }

  function getBarId(index: number): string {
    return `timeline-bar-${index}`
  }

  // Track selected bar
  let selectedBarIndex = $state<number | null>(null)

  function handleBarHover(index: number) {
    selectedBarIndex = index
  }

  function handleBarLeave() {
    selectedBarIndex = null
  }

  function getBarHeight(index: number): number {
    return selectedBarIndex === index ? TIMELINE_HEIGHT + TOP_PADDING : TIMELINE_HEIGHT
  }

  function getBarY(index: number): number {
    // Normal bars start at TOP_PADDING, active bars start at 0
    return selectedBarIndex === index ? 0 : TOP_PADDING
  }

  const TOTAL_SVG_HEIGHT = TIMELINE_HEIGHT + TOP_PADDING
</script>

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
        <rect
          id={getBarId(index)}
          x="{getBarX(index)}%"
          y={getBarY(index)}
          width="{barWidth}%"
          height={getBarHeight(index)}
          class="{getBarColor(
            group.status,
            index
          )} cursor-pointer transition-all outline-none focus:outline-none"
          role="button"
          tabindex="0"
          onmouseenter={() => handleBarHover(index)}
          onmouseleave={handleBarLeave}
          onclick={() => onBarClick?.(group)}
          onkeydown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              e.preventDefault()
              onBarClick?.(group)
            }
          }}
        />
      {/if}
    {/each}
  </svg>

  <div class="flex flex-wrap justify-between">
    <div class="text-nowrap">
      {#if nonNull(selectedBarIndex) && groups[selectedBarIndex] !== null}
        {@const group = groups[selectedBarIndex]!}
        {formatDateTime({ ms: group.startTime })} - {formatDateTime({
          ms: group.endTime
        })}
      {:else}
        {formatDateTime(startAt)} - {formatDateTime(endAt)}
      {/if}
    </div>
    <div class="flex items-center gap-4 text-xs">
      <div class="flex items-center gap-1.5">
        <div class="h-3 w-3 rounded-sm bg-green-500"></div>
        <span class="text-surface-600-300">Operational</span>
      </div>
      <div class="flex items-center gap-1.5">
        <div class="h-3 w-3 rounded-sm bg-yellow-500"></div>
        <span class="text-surface-600-300">Partial Issue</span>
      </div>
      <div class="flex items-center gap-1.5">
        <div class="h-3 w-3 rounded-sm bg-red-500"></div>
        <span class="text-surface-600-300">Major Issue</span>
      </div>
    </div>
  </div>
</div>
