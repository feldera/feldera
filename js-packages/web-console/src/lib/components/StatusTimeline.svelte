<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
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
    events: TimelineEvent[]
    startAt: Date
    endAt: Date
    unitDurationMs: number // milliseconds
    class?: string
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

  // Timeline configuration
  const TIMELINE_HEIGHT = 32
  const TOP_PADDING = 16 // Reserved space for bar expansion
  const BAR_TO_GAP_RATIO = 4 // bars are 2x wider than gaps

  // Group events into time buckets
  const groups = $derived.by(() => {
    const startTime = startAt.getTime()
    const endTime = endAt.getTime()
    const numGroups = Math.ceil((endTime - startTime) / unitDurationMs)

    const groupsMap = new Map<number, TimelineGroup>()

    // Initialize all groups with 'healthy' status
    for (let i = 0; i < numGroups; i++) {
      const groupStart = startTime + i * unitDurationMs
      const groupEnd = Math.min(groupStart + unitDurationMs, endTime)
      groupsMap.set(i, {
        startTime: groupStart,
        endTime: groupEnd,
        status: 'healthy',
        events: []
      })
    }

    // Place events into appropriate groups
    for (const event of events) {
      const eventTime = event.timestamp.getTime()
      if (eventTime < startTime || eventTime > endTime) {
        continue
      }

      const groupIndex = Math.floor((eventTime - startTime) / unitDurationMs)
      const group = groupsMap.get(groupIndex)
      if (group) {
        group.events.push(event)
        // Update group status based on event severity
        if (event.type === 'major_issue') {
          group.status = 'major_issue'
        } else if (event.type === 'unhealthy' && group.status !== 'major_issue') {
          group.status = 'unhealthy'
        }
      }
    }

    return Array.from(groupsMap.values())
  })

  // Calculate bar dimensions
  const barWidth = $derived.by(() => {
    const totalGroups = groups.length
    if (totalGroups === 0) return 0
    // If bars are 2x wider than gaps: bar_width = 2 * gap_width
    // Total width = groups * bar_width + (groups - 1) * gap_width
    // Total width = groups * bar_width + (groups - 1) * bar_width/2
    // Total width = bar_width * (groups + (groups - 1)/2)
    // Total width = bar_width * (groups * 2 + groups - 1) / 2
    // bar_width = Total width * 2 / (groups * 3 - 1)
    const barWidthCalc = (100 / (totalGroups * 3 - 1)) * 2
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

  function formatDate(timestamp: number): string {
    return new Date(timestamp).toLocaleString()
  }

  function getBarId(index: number): string {
    return `timeline-bar-${index}`
  }

  // Track selected bar
  let selectedBarIndex = $state<number | null>(null)
  let tooltipOpen = $state(false)
  let activeGroup = $derived(selectedBarIndex !== null ? groups[selectedBarIndex] : null)

  function handleBarHover(index: number) {
    selectedBarIndex = index
    tooltipOpen = true
  }

  function handleBarLeave() {
    selectedBarIndex = null
    tooltipOpen = false
  }

  function getBarHeight(index: number): number {
    return selectedBarIndex === index ? TIMELINE_HEIGHT + TOP_PADDING : TIMELINE_HEIGHT
  }

  function getBarY(index: number): number {
    // Normal bars start at TOP_PADDING, active bars start at 0
    return selectedBarIndex === index ? 0 : TOP_PADDING
  }

  const TOTAL_SVG_HEIGHT = TIMELINE_HEIGHT + TOP_PADDING
  const triggeredById = $derived(selectedBarIndex !== null ? `#${getBarId(selectedBarIndex)}` : '')
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
    {/each}
  </svg>

  {#if triggeredById && activeGroup}
    <Tooltip
      open={tooltipOpen}
      triggeredBy={triggeredById}
      class="bg-white-dark z-50 max-w-sm rounded-lg p-3 shadow-lg"
    >
      <div class="mb-1 text-xs text-surface-600-400">
        {formatDate(activeGroup.startTime)} - {formatDate(activeGroup.endTime)}
      </div>
      {#if activeGroup.events.length === 0}
        <div class="text-sm font-medium text-green-600 dark:text-green-400">No issues reported</div>
      {:else}
        <div class="space-y-2">
          {#each activeGroup.events as event}
            <div class="text-sm">
              <div
                class="font-medium capitalize {event.type === 'major_issue'
                  ? 'text-red-600 dark:text-red-400'
                  : event.type === 'unhealthy'
                    ? 'text-yellow-600 dark:text-yellow-400'
                    : 'text-green-600 dark:text-green-400'}"
              >
                {event.type.replace('_', ' ')}
              </div>
              <div class="text-surface-700-200 mt-1 whitespace-pre-wrap">
                {event.description}
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </Tooltip>
  {/if}
  <div class="flex flex-wrap justify-between">
    <div class="text-nowrap">
      {#if nonNull(selectedBarIndex)}
        {formatDateTime({ ms: groups.at(selectedBarIndex)!.startTime })} - {formatDateTime({
          ms: groups.at(selectedBarIndex)!.endTime
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
