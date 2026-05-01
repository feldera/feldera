<script lang="ts" generics="S extends BaseHealthStatus, T extends string">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { untrack } from 'svelte'
  import { formatDateTime, formatDateTimeRange } from '$lib/functions/format'
  import type { BaseHealthStatus, HealthEventBucket } from '$lib/functions/pipelines/health'

  type Bucket = HealthEventBucket<S, T>
  type EventDetail = { timestamp: Date; description: string }

  let {
    eventParts,
    loadEventDetail,
    onClose
  }: {
    eventParts: Bucket
    loadEventDetail: (eventId: string, bucket: Bucket) => Promise<EventDetail | null>
    onClose: () => void
  } = $props()

  let fullEvents: (null | EventDetail)[] = $state([])
  let loadingEvents = $state(false)

  let pageNum = $derived(eventParts.events.length)
  let currentPage = $state(0)
  const fullEvent = $derived(fullEvents[currentPage])

  // Reset fullEvents when eventParts changes
  $effect.pre(() => {
    eventParts
    untrack(() => {
      fullEvents = Array(eventParts.events.length).fill(null)
      currentPage = 0
    })
  })

  // Load current event on-demand
  $effect(() => {
    currentPage
    const event = eventParts.events[currentPage]
    if (!event || fullEvents[currentPage] !== null) {
      return
    }

    untrack(() => {
      loadingEvents = true
      loadEventDetail(event.id, eventParts).then(
        (detail) => {
          if (detail) {
            fullEvents[currentPage] = detail
          }
          loadingEvents = false
        },
        () => {
          loadingEvents = false
        }
      )
    })
  })

  $effect(() => {
    const handleKeydown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
      }
    }
    window.addEventListener('keydown', handleKeydown)
    return () => {
      window.removeEventListener('keydown', handleKeydown)
    }
  })
</script>

<div class="flex flex-col gap-4">
  <div class="flex w-full flex-nowrap">
    <span class="text-2xl font-semibold">
      {eventParts.title}
    </span>
    <button
      class="fd fd-x -m-1 ml-auto btn-icon text-[24px]"
      onclick={onClose}
      aria-label="Confirm dangerous action"
    ></button>
  </div>

  <span>
    {formatDateTimeRange(eventParts.timestampFrom, eventParts.timestampTo)}
  </span>

  <div class="flex flex-nowrap items-center gap-2">
    <button
      class="fd fd-chevron-left btn-icon hover:preset-tonal-surface"
      onclick={() => (currentPage = (currentPage - 1 + pageNum) % pageNum)}
      aria-label="Previous event"
    >
    </button>
    {currentPage + 1}/{pageNum}
    <button
      class="fd fd-chevron-right btn-icon hover:preset-tonal-surface"
      onclick={() => (currentPage = (currentPage + 1) % pageNum)}
      aria-label="Next event"
    >
    </button>
  </div>
  {formatDateTime(eventParts.events[currentPage].timestamp)}
  {#if fullEvent}
    <span class="font-dm-mono whitespace-pre-wrap">
      {fullEvent.description}
    </span>
  {:else if loadingEvents}
    <Progress class="h-1" value={null}>
      <Progress.Track>
        <Progress.Range class="bg-primary-500" />
      </Progress.Track>
    </Progress>
  {/if}
</div>
