<script lang="ts" generics="S extends HealthStatus, T extends string">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { formatDateTime, formatDateTimeRange } from 'common-lib/format'
  import type { HealthEventBucket, HealthStatus } from './healthEvents'
  import { slide } from 'svelte/transition'
  import { healthStatusColors } from './healthStatus'
  import InlineDropdown from './InlineDropdown.svelte'

  type Bucket = HealthEventBucket<S, T>
  type EventDetail = { timestamp: Date; description: string }

  let {
    eventParts,
    loadEventDetail,
    onClose,
    onNavigatePrevious,
    onNavigateNext
  }: {
    eventParts: Bucket
    loadEventDetail: (eventId: string, bucket: Bucket) => Promise<EventDetail | null>
    onClose: () => void
    onNavigatePrevious?: () => void
    onNavigateNext?: () => void
  } = $props()

  type DetailLoad =
    | { state: 'idle' }
    | { state: 'loading' }
    | { state: 'loaded'; detail: EventDetail }
    | { state: 'missing' }
    | { state: 'failed' }

  let detailLoads = $state<DetailLoad[]>([])
  let openStates = $state<boolean[]>([])
  // Increments when `eventParts` changes, so that a reply for the previous `eventParts` is
  // ignored instead of being written to the event at the same index of the new one.
  let generation = 0

  $effect.pre(() => {
    eventParts
    generation++
    detailLoads = eventParts.events.map(() => ({ state: 'idle' }))
    openStates = eventParts.events.map((_, i) => i === 0)
  })

  // Only an idle event starts a load. The effect below runs again after each load, so loading a
  // missing or failed event again here would retry it at once and without end.
  function loadEvent(index: number) {
    const event = eventParts.events[index]
    if (!event || detailLoads[index]?.state !== 'idle') {
      return
    }
    const requestGeneration = generation
    const setLoad = (load: DetailLoad) => {
      if (requestGeneration === generation) {
        detailLoads[index] = load
      }
    }
    setLoad({ state: 'loading' })
    loadEventDetail(event.id, eventParts).then(
      (detail) => setLoad(detail ? { state: 'loaded', detail } : { state: 'missing' }),
      () => setLoad({ state: 'failed' })
    )
  }

  // Closing a failed event makes it idle, so that opening it again retries the load.
  function setOpen(index: number, isOpen: boolean) {
    openStates[index] = isOpen
    if (!isOpen && detailLoads[index]?.state === 'failed') {
      detailLoads[index] = { state: 'idle' }
    }
  }

  $effect(() => {
    openStates.forEach((isOpen, index) => {
      if (isOpen) {
        loadEvent(index)
      }
    })
  })

  $effect(() => {
    const handleKeydown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose()
      } else if (e.key === 'ArrowLeft' && onNavigatePrevious) {
        e.preventDefault()
        onNavigatePrevious()
      } else if (e.key === 'ArrowRight' && onNavigateNext) {
        e.preventDefault()
        onNavigateNext()
      }
    }
    window.addEventListener('keydown', handleKeydown)
    return () => {
      window.removeEventListener('keydown', handleKeydown)
    }
  })
</script>

<div class="h-full overflow-hidden">
  <div class="scrollbar flex h-full flex-col gap-4 overflow-x-hidden overflow-y-auto pr-2">
    <div class="bg-white-dark sticky -top-4 z-10 -mt-4 pt-4">
      <div class="flex w-full flex-nowrap items-center">
        <span class="text-xl font-semibold">
          {eventParts.title}
        </span>
        <button
          class="fd fd-x -m-1 ml-auto btn-icon text-[20px]"
          onclick={onClose}
          aria-label="Close event details"
        ></button>
      </div>

      <div class="flex flex-nowrap items-center gap-2">
        <div class="text-surface-600-300 w-66 text-sm">
          {formatDateTimeRange(eventParts.timestampFrom, eventParts.timestampTo)}
        </div>
        <button
          class="fd fd-chevron-left btn-icon hover:not-disabled:preset-tonal-surface"
          onclick={onNavigatePrevious}
          disabled={!onNavigatePrevious}
          aria-label="Previous event"
        >
        </button>
        <button
          class="fd fd-chevron-right btn-icon hover:not-disabled:preset-tonal-surface"
          onclick={onNavigateNext}
          disabled={!onNavigateNext}
          aria-label="Next event"
        >
        </button>
      </div>
    </div>

    <div class="flex flex-col gap-2">
      {#each eventParts.events as event, i}
        <InlineDropdown bind:open={() => openStates[i], (isOpen) => setOpen(i, isOpen)}>
          {#snippet header(open, toggle)}
            <div
              class="flex cursor-pointer items-center gap-3 rounded p-2 hover:bg-surface-100-900/50"
              onclick={toggle}
              role="button"
              tabindex="0"
              onkeydown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault()
                  toggle()
                }
              }}
            >
              <div
                class={'fd fd-chevron-down text-[16px] transition-transform ' +
                  (open ? 'rotate-180' : '')}
              ></div>
              <div class="h-3 w-3 rounded-full {healthStatusColors(event.status).healthDot}"></div>
              <span class="text-sm">
                {formatDateTime(event.timestamp, 'MMM D, YYYY h:mm:ss A')}
              </span>
            </div>
          {/snippet}
          {#snippet content()}
            {@const load = detailLoads[i]}
            {#if load?.state === 'loaded'}
              <div transition:slide={{ duration: 150 }} class="px-2 pb-2">
                <div class="rounded font-dm-mono whitespace-pre-wrap">
                  {load.detail.description}
                </div>
              </div>
            {:else if load?.state === 'missing'}
              <div transition:slide={{ duration: 150 }} class="px-2 pb-2 text-surface-600-400">
                This event has no details.
              </div>
            {:else if load?.state === 'failed'}
              <div transition:slide={{ duration: 150 }} class="px-2 pb-2 text-surface-600-400">
                The details of this event could not be loaded. Close and reopen the event to try
                again.
              </div>
            {:else if load?.state === 'loading'}
              <Progress class="-mt-2 -mb-1 h-1" value={null}>
                <Progress.Track>
                  <Progress.Range class="bg-primary-500" />
                </Progress.Track>
              </Progress>
            {/if}
          {/snippet}
        </InlineDropdown>
      {/each}
    </div>
  </div>
</div>
