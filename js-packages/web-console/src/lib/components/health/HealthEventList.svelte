<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { slide } from 'svelte/transition'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { formatDateTime } from '$lib/functions/format'
  import type { HealthEventBucket } from '$lib/functions/pipelines/health'

  let {
    eventParts,
    onClose,
    onNavigatePrevious,
    onNavigateNext,
    loadEventDetail
  }: {
    eventParts: HealthEventBucket
    onClose: () => void
    onNavigatePrevious?: () => void
    onNavigateNext?: () => void
    loadEventDetail: (eventId: string) => Promise<{ timestamp: Date; description: string } | null>
  } = $props()

  // Track loaded event details and loading states
  let fullEvents: (null | {
    timestamp: Date
    description: string
  })[] = $state([])

  let loadingStates = $state<boolean[]>([])
  let openStates = $state<boolean[]>([])

  // Reset state when eventParts changes
  $effect.pre(() => {
    eventParts
    fullEvents = Array(eventParts.events.length).fill(null)
    loadingStates = Array(eventParts.events.length).fill(false)
    openStates = Array(eventParts.events.length)
      .fill(false)
      .map((_, i) => i === 0)
  })

  // Load event details on-demand when dropdown is opened
  function loadEvent(index: number) {
    if (fullEvents[index] !== null || loadingStates[index]) {
      return
    }

    const event = eventParts.events[index]
    if (!event) {
      return
    }

    loadingStates[index] = true
    loadEventDetail(event.id).then(
      (detail) => {
        if (!detail) {
          loadingStates[index] = false
          return
        }
        fullEvents[index] = detail
        loadingStates[index] = false
      },
      () => {
        loadingStates[index] = false
      }
    )
  }

  // Load event when dropdown is opened
  $effect(() => {
    openStates.forEach((isOpen, index) => {
      if (isOpen) {
        loadEvent(index)
      }
    })
  })

  // Get status color based on event type
  function getStatusColor(type: string): string {
    switch (type) {
      case 'major_issue':
        return 'bg-red-500'
      case 'unhealthy':
        return 'bg-yellow-500'
      case 'healthy':
      default:
        return 'bg-green-500'
    }
  }

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
  <div class="scrollbar flex h-full flex-col gap-4 overflow-x-hidden overflow-y-auto">
    <div class="bg-white-dark sticky -top-4 z-10 -mt-4 pt-4">
      <div class="flex w-full flex-nowrap items-center">
        <span class="text-2xl font-semibold">
          {eventParts.title}
        </span>
        <button
          class="fd fd-x -m-1 ml-auto btn-icon text-[24px]"
          onclick={onClose}
          aria-label="Close event details"
        ></button>
      </div>

      <div class="flex flex-nowrap items-center gap-2">
        <div class="w-66 text-sm">
          {formatDateTime(eventParts.timestampFrom)} - {formatDateTime(eventParts.timestampTo)}
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
        <InlineDropdown bind:open={openStates[i]}>
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
              <div class="h-3 w-3 rounded-full {getStatusColor(event.status)}"></div>
              <span class="text-sm">
                {formatDateTime(event.timestamp, 'h:mm:ss A')}
              </span>
              {#if event.thumb}
                <span>
                  {event.thumb}
                </span>
              {/if}
            </div>
          {/snippet}
          {#snippet content()}
            <div transition:slide={{ duration: 150 }}>
              {#if fullEvents[i]}
                <div class="px-2 pb-2">
                  <div class="rounded font-dm-mono text-sm whitespace-pre-wrap">
                    {fullEvents[i]?.description || ''}
                  </div>
                </div>
              {:else if loadingStates[i]}
                <Progress class="-mt-2 -mb-1 h-1" value={null}>
                  <Progress.Track>
                    <Progress.Range class="bg-primary-500" />
                  </Progress.Track>
                </Progress>
              {/if}
            </div>
          {/snippet}
        </InlineDropdown>
      {/each}
    </div>
  </div>
</div>
