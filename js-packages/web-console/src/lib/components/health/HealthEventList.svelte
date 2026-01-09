<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { slide } from 'svelte/transition'
  import { match } from 'ts-pattern'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { formatDateTime } from '$lib/functions/format'
  import type { HealthEventBucket } from '$lib/functions/pipelines/health'

  let { eventParts, onClose }: { eventParts: HealthEventBucket; onClose: () => void } = $props()

  const api = usePipelineManager()

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
    api.getClusterEvent(event.id).then(
      (e) => {
        if (!e) {
          loadingStates[index] = false
          return
        }
        if (eventParts === null) {
          // Handle the case when the component was unmounted before the request completed
          return
        }
        fullEvents[index] = {
          timestamp: new Date(e.recorded_at),
          description: match(eventParts.tag)
            .with('api', () => (e.api_self_info || '') + '\n' + (e.api_resources_info || ''))
            .with(
              'compiler',
              () => (e.compiler_self_info || '') + '\n' + (e.compiler_resources_info || '')
            )
            .with(
              'runner',
              () => (e.runner_self_info || '') + '\n' + (e.runner_resources_info || '')
            )
            .exhaustive()
        }
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
      }
    }
    window.addEventListener('keydown', handleKeydown)
    return () => {
      window.removeEventListener('keydown', handleKeydown)
    }
  })
</script>

<!-- TODO: add buttons to navigate forward and backward between time intervals -
whether between hours when selected through StatusTimeline,
or between incidents when selected through from EventLogList -->

<div class="h-full overflow-hidden">
  <div class="flex flex-col gap-4 overflow-y-auto overflow-x-hidden h-full scrollbar">
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

      <div class="text-surface-600-300 pt-4 text-sm">
        {formatDateTime(eventParts.timestampFrom)} - {formatDateTime(eventParts.timestampTo)}
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
                {formatDateTime(event.timestamp)}
              </span>
            </div>
          {/snippet}
          {#snippet content()}
            {#if fullEvents[i]}
              <div transition:slide={{ duration: 150 }} class="px-2 pb-2">
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
          {/snippet}
        </InlineDropdown>
      {/each}
    </div>
  </div>
</div>
