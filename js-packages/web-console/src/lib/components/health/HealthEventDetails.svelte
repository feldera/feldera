<script lang="ts">
  import { match } from 'ts-pattern'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { formatDateTime } from '$lib/functions/format'
  import type { HealthEventParts } from '$lib/functions/pipelines/health'
  import { untrack } from 'svelte'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

  let { eventParts, onClose }: { eventParts: HealthEventParts; onClose: () => void } = $props()
  let fullEvents: (null | {
    timestamp: Date
    description: string
  })[] = $state([])

  const api = usePipelineManager()
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
      api.getClusterEvent(event.id).then(
        (e) => {
          fullEvents[currentPage] = {
            timestamp: new Date(e.recorded_at),
            description: match(eventParts.tag)
              .with('api', () => e.api_self_info + '\n' + e.api_resources_info)
              .with('compiler', () => e.compiler_self_info + '\n' + e.compiler_resources_info)
              .with('runner', () => e.runner_self_info + '\n' + e.runner_resources_info)
              .exhaustive()
          }
          loadingEvents = false
        },
        () => {
          loadingEvents = false
        }
      )
    })
  })

  const tagLabel = {
    api: 'API server incident',
    compiler: 'Compiler server incident',
    runner: 'Kubernetes runner incident'
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

<div class="flex flex-col gap-4">
  <div class="flex w-full flex-nowrap">
    <span class="text-2xl font-semibold">
      {tagLabel[eventParts.tag]}
    </span>
    <button
      class="fd fd-x -m-1 ml-auto btn-icon text-[24px]"
      onclick={onClose}
      aria-label="Confirm dangerous action"
    ></button>
  </div>

  <span>
    {formatDateTime(eventParts.timestampFrom)} - {formatDateTime(eventParts.timestampTo)}
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
  {:else}
    <Progress
      class="h-1"
      value={null}
    >
      <Progress.Track>
        <Progress.Range class="bg-primary-500" />
      </Progress.Track>
    </Progress>
  {/if}
</div>
