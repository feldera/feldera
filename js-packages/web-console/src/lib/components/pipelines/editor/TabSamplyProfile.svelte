<script lang="ts" module>
  export { label as Label }

  import { Input } from 'flowbite-svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { type ExtendedPipeline, getSamplyProfile } from '$lib/services/pipelineManager'

  // Helper to extract ProfilingNotEnabled error message
  export const getProfilingNotEnabledMessage = (error: unknown): string | null => {
    if (Error.isError(error)) {
      const cause = (error as { cause?: { error_code?: string; message?: string } }).cause
      if (cause?.error_code === 'ProfilingNotEnabled') {
        return cause.message ?? 'CPU profiling is not enabled.'
      }
    }
    return null
  }
</script>

<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { onDestroy, untrack } from 'svelte'
  import { isPipelineInteractive } from '$lib/functions/pipelines/status'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { humanSize } from '$lib/functions/common/string'
  import { triggerFileDownload } from '$lib/services/browser'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const toast = useToast()
  const downloadProgress = useDownloadProgress()

  // State
  let duration = $state(30)
  let isCollecting = $state(false)
  let startTime = $state<number | null>(null)
  let expectedCompletion = $state<number | null>(null)
  let profileReady = $state(false)
  let tickInterval: ReturnType<typeof setInterval> | null = null
  let tick = $state(0) // Force UI updates during countdown
  let isDownloading = $state(false)
  let downloadCancelFn: (() => void) | null = null
  let profilingNotEnabledMessage = $state<string | null>(null)
  let hasAvailableProfile = $state(false)

  const minDuration = 1
  const maxDuration = 3600

  let pipelineNotRunning = $derived(!isPipelineInteractive(pipeline.current.status))
  let disabled = $derived(pipelineNotRunning || profilingNotEnabledMessage !== null)

  // Time formatting: "Xm Ys" ignoring zeros
  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60)
    const secs = Math.floor(seconds % 60)
    if (mins === 0) return `${secs}s`
    if (secs === 0) return `${mins}m`
    return `${mins}m ${secs}s`
  }

  // Progress calculation
  const progressValue = $derived.by(() => {
    if (duration === null) {
      return 0
    }
    if (!isCollecting) {
      // Selection mode: logarithmic
      return Math.log10(duration)
    } else {
      // Collecting mode: linear countdown from log10(duration) to 0
      // Use tick to force recalculation
      tick
      if (startTime === null || expectedCompletion === null) return 0
      const now = Date.now()
      const remainingSeconds = Math.max(0, (expectedCompletion - now) / 1000)
      const logDuration = Math.log10(duration)
      // Linearly interpolate from logDuration (at start) to 0 (at end)
      return (remainingSeconds / duration) * logDuration
    }
  })

  const progressMax = $derived.by(() => {
    // Max is always the same to maintain consistent scale
    return Math.log10(maxDuration)
  })

  const displayTime = $derived.by(() => {
    if (!isCollecting) {
      return formatTime(duration)
    } else {
      // Use tick to force recalculation
      tick
      if (startTime === null || expectedCompletion === null) return formatTime(0)
      const now = Date.now()
      const remaining = Math.max(0, Math.ceil((expectedCompletion - now) / 1000))
      return formatTime(remaining)
    }
  })

  // Collect profile handler
  const handleCollectProfile = async () => {
    try {
      isCollecting = true
      profileReady = false
      startTime = Date.now()
      expectedCompletion = startTime + duration * 1000

      await api.collectSamplyProfile(pipeline.current.name, duration)

      // Start countdown timer
      startCountdown()
    } catch (error) {
      isCollecting = false
      startTime = null
      expectedCompletion = null
      profilingNotEnabledMessage ??= getProfilingNotEnabledMessage(error)
    }
  }

  const startCountdown = () => {
    stopCountdown()
    // Tick every second for UI updates
    tickInterval = setInterval(() => {
      tick++
      // Check if countdown is complete
      if (expectedCompletion && Date.now() >= expectedCompletion) {
        isCollecting = false
        profileReady = true
        hasAvailableProfile = true
        stopCountdown()
      }
    }, 1000)
  }

  const stopCountdown = () => {
    if (tickInterval) {
      clearInterval(tickInterval)
      tickInterval = null
    }
  }

  // Download profile handler
  const handleDownloadProfile = async (latest: boolean) => {
    try {
      isDownloading = true
      downloadProgress.reset()
      downloadCancelFn = null

      const result = await api.getSamplyProfile(
        pipeline.current.name,
        latest,
        downloadProgress.onProgress
      )
      if ('expectedInSeconds' in result) {
        // Profile is still being collected, update countdown with the server's expected time
        const now = Date.now()
        expectedCompletion = now + result.expectedInSeconds * 1000

        // Ensure collecting state is active
        if (!isCollecting) {
          isCollecting = true
          profileReady = false
          startTime = now
          startCountdown()
        }
        return
      }

      // Store cancel function
      downloadCancelFn = result.cancel

      // Show download dialog for actual download
      globalDialog.dialog = downloadDialog

      const download = await result.downloadPromise

      // Await the download to complete
      triggerFileDownload(download.filename, await download.dataPromise)

      globalDialog.dialog = null
    } catch (error) {
      globalDialog.dialog = null
      profilingNotEnabledMessage ??= getProfilingNotEnabledMessage(error)
    } finally {
      isDownloading = false
      downloadCancelFn = null
    }
  }

  // Check profile status when tab is opened
  const checkProfileStatus = async () => {
    try {
      const result = await getSamplyProfile(
        pipeline.current.name,
        true, // Use ?latest to check status
        undefined // No progress callback for status check
      )

      if ('expectedInSeconds' in result) {
        // Profile is currently being collected - set up countdown
        const now = Date.now()
        expectedCompletion = now + result.expectedInSeconds * 1000
        isCollecting = true
        profileReady = false
        startTime = now
        startCountdown()
      } else {
        // Profile is ready but we're just checking status - cancel the download
        hasAvailableProfile = true
        result.cancel()
      }
    } catch (error) {
      // Check if profiling is not enabled (permission error)
      profilingNotEnabledMessage ??= getProfilingNotEnabledMessage(error)
      // Silently fail - profile may not exist yet
      console.debug('No profile status available:', error)
    }
  }

  const resetState = () => {
    stopCountdown()
    duration = 30
    isCollecting = false
    startTime = null
    expectedCompletion = null
    profileReady = false
    hasAvailableProfile = false
    tick = 0
  }

  // Track previous values for detecting transitions
  let pipelineName = $derived(pipeline.current.name)
  let isInteractive = $derived(isPipelineInteractive(pipeline.current.status))

  // Check profile status when pipeline becomes interactive, reset download state when non-interactive
  // Refresh information when opening another pipeline
  $effect(() => {
    pipelineName
    if (isInteractive) {
      untrack(() => checkProfileStatus())
    } else {
      resetState()
    }
  })

  onDestroy(() => {
    stopCountdown()
    // Cancel any ongoing download
    if (downloadCancelFn) {
      downloadCancelFn()
    }
  })

  $effect.pre(() => {
    if (duration === null) {
      return
    }
    if (duration < minDuration) {
      duration = minDuration
      return
    }
    if (duration > maxDuration) {
      duration = maxDuration
      return
    }
  })
</script>

{#snippet label()}
  <span class=""> CPU Profile </span>
{/snippet}

<div class="flex h-full flex-col">
  {#if profilingNotEnabledMessage}
    <WarningBanner>{profilingNotEnabledMessage}</WarningBanner>
  {:else if pipelineNotRunning}
    <WarningBanner>Start the pipeline to collect the profile</WarningBanner>
  {/if}
  <div class="flex flex-nowrap gap-4 p-2">
    <Progress value={progressValue} class="relative w-fit items-center" max={progressMax}>
      <div class="absolute inset-0 flex items-center justify-center">
        <span>{displayTime}</span>
      </div>
      <Progress.Circle>
        <Progress.CircleTrack />
        <Progress.CircleRange />
      </Progress.Circle>
    </Progress>
    <div class="grid grid-cols-3 gap-2">
      <span class="col-span-3">Select Samply profile duration:</span>
      {#each [30, 60, 120, 300] as time}
        <button
          class="btn h-8 py-0! {time === duration
            ? '-mx-px preset-outlined-primary-500'
            : 'preset-tonal-surface'}"
          onclick={() => {
            duration = time
          }}
          disabled={isCollecting}
        >
          {formatTime(time)}
        </button>
      {/each}
      <Input
        type="number"
        class="col-span-2 input text-base"
        bind:value={duration}
        min={minDuration}
        max={maxDuration}
        disabled={isCollecting}
      />
    </div>
    <div class="flex flex-col justify-end gap-2">
      {#if hasAvailableProfile}
        <button
          class="btn preset-tonal-surface"
          {disabled}
          onclick={() => handleDownloadProfile(!isCollecting)}
        >
          {#if isCollecting}
            Download last profile
          {:else}
            Download profile
          {/if}
        </button>
      {/if}
      <button
        class="btn preset-tonal-surface"
        onclick={handleCollectProfile}
        disabled={disabled || isCollecting}
      >
        {isCollecting ? `Collecting (${displayTime})` : 'Collect profile'}
      </button>
    </div>
  </div>
</div>

{#snippet downloadDialog()}
  <GenericDialog
    onClose={() => {
      downloadCancelFn?.()
      globalDialog.dialog = null
      isDownloading = false
    }}
    confirmLabel="Download"
    onApply={() => {}}
    disabled={true}
  >
    {#snippet title()}
      Downloading Samply Profile
    {/snippet}
    <div class="flex flex-col items-center gap-3 py-4">
      <Progress class="h-1" value={downloadProgress.percent ?? 0} max={100}>
        <Progress.Track>
          <Progress.Range class="bg-primary-500" />
        </Progress.Track>
      </Progress>
      <div class="flex w-full justify-between gap-2">
        <span>Downloading profile...</span>
        {#if downloadProgress.percent}
          <span
            >{humanSize(downloadProgress.bytes.downloaded)} / {humanSize(
              downloadProgress.bytes.total
            )}</span
          >
        {/if}
      </div>
    </div>
  </GenericDialog>
{/snippet}
