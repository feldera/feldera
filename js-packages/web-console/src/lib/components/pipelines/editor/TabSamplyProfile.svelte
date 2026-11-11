<script lang="ts" module>
  export { label as Label }

  import { Input } from 'flowbite-svelte'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { type ExtendedPipeline, getSamplyProfile } from '#lib/services/pipelineManager.ts'

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
  import { onDestroy, untrack } from 'svelte'
  import { isPipelineInteractive } from '#lib/functions/pipelines/status.ts'
  import WarningBanner from '#lib/components/pipelines/editor/WarningBanner.svelte'
  import GenericDialog from '#lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '#lib/compositions/layout/useGlobalDialog.svelte.ts'
  import { useDownloadProgress } from '#lib/compositions/useDownloadProgress.svelte.ts'
  import { useToast } from '#lib/compositions/useToastNotification.ts'
  import { triggerFileDownload } from '#lib/services/browser.ts'
  import DownloadProgressDisplay from '#lib/components/dialogs/DownloadProgressDisplay.svelte'

  let {
    pipeline,
    deleted = false
  }: { pipeline: { current: ExtendedPipeline }; deleted?: boolean } = $props()

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
  // Keep equal to `SamplyProfileParams::MAX_DURATION_SECS` in `feldera-types`.
  const maxDuration = 300

  let pipelineNotRunning = $derived(!isPipelineInteractive(pipeline.current.status))
  let disabled = $derived(deleted || pipelineNotRunning || profilingNotEnabledMessage !== null)

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

  // The small countdown ring: the share of the collection time still to go.
  const ringLength = 2 * Math.PI * 8
  const remainingFraction = $derived(
    duration > 1 ? Math.min(1, Math.max(0, progressValue / Math.log10(duration))) : 0
  )

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
    isDownloading = true
    downloadProgress.reset()
    downloadCancelFn = null

    // Show dialog immediately so the indeterminate animation plays while the request is pending
    globalDialog.dialog = downloadDialog

    try {
      const result = await api.getSamplyProfile(
        pipeline.current.name,
        latest,
        downloadProgress.onProgress
      )
      if (result === null) {
        // There is no profile to download
        globalDialog.dialog = null
        hasAvailableProfile = false
        return
      }
      if ('expectedInSeconds' in result) {
        // Profile is still being collected, close the dialog and update countdown
        globalDialog.dialog = null
        const now = Date.now()
        expectedCompletion = now + result.expectedInSeconds * 1000
        if (!isCollecting) {
          isCollecting = true
          profileReady = false
          startTime = now
          startCountdown()
        }
        return
      }

      // If user dismissed the dialog while the request was pending, stop
      if (!isDownloading) {
        result.cancel()
        return
      }

      downloadCancelFn = result.cancel

      const download = await result.downloadPromise
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

      if (result === null) {
        // No profile was collected yet
        hasAvailableProfile = false
      } else if ('expectedInSeconds' in result) {
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
  {#if profilingNotEnabledMessage || pipelineNotRunning}
    <!-- Inset from the tabs and the panel edges, with all four corners rounded. -->
    <div class="px-4 pt-4">
      {#if profilingNotEnabledMessage}
        <WarningBanner class="rounded!">{profilingNotEnabledMessage}</WarningBanner>
      {:else}
        <WarningBanner class="rounded!">Start the pipeline to collect the profile</WarningBanner>
      {/if}
    </div>
  {/if}
  <div class="flex flex-wrap items-center gap-6 px-4 pt-6 pb-4">
    <div class="flex flex-col gap-3">
      <span class="text-surface-600-400">Select Samply profile duration</span>
      <div class="flex flex-wrap items-center gap-2">
        {#each [30, 60, 120, 300] as time}
          <button
            class="btn h-8 min-w-14 px-3 {time === duration
              ? 'preset-outlined-primary-500'
              : 'preset-tonal-surface'}"
            onclick={() => {
              duration = time
            }}
            disabled={isCollecting}
          >
            {formatTime(time)}
          </button>
        {/each}
        <label class="flex items-center gap-2">
          <Input
            type="number"
            class="input h-8 w-24 text-base"
            bind:value={duration}
            min={minDuration}
            max={maxDuration}
            disabled={isCollecting}
          />
          <span class="text-surface-600-400">seconds</span>
        </label>
      </div>
      <div class="flex flex-wrap gap-2 pt-1">
        <button
          class="btn h-8 preset-filled-primary-500 px-4"
          onclick={handleCollectProfile}
          disabled={disabled || isCollecting}
        >
          {isCollecting ? `Collecting (${displayTime})` : 'Collect profile'}
        </button>
        {#if isCollecting}
          <!-- A small countdown ring beside the button; it empties as the collection runs. -->
          <svg
            class="h-5 w-5 shrink-0 -rotate-90 self-center"
            viewBox="0 0 20 20"
            aria-hidden="true"
          >
            <circle
              cx="10"
              cy="10"
              r="8"
              fill="none"
              stroke-width="2.5"
              class="stroke-surface-200-800"
            />
            <circle
              cx="10"
              cy="10"
              r="8"
              fill="none"
              stroke-width="2.5"
              stroke-linecap="round"
              class="stroke-surface-950-50 transition-[stroke-dashoffset] duration-1000 ease-linear"
              stroke-dasharray={ringLength}
              stroke-dashoffset={ringLength * (1 - remainingFraction)}
            />
          </svg>
        {/if}
        {#if hasAvailableProfile}
          <button
            class="btn h-8 preset-tonal-surface px-4"
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
      </div>
    </div>
  </div>
</div>

{#snippet downloadDialog()}
  <GenericDialog
    content={{
      title: 'Downloading Samply Profile',
      onSuccess: { name: 'Download', callback: () => {} },
      onCancel: {
        callback: () => {
          downloadCancelFn?.()
          isDownloading = false
        }
      }
    }}
    disabled={true}
  >
    <DownloadProgressDisplay progress={downloadProgress} label="Downloading profile..." />
  </GenericDialog>
{/snippet}
