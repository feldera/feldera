<script lang="ts" module>
  let loadedPipelineName: string | null = null
  let getProfileData:
    | (() => {
        profile: JsonProfiles
        dataflow: Dataflow | undefined
        sources: string[] | undefined
      })
    | null = $state(null)
  let getProfileFiles: () => [Date, ZipItem[]][] = $state(() => [])
  let selectedProfile: Date | null = $state(null)

  export { label as Label }
</script>

<script lang="ts">
  import {
    ProfilerLayout,
    ProfileTimestampSelector,
    getSuitableProfiles,
    processProfileFiles,
    type ZipItem
  } from 'profiler-layout'
  import Popup from '$lib/components/common/Popup.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { enclosure, nonNull } from '$lib/functions/common/function'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { JsonProfiles, Dataflow, SourcePositionRange } from 'profiler-lib'
  import { untrack } from 'svelte'
  import { slide, fade } from 'svelte/transition'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { goto } from '$app/navigation'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  const api = usePipelineManager()

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    untrack(() => {
      if (getProfileData && loadedPipelineName !== pipelineName) {
        getProfileData = null
        loadedPipelineName = null
        getProfileFiles = () => []
      }
    })
  })

  const processZipBundle = async (zipData: Uint8Array, sourceName: string, message: string) => {
    downloadProgress.onProgress(1, 1)
    try {
      const suitableProfiles = getSuitableProfiles(zipData)
      if (suitableProfiles.length === 0) {
        errorMessage = message
        return false
      }

      loadedPipelineName = sourceName
      getProfileFiles = () => suitableProfiles
      selectedProfile = suitableProfiles.at(-1)![0] // Select most recent
      if (!selectedProfile || isNaN(selectedProfile.getTime())) {
        // Hopefully a redundant check for an obscure case where we did not pick a valid bundle
        errorMessage = `Unexpected error: unable to pick the latest profile in a support bundle. Inspect the bundle archive for integrity.`
        selectedProfile = null
        return false
      }
      return true
    } catch (error) {
      if (error instanceof Error) {
        errorMessage = error.message
      }
      return false
    }
  }

  // Effect to load profile data when timestamp is selected
  $effect(() => {
    if (!selectedProfile) {
      return
    }

    const profileFiles = getProfileFiles().find(
      (profile) => profile[0].getTime() === selectedProfile!.getTime()
    )

    if (!profileFiles) {
      return
    }

    ;(async () => {
      try {
        const processedProfile = await processProfileFiles(profileFiles[1])
        getProfileData = enclosure({
          profile: processedProfile.profile,
          dataflow: processedProfile.dataflow,
          sources: processedProfile.sources
        })
      } catch (error) {
        errorMessage = error instanceof Error ? error.message : 'Failed to process profile'
      }
    })()
  })

  let collectNewData = $state(true)

  let downloadProgress = useDownloadProgress()

  const loadProfileData = async () => {
    errorMessage = ''

    downloadProgress.onProgress(0, 1)

    const { downloadPromise } = api.getPipelineSupportBundle(
      pipelineName,
      {
        collect: collectNewData,
        circuit_profile: true,
        pipeline_config: true,
        dataflow_graph: true
      },
      downloadProgress.onProgress
    )
    const supportBundle = await downloadPromise.catch((error) => {
      errorMessage = error instanceof Error ? error.message : String(error)
      downloadProgress.reset()
      return null
    })
    if (!supportBundle) {
      return
    }

    const success = await processZipBundle(
      new Uint8Array(await (await supportBundle.dataPromise).arrayBuffer()),
      pipelineName,
      'No suitable profiles found. If you are trying to debug a pipeline of an older version try enabling "Collect new data" option.'
    )
    if (!success) {
      downloadProgress.reset()
    }
    // If successful, loading will complete when $effect sets the data
  }

  let fileInput: HTMLInputElement | null = $state(null)

  const uploadSupportBundle = async () => {
    if (!fileInput) {
      return
    }

    const file = fileInput.files?.[0]
    if (!file) {
      return
    }

    errorMessage = ''
    downloadProgress.onProgress(0, 1)
    const arrayBuffer = await file.arrayBuffer().catch((error) => {
      errorMessage = `Error processing uploaded support bundle: ${error}`
      downloadProgress.reset()
      return null
    })
    // Reset file input
    fileInput.value = ''
    if (!arrayBuffer) {
      return
    }
    const success = await processZipBundle(
      new Uint8Array(arrayBuffer),
      `uploaded-${file.name}`,
      'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile and dataflow graph (optional).'
    )
    if (!success) {
      downloadProgress.reset()
    }
    // If successful, loading will complete when $effect sets the data
  }

  $effect(() => {
    if (downloadProgress.percent === 100) {
      downloadProgress.reset()
    }
  })

  const triggerFileUpload = () => {
    fileInput?.click()
  }
  let errorMessage = $state('')

  const toast = useToast()
  $effect(() => {
    if (errorMessage && getProfileData) {
      toast.toastError(new Error(errorMessage), 10000)
    }
  })

  function highlightSourceRanges(sourceRanges: SourcePositionRange[]) {
    // Build URL hash with multiple ranges
    // Format: #program.sql:startLine:startColumn-endLine:endColumn,startLine2:startColumn2-endLine2:endColumn2
    const rangeStrings = sourceRanges.map((range) => {
      const startLine = range.start.line
      const startColumn = range.start.column
      const endLine = range.end.line
      const endColumn = range.end.column
      return `${startLine}:${startColumn}-${endLine}:${endColumn}`
    })

    const hash = `#program.sql:${rangeStrings.join(',')}`
    goto(hash)
  }
</script>

{#snippet label()}
  <span class=""> Profiler </span>
{/snippet}

<div class="flex h-full flex-col">
  <input
    type="file"
    accept=".zip"
    bind:this={fileInput}
    onchange={uploadSupportBundle}
    class="hidden"
  />
  <div class="{nonNull(downloadProgress.percent) ? '' : 'opacity-0'} transition-opacity">
    <Progress
      class="-mt-1 h-1 sm:-mt-4"
      value={downloadProgress.percent ? downloadProgress.percent : null}
      max={100}
    >
      <Progress.Track>
        <Progress.Range class="bg-primary-500" />
      </Progress.Track>
    </Progress>
  </div>
  {#if getProfileData}
    {@const { profile, dataflow, sources } = getProfileData()}
    <ProfilerLayout
      profileData={profile}
      dataflowData={dataflow}
      programCode={sources}
      onHighlightSourceRanges={highlightSourceRanges}
      toolbarClass="pb-2 sm:-mt-2"
      diagramClass="bg-white-dark rounded"
    >
      {#snippet toolbarStart()}
        <!-- File Menu Popup -->
        <Popup>
          {#snippet trigger(toggle)}
            <button class="btn !bg-surface-100-900" onclick={toggle}>Load profile</button>
          {/snippet}
          {#snippet content(close)}
            <div
              transition:fade={{ duration: 100 }}
              class="absolute top-10 left-0 z-30 w-max min-w-[200px]"
            >
              <div class="bg-white-dark flex flex-col rounded-container shadow-md">
                <!-- Download Profile -->
                <button
                  class="flex items-center gap-2 rounded-t-container px-4 py-2 text-left hover:preset-tonal-surface"
                  onclick={() => {
                    loadProfileData()
                    close()
                  }}
                >
                  Download profile
                </button>

                <!-- Collect New Data Toggle -->
                <label
                  class="flex cursor-pointer items-center justify-between gap-3 px-4 py-2 hover:preset-tonal-surface"
                >
                  <span>Collect new data</span>
                  <input type="checkbox" bind:checked={collectNewData} class="checkbox" />
                </label>

                <!-- Divider -->
                <div class="hr"></div>

                <!-- Upload Support Bundle -->
                <button
                  class="flex items-center gap-2 rounded-b-container px-4 py-2 text-left hover:preset-tonal-surface"
                  onclick={() => {
                    triggerFileUpload()
                    close()
                  }}
                >
                  Upload support bundle
                </button>
              </div>
            </div>
          {/snippet}
        </Popup>

        <!-- Profile Timestamp Selector -->
        <ProfileTimestampSelector
          profileFiles={getProfileFiles()}
          bind:selectedTimestamp={selectedProfile}
        />
      {/snippet}
    </ProfilerLayout>
  {:else}
    <div class="flex h-full flex-col items-center justify-center gap-4">
      {#if errorMessage}
        <div class="rounded preset-outlined-error-600-400 p-2" transition:slide>
          {errorMessage}
        </div>
      {/if}
      <div class="flex gap-4">
        <button class="btn preset-filled-primary-500" onclick={loadProfileData}>
          Download pipeline profile
        </button>
        <label class="flex cursor-pointer items-center gap-2">
          <input type="checkbox" bind:checked={collectNewData} class="checkbox" />
          <span class="text-sm">Collect new data</span>
        </label>
      </div>
      or
      <button class="link -mt-2 p-2 hover:underline" onclick={triggerFileUpload}>
        upload a support bundle zip
      </button>
    </div>
  {/if}
</div>
