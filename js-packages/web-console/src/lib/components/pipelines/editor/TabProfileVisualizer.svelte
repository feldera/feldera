<script lang="ts" module>
  let loadedPipelineName: string | null = null
  let getProfileData:
    | (() => {
        profile: JsonProfiles
        dataflow: Dataflow
        sources: string[]
      })
    | null = $state(null)
  let getProfileFiles: () => [Date, ZipItem[]][] = $state(() => [])
  let selectedProfile: Date | null = $state(null)
</script>

<script lang="ts">
  import ProfilerLayout from '$lib/components/profiler/ProfilerLayout.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { groupBy } from '$lib/functions/common/array'
  import { enclosure, nonNull } from '$lib/functions/common/function'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { unzip, type ZipItem } from 'but-unzip'
  import type { JsonProfiles, Dataflow } from 'profiler-lib'
  import sortOn from 'sort-on'
  import { untrack } from 'svelte'
  import { slide, fade } from 'svelte/transition'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { tuple } from '$lib/functions/common/tuple'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'

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
  const circuitProfileRegex = /circuit_profile\.json$/
  const dataflowGraphRegex = /dataflow_graph\.json$/
  const pipelineConfigRegex = /pipeline_config\.json$/

  const getSuitableProfiles = (profiles: ZipItem[]) => {
    const profileTimestamps = groupBy(
      profiles,
      (file) => file.filename.match(/^(.*?)_/)?.[1] ?? ''
    ).filter(
      (group) =>
        group[0] &&
        group[1].some((file) => circuitProfileRegex.test(file.filename)) &&
        group[1].some((file) => dataflowGraphRegex.test(file.filename)) &&
        group[1].some((file) => pipelineConfigRegex.test(file.filename))
    )
    return sortOn(
      profileTimestamps.map(([timestamp, files]) => tuple(new Date(timestamp), files)),
      (p) => p[0]
    )
  }

  const processZipBundle = async (zipData: Uint8Array, sourceName: string, message: string) => {
    downloadProgress.onProgress(1, 1)
    const profiles = (() => {
      try {
        return unzip(zipData)
      } catch (error) {
        if (error instanceof Error) {
          errorMessage = error.message
          return null
        }
      }
    })()
    if (!profiles) {
      return
    }
    const suitableProfiles = getSuitableProfiles(profiles)

    if (suitableProfiles.length === 0) {
      errorMessage = message
      return false
    }

    const profile = suitableProfiles.at(-1)!

    loadedPipelineName = sourceName
    getProfileFiles = () => suitableProfiles
    selectedProfile = profile[0]
    return true
  }

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
      const decoder = new TextDecoder()
      const profile = JSON.parse(
        decoder.decode(
          await profileFiles[1].find((file) => circuitProfileRegex.test(file.filename))!.read()
        )
      ) as unknown as JsonProfiles
      const dataflow = JSON.parse(
        decoder.decode(
          await profileFiles[1].find((file) => dataflowGraphRegex.test(file.filename))!.read()
        )
      ) as unknown as Dataflow
      const sources = pipeline.current.programCode.split('\n')

      getProfileData = enclosure({
        profile,
        dataflow,
        sources
      })
    })()
  })

  let collectNewData = $state(true)

  let downloadProgress = useDownloadProgress()

  const loadProfileData = async () => {
    errorMessage = ''

    downloadProgress.onProgress(0, 1)
    const supportBundle = await api
      .getPipelineSupportBundle(
        pipelineName,
        {
          collect: collectNewData,
          circuit_profile: true,
          pipeline_config: true,
          dataflow_graph: true
        },
        downloadProgress.onProgress
      )
      .catch((error) => {
        errorMessage = error instanceof Error ? error.message : String(error)
        downloadProgress.reset()
        return null
      })
    if (!supportBundle) {
      return
    }

    const success = await processZipBundle(
      new Uint8Array(await supportBundle.arrayBuffer()),
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
      'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile, dataflow graph and pipeline config.'
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
</script>

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
      value={downloadProgress.percent ? downloadProgress.percent : null}
      classes="-mt-1 sm:-mt-5 h-0"
      trackClasses="!h-1"
    />
  </div>
  {#if getProfileData}
    {@const { profile, dataflow, sources } = getProfileData()}
    <ProfilerLayout
      profileData={profile}
      dataflowData={dataflow}
      programCode={sources}
      class="bg-white-dark rounded"
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
              class="absolute left-0 top-10 z-30 w-max min-w-[200px]"
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
        <label class="flex items-center gap-2 text-sm">
          <span class="text-surface-600-400">Snapshot:</span>
          <select
            class="select w-40 md:ml-0"
            value={selectedProfile?.getTime()}
            onchange={(e) => {
              selectedProfile = new Date(parseInt(e.currentTarget.value))
            }}
          >
            {#each getProfileFiles().map((p) => p[0]) as timestamp (timestamp)}
              <option value={timestamp.getTime()}>{timestamp.toLocaleTimeString()}</option>
            {/each}
          </select>
        </label>
      {/snippet}
    </ProfilerLayout>
  {:else}
    <div class="flex h-full flex-col items-center justify-center gap-4">
      {#if errorMessage}
        <div class="rounded p-2 preset-outlined-error-600-400" transition:slide>
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
