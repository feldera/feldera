<script lang="ts" module>
  let loadedPipelineName: string | null = null
  // let getCircuitProfileData: (() => JsonProfiles) | null = $state(null)
  // let getDataflowData: (() => Dataflow) | null = $state(null)
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
  import ProfilerDiagram from '$lib/components/profiler/ProfilerDiagram.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { groupBy } from '$lib/functions/common/array'
  import { enclosure } from '$lib/functions/common/function'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { unzip, type ZipItem } from 'but-unzip'
  import type { JsonProfiles, Dataflow } from 'profiler-lib'
  import sortOn from 'sort-on'
  import { untrack } from 'svelte'
  import { slide } from 'svelte/transition'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { tuple } from '$lib/functions/common/tuple'
  import { Progress } from '@skeletonlabs/skeleton-svelte'

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
      loadingProgress = 9
      const dataflow = JSON.parse(
        decoder.decode(
          await profileFiles[1].find((file) => dataflowGraphRegex.test(file.filename))!.read()
        )
      ) as unknown as Dataflow
      const sources = pipeline.current.programCode.split('\n')
      loadingProgress = 10

      getProfileData = enclosure({
        profile,
        dataflow,
        sources
      })
    })()
  })

  let collectNewData = $state(true)
  let loadingProgress = $state(0)
  const MAX_PROGRESS = 10

  const loadProfileData = async () => {
    errorMessage = ''
    loadingProgress = 7

    const supportBundle = await api
      .getPipelineSupportBundle(pipelineName, {
        collect: collectNewData,
        circuit_profile: true,
        pipeline_config: true,
        dataflow_graph: true
      })
      .catch((error) => {
        errorMessage = error instanceof Error ? error.message : String(error)
        loadingProgress = 0
        return null
      })
    if (!supportBundle) {
      return
    }
    loadingProgress = 8

    const success = await processZipBundle(
      supportBundle,
      pipelineName,
      'No suitable profiles found. If you are trying to debug a pipeline of an older version try enabling "Collect new data" option.'
    )
    if (!success) {
      loadingProgress = 0
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
    loadingProgress = 7
    const arrayBuffer = await file.arrayBuffer().catch((error) => {
      errorMessage = `Error processing uploaded support bundle: ${error}`
      loadingProgress = 0
      return null
    })
    // Reset file input
    fileInput.value = ''
    if (!arrayBuffer) {
      return
    }
    loadingProgress = 8
    const success = await processZipBundle(
      new Uint8Array(arrayBuffer),
      `uploaded-${file.name}`,
      'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile, dataflow graph and pipeline config.'
    )
    if (!success) {
      loadingProgress = 0
    }
    // If successful, loading will complete when $effect sets the data
  }

  $effect(() => {
    if (loadingProgress >= MAX_PROGRESS) {
      loadingProgress = 0
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
  <div class="{loadingProgress ? '' : 'opacity-0'} transition-opacity">
    <Progress
      value={null}
      max={MAX_PROGRESS}
      classes="-mt-1 sm:-mt-5 h-0"
      meterTransition="duration-1000"
      trackClasses="!h-1"
    />
  </div>
  {#if getProfileData}
    <div class="flex flex-nowrap gap-4 pb-2 sm:-mt-2">
      <button class="btn !bg-surface-100-900" onclick={loadProfileData}>Download profile</button>
      <label class="flex cursor-pointer items-center gap-2">
        <input type="checkbox" bind:checked={collectNewData} class="checkbox" />
        <span class="text-sm">Collect new data</span>
      </label>
      <button class="btn !bg-surface-100-900" onclick={triggerFileUpload}
        >Upload support bundle</button
      >
      <div class="ml-auto">
        <select
          class="select w-40 md:ml-0"
          value={selectedProfile?.getTime()}
          onchange={(e) => {
            console.log(
              'e.currentTarget.value',
              typeof e.currentTarget.value,
              e.currentTarget.value
            )
            selectedProfile = new Date(parseInt(e.currentTarget.value))
          }}
        >
          {#each getProfileFiles().map((p) => p[0]) as timestamp (timestamp)}
            <option value={timestamp.getTime()}>{timestamp.toLocaleTimeString()}</option>
          {/each}
        </select>
      </div>
    </div>
  {/if}
  <div class="relative h-full w-full">
    {#if getProfileData}
      {@const { profile, dataflow, sources } = getProfileData()}
      <ProfilerDiagram
        profileData={profile}
        dataflowData={dataflow}
        programCode={sources}
        class="bg-white-dark rounded"
      ></ProfilerDiagram>
    {:else}
      <div class="flex h-full flex-col items-center justify-center gap-4">
        {#if errorMessage}
          <div class="rounded p-2 preset-outlined-error-600-400" transition:slide>
            {errorMessage}
          </div>
        {/if}
        <div class="flex gap-4">
          <button class="btn preset-filled-primary-500" onclick={loadProfileData}
            >Download pipeline profile</button
          ><label class="flex cursor-pointer items-center gap-2">
            <input type="checkbox" bind:checked={collectNewData} class="checkbox" />
            <span class="text-sm">Collect new data</span>
          </label>
        </div>
        or<button class="link -mt-2 p-2 hover:underline" onclick={triggerFileUpload}
          >upload a support bundle zip</button
        >
      </div>
    {/if}
  </div>
</div>
