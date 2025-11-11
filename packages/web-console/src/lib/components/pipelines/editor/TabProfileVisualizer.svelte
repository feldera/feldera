<script lang="ts" module>
  let loadedPipelineName: string | null = null
  let getCircuitProfileData: (() => JsonProfiles) | null = $state(null)
  let getDataflowData: (() => Dataflow) | null = $state(null)
  let getProfileData: () => [Date, ZipItem[]][] = $state(() => [])
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

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  const api = usePipelineManager()

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    untrack(() => {
      if (getCircuitProfileData && loadedPipelineName !== pipelineName) {
        getCircuitProfileData = null
        getDataflowData = null
        loadedPipelineName = null
        getProfileData = () => []
      }
    })
  })
  const circuitProfileRegex = /json_circuit_profile\.json$/
  const dataflowRegex = /dataflow_graph\.json$/

  const getSuitableProfiles = (profiles: ZipItem[]) => {
    const profileTimestamps = groupBy(
      profiles,
      (file) => file.filename.match(/^(.*?)_/)?.[1] ?? ''
    ).filter(
      (group) => group[0] && group[1].some((file) => circuitProfileRegex.test(file.filename))
    )
    return sortOn(
      profileTimestamps.map(([timestamp, files]) => tuple(new Date(timestamp), files)),
      (p) => p[0]
    )
  }

  const processZipBundle = async (zipData: Uint8Array, sourceName: string) => {
    const profiles = (() => {
      try {
        return unzip(zipData)
      } catch (error) {
        if (Error.isError(error)) {
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
      errorMessage =
        'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile.'
      return false
    }

    // Load the dataflow graph once (if available)
    const dataflowFile = profiles.find((file) => dataflowRegex.test(file.filename))
    if (dataflowFile) {
      const decoder = new TextDecoder()
      getDataflowData = enclosure(
        JSON.parse(decoder.decode(await dataflowFile.read())) as unknown as Dataflow
      )
    } else {
      getDataflowData = null
    }

    const profile = suitableProfiles.at(-1)!

    loadedPipelineName = sourceName
    getProfileData = () => suitableProfiles
    selectedProfile = profile[0]
    return true
  }

  $effect(() => {
    selectedProfile

    if (!selectedProfile) {
      return
    }

    const profile = getProfileData().find(
      (profile) => profile[0].getTime() === selectedProfile!.getTime()
    )

    if (!profile) {
      return
    }

    ;(async () => {
      const decoder = new TextDecoder()

      getCircuitProfileData = enclosure(
        JSON.parse(
          decoder.decode(
            await profile[1].find((file) => circuitProfileRegex.test(file.filename))!.read()
          )
        ) as unknown as JsonProfiles
      )
    })()
  })

  const loadProfileData = async () => {
    errorMessage = ''
    const supportBundle = await api.getPipelineSupportBundle(pipelineName)
    await processZipBundle(supportBundle, pipelineName)
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

    const arrayBuffer = await file.arrayBuffer().catch((error) => {
      errorMessage = `Error processing uploaded support bundle: ${error}`
      return null
    })
    if (!arrayBuffer) {
      return
    }
    await processZipBundle(new Uint8Array(arrayBuffer), `uploaded-${file.name}`)

    // Reset file input
    fileInput.value = ''
  }

  const triggerFileUpload = () => {
    fileInput?.click()
  }
  let errorMessage = $state('')

  const toast = useToast()
  $effect(() => {
    if (errorMessage && getCircuitProfileData) {
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
  {#if getCircuitProfileData}
    <div class="flex flex-nowrap gap-4 pb-2 sm:-mt-2">
      <button class="btn !bg-surface-100-900" onclick={loadProfileData}>Load profile</button>
      <button class="btn !bg-surface-100-900" onclick={triggerFileUpload}
        >Upload support bundle</button
      >
      <select
        class="select ml-auto w-40 md:ml-0"
        value={selectedProfile?.getTime()}
        onchange={(e) => {
          console.log('e.currentTarget.value', typeof e.currentTarget.value, e.currentTarget.value)
          selectedProfile = new Date(parseInt(e.currentTarget.value))
        }}
      >
        {#each getProfileData().map((p) => p[0]) as timestamp (timestamp)}
          <option value={timestamp.getTime()}>{timestamp.toLocaleTimeString()}</option>
        {/each}
      </select>
    </div>
  {/if}
  <div class="relative h-full w-full">
    {#if getCircuitProfileData && getDataflowData}
      <ProfilerDiagram
        profileData={getCircuitProfileData()}
        dataflowData={getDataflowData()}
        class="bg-white-dark rounded"
      ></ProfilerDiagram>
    {:else}
      <div class="flex h-full flex-col items-center justify-center gap-4">
        {#if errorMessage}
          <div class="rounded p-2 preset-outlined-error-600-400" transition:slide>
            {errorMessage}
          </div>
        {/if}
        <div>
          <button class="btn preset-filled-primary-500" onclick={loadProfileData}
            >Load latest pipeline profile</button
          >
        </div>
        or<button class="link -mt-2 p-2 hover:underline" onclick={triggerFileUpload}
          >upload a support bundle zip</button
        >
      </div>
    {/if}
  </div>
</div>
