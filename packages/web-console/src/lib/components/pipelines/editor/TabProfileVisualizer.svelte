<script lang="ts" module>
  let loadedPipelineName: string | null = null
  let getProfileData: (() => JsonProfiles) | null = $state(null)
  let getDataflowData: (() => Dataflow) | null = $state(null)
</script>

<script lang="ts">
  import ProfilerDiagram from '$lib/components/profiler/ProfilerDiagram.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { groupBy } from '$lib/functions/common/array'
  import { enclosure } from '$lib/functions/common/function'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { ZipItem } from 'but-unzip'
  import type { JsonProfiles, Dataflow } from 'profiler-lib'
  import sortOn from 'sort-on'
  import { untrack } from 'svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  const api = usePipelineManager()

  let pipelineName = $derived(pipeline.current.name)
  $effect(() => {
    pipelineName
    untrack(() => {
      if (getProfileData && loadedPipelineName !== pipelineName) {
        getProfileData = null
        getDataflowData = null
        loadedPipelineName = null
      }
    })
  })
  const circuitProfileRegex = /json_circuit_profile\.json$/
  const dataflowRegex = /dataflow_graph\.json$/
  const loadProfileData = async () => {
    // const [profileData, dataflowData] = await Promise.all([
    //   api.getPipelineDataflowGraph(pipeline.current.name),
    //   api.getPipelineProfile(pipeline.current.name)
    // ])
    const supportBundle = await api.getPipelineSupportBundle(pipelineName)
    const suitableProfiles = getSuitableProfiles(supportBundle)
    if (suitableProfiles.length === 0) {
      return
    }
    const profile = suitableProfiles[0]
    const decoder = new TextDecoder()
    // console.log('profile decoded', decoder.decode(await profile[1].find(file => circuitProfileRegex.test(file.filename))!.read()))
    getProfileData = enclosure(
      JSON.parse(
        decoder.decode(
          await profile[1].find((file) => circuitProfileRegex.test(file.filename))!.read()
        )
      ) as unknown as JsonProfiles
    )
    getDataflowData = enclosure(
      JSON.parse(
        decoder.decode(await profile[1].find((file) => dataflowRegex.test(file.filename))!.read())
      ) as unknown as Dataflow
    )
    loadedPipelineName = pipelineName
  }

  const getSuitableProfiles = (profiles: ZipItem[]) => {
    const profileTimestamps = groupBy(
      profiles,
      (file) => file.filename.match(/^(.*?)_/)?.[1] ?? ''
    ).filter(
      (group) =>
        group[0] &&
        group[1].some((file) => circuitProfileRegex.test(file.filename)) &&
        group[1].some((file) => dataflowRegex.test(file.filename))
    )
    return sortOn(profileTimestamps, (p) => new Date(p[0]))
  }
</script>

<div class="flex h-full flex-col">
  <div>
    <button class="preset-outlined-primary btn" onclick={loadProfileData}>Load profile</button>
  </div>
  {#if getProfileData && getDataflowData}
    <ProfilerDiagram
      profileData={getProfileData()}
      dataflowData={getDataflowData()}
      class="bg-white-dark rounded"
    ></ProfilerDiagram>
  {/if}
</div>
