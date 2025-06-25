<script lang="ts">
  import { type ExtendedPipeline, type Pipeline } from '$lib/services/pipelineManager'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import MultiJSONDialog from '$lib/components/dialogs/MultiJSONDialog.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import Tooltip from '$lib/components/common/Tooltip.svelte'

  let {
    pipeline,
    pipelineBusy
  }: {
    pipeline: {
      current: ExtendedPipeline
      patch: (pipeline: Partial<Pipeline>) => Promise<ExtendedPipeline>
    }
    pipelineBusy: boolean
  } = $props()

  const globalDialog = useGlobalDialog()
  const { toastError } = useToast()

  let storageBound = $state(true)
</script>

<button
  class="fd fd-settings btn btn-icon text-[20px] preset-tonal-surface"
  onclick={() => (globalDialog.dialog = pipelineConfigurationsDialog)}
  aria-label="Pipeline actions"
></button>
<!-- <button
  class="group flex flex-nowrap items-center gap-2 rounded px-2 text-[20px] preset-tonal-surface hover:preset-filled-surface-100-900 sm:gap-3 sm:px-3"
  onclick={() => (globalDialog.dialog = pipelineConfigurationsDialog)}
  aria-label="Pipeline actions"
>
  <div class="fd fd-settings group-hover:preset-filled-surface-100-900"></div>
  {#if pipelineBusy}
    <Tooltip class="z-20 rounded bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline and unbind storage to edit configuration
    </Tooltip>
  {:else}
    <Tooltip class="z-20 rounded bg-white text-surface-950-50 dark:bg-black" placement="top">
      Compilation and runtime configuration
    </Tooltip>
  {/if}
  <span class="w-0 -translate-x-[1px] text-base text-surface-500">|</span>
  <div class="fd rotate-90 text-surface-500 {storageBound ? 'fd-mic' : 'fd-mic-off'}"></div>

  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50">
    {#if storageBound}
      Storage is in use, click to change
    {:else}
      Storage is not in use, click to change
    {/if}
  </Tooltip>
</button> -->

{#snippet pipelineConfigurationsDialog()}
  <MultiJSONDialog
    disabled={pipelineBusy}
    values={{
      runtimeConfig: JSONbig.stringify(pipeline.current.runtimeConfig, undefined, '  '),
      programConfig: JSONbig.stringify(pipeline.current.programConfig, undefined, '  ')
    }}
    metadata={{
      runtimeConfig: {
        title: `Runtime configuration`,
        editorClass: 'h-96',
        filePath: `file://pipelines/${pipeline.current.name}/RuntimeConfig.json`
      },
      programConfig: {
        title: `Compilation configuration`,
        editorClass: 'h-24',
        filePath: `file://pipelines/${pipeline.current.name}/ProgramConfig.json`
      }
    }}
    onApply={async (json) => {
      let patch: Partial<Pipeline> = {}
      try {
        patch.runtimeConfig = JSONbig.parse(json.runtimeConfig)
        patch.programConfig = JSONbig.parse(json.programConfig)
      } catch (e) {
        toastError(e as any)
        throw e
      }
      await pipeline.patch(patch)
    }}
    onClose={() => (globalDialog.dialog = null)}
  >
    {#snippet title()}
      <div class="h5 font-normal">
        Configure {pipeline.current.name} pipeline
      </div>
    {/snippet}
  </MultiJSONDialog>
  <!-- <div class="px-3">
    <hr class="hr" />
  </div>
  <div class="flex flex-col gap-3 p-3">
    <div class="flex flex-nowrap items-center gap-2">
      {#if storageBound}
        Pipeline storage is in use.
      {:else}
        Pipeline storage is not in use.
      {/if}
      <button
        class="btn preset-filled-surface-50-950"
        onclick={() => (storageBound = !storageBound)}
      >
        {#if storageBound}
          Unbind storage
        {:else}
          Bind storage
        {/if}
      </button>
    </div>
    <span>
      {#if storageBound}
        Depending on the storage backend, unbinding storage may delete the pipeline state.
      {:else}
        Binding storage will allocate and provision storage resources.
      {/if}
    </span>
  </div> -->
  <!-- ======================  -->
  <!-- <div class="px-3">
  <hr class="hr" />
  </div>
  <div class="m-3 p-3 border border-error-500 rounded">
    <div class="pb-3">Deleting the pipeline deletes the associated code, data and logs.</div>
    <button class="btn preset-outlined-error-500 text-error-500"> Delete the pipeline </button>
  </div> -->
{/snippet}
