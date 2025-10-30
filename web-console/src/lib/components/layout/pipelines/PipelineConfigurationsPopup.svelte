<script lang="ts">
  import { type ExtendedPipeline, type Pipeline } from '$lib/services/pipelineManager'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import MultiJSONDialog from '$lib/components/dialogs/MultiJSONDialog.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'

  let {
    pipeline,
    pipelineBusy
  }: {
    pipeline: WritablePipeline<true>
    pipelineBusy: boolean
  } = $props()

  const globalDialog = useGlobalDialog()
  const { toastError } = useToast()
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
      Stop the pipeline to edit settings
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
        editorClass: 'h-[40vh]',
        filePath: `file://pipelines/${pipeline.current.name}/RuntimeConfig.json`,
        readOnlyMessage: 'Cannot edit config while pipeline is running'
      },
      programConfig: {
        title: `Compilation configuration`,
        editorClass: 'h-24',
        filePath: `file://pipelines/${pipeline.current.name}/ProgramConfig.json`,
        readOnlyMessage: 'Cannot edit config while pipeline is running'
      }
    }}
    onApply={async (json) => {
      let patch: Partial<Pipeline> = {}
      try {
        patch.runtimeConfig = JSONbig.parse(json.runtimeConfig)
        patch.programConfig = JSONbig.parse(json.programConfig)
        await pipeline.patch(patch)
      } catch (e) {
        toastError(e as any)
        throw e
      }
    }}
    onClose={() => (globalDialog.dialog = null)}
  >
    {#snippet title()}
      Configure {pipeline.current.name} pipeline
    {/snippet}
  </MultiJSONDialog>
{/snippet}
