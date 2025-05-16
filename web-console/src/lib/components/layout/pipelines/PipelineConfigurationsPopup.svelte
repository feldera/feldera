<script lang="ts">
  import { type ExtendedPipeline, type Pipeline } from '$lib/services/pipelineManager'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import MultiJSONDialog from '$lib/components/dialogs/MultiJSONDialog.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'

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
</script>

<button
  class="fd fd-settings btn btn-icon text-[20px] preset-tonal-surface"
  onclick={() => (globalDialog.dialog = pipelineConfigurationsDialog)}
  aria-label="Pipeline actions"
></button>

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
{/snippet}
