<script lang="ts">
  import { type ExtendedPipeline, type Pipeline } from '$lib/services/pipelineManager'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import MultiJSONDialog from '$lib/components/dialogs/MultiJSONDialog.svelte'

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
      await pipeline.patch({
        runtimeConfig: JSONbig.parse(json.runtimeConfig),
        programConfig: JSONbig.parse(json.programConfig)
      })
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
