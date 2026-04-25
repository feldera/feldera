<!--
  Thin wrapper: constructs a minimal WritablePipeline so the real Actions
  component can be rendered in a test. Does NOT reimplement any of
  Actions.svelte's logic.
-->
<script lang="ts">
  import Actions from '$lib/components/pipelines/list/Actions.svelte'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  let {
    pipeline: pipelineProp,
    deleted = false
  }: {
    pipeline: { current: ExtendedPipeline }
    deleted?: boolean
  } = $props()

  const pipeline: WritablePipeline<true> = {
    get current() {
      return pipelineProp.current
    },
    async patch() {
      return pipelineProp.current
    }
  }
</script>

<Actions
  {pipeline}
  {deleted}
  editConfigDisabled={deleted}
  unsavedChanges={false}
  saveFile={() => {}}
></Actions>
