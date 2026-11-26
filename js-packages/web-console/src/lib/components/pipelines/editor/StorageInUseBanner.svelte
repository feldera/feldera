<script lang="ts">
  import RBAC from '#lib/components/auth/RBAC.svelte'
  import UpdateRuntimeButton from '#lib/components/pipelines/UpdateRuntimeButton.svelte'
  import { page } from '$app/state'

  const {
    pipelineName,
    runtimeVersion
  }: {
    pipelineName: string
    runtimeVersion: { version: string; status: 'latest' | 'update_available' | 'custom' }
  } = $props()
</script>

<div class="flex h-fit min-h-10 items-center gap-2 preset-tonal-tertiary px-4">
  <span class="fd fd-triangle-alert text-[20px]"></span>
  {#if runtimeVersion.status === 'update_available'}
    <span>
      A new runtime version {page.data.feldera!.version} is available. Update the runtime to edit the
      pipeline.
    </span>
    <RBAC require="exec:runtime_upgrade">
      <UpdateRuntimeButton {pipelineName} class="h-7 py-0 text-sm" />
    </RBAC>
  {:else}
    <span>
      Editing a pipeline with existing state in storage — changes will take effect when the pipeline
      restarts. See the <a
        href="https://docs.feldera.com/pipelines/modifying"
        target="_blank"
        rel="noreferrer"
        class="underline">documentation</a
      >.
    </span>
  {/if}
</div>
