<script lang="ts">
  import { ClickFeedback } from 'common-ui'
  import IconLoader from '#assets/icons/generic/loader-alt.svg?component'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '#lib/compositions/pipelines/usePipelineList.svelte.ts'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { isPipelineCompiling } from '#lib/functions/pipelines/status.ts'
  import { page } from '$app/state'

  const {
    pipelineName,
    class: _class = '',
    onUpdate
  }: {
    pipelineName: string
    /**
     * Size classes for the button. The component sets the colors. Put a margin on a wrapper,
     * because a margin on the button moves the spinner off center.
     */
    class?: string
    /** Called after the runtime update is requested. */
    onUpdate?: () => void
  } = $props()

  const api = usePipelineManager()
  const pipelineList = usePipelineList()
  const { updatePipeline, discardPendingListRefresh } = useUpdatePipelineList()

  // The runtime update recompiles the pipeline, so the button stays busy until the compilation ends.
  const isCompiling = $derived(
    isPipelineCompiling(pipelineList.pipelines?.find((p) => p.name === pipelineName)?.status)
  )

  const updateRuntime = () => {
    // The pipeline manager sets the platform version to its own version and queues a compilation.
    updatePipeline(pipelineName, (p) => ({
      ...p,
      platformVersion: page.data.feldera!.version,
      status: { Queued: { cause: 'compile' } }
    }))
    // Drops the poll response in flight, which has the status from before the update.
    discardPendingListRefresh()
    api.postUpdateRuntime(pipelineName)
    onUpdate?.()
  }
</script>

<ClickFeedback active={isCompiling}>
  {#snippet children({ active, clickFeedback })}
    <div class="flex items-center gap-2">
      <!-- `disabled` would fade the outlined button, so `pointer-events-none` blocks the clicks. -->
      <button
        class="btn {_class} {active
          ? 'pointer-events-none preset-outlined-primary-500'
          : 'border-0 preset-filled-primary-500'}"
        aria-disabled={active}
        onclick={() => {
          if (active) {
            return
          }
          clickFeedback()
          updateRuntime()
        }}
      >
        Update
      </button>
      {#if active}
        <IconLoader class="h-4 flex-none animate-spin fill-surface-950-50"></IconLoader>
      {/if}
    </div>
  {/snippet}
</ClickFeedback>
