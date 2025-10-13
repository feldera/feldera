<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { SupportBundleOptions } from '$lib/services/pipelineManager'
  let { pipelineName }: { pipelineName: string } = $props()

  let api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const toast = useToast()
  const submitHandler = async () => {
    try {
      await api.downloadPipelineSupportBundle(pipelineName, data)
    } catch (error) {
      toast.toastError(new Error(`Failed to download support bundle: ${error}`))
    }
  }

  let defaultData: SupportBundleOptions = {
    circuit_profile: true,
    heap_profile: true,
    logs: true,
    metrics: true,
    pipeline_config: true,
    stats: true,
    system_config: true
  }
  let data: SupportBundleOptions = $state(defaultData)

  let fields = {
    circuit_profile: {
      label: 'Circuit profile',
      description: 'Include circuit profiling data'
    },
    heap_profile: {
      label: 'Heap profile',
      description: 'Include heap profiling data'
    },
    logs: {
      label: 'Logs',
      description: 'Include logs'
    },
    metrics: {
      label: 'Metrics',
      description: 'Include metrics'
    },
    pipeline_config: {
      label: 'Pipeline config',
      description: 'Include pipeline config'
    },
    stats: {
      label: 'Stats',
      description: 'Include stats'
    },
    system_config: {
      label: 'System config',
      description: 'Include system config'
    }
  }
</script>

<button
  class="btn preset-tonal-surface"
  onclick={async () => {
    data = defaultData
    globalDialog.dialog = supportBundleDialog
  }}
>
  <span class="fd fd-file-down text-[20px] text-primary-500"></span>
  Support bundle
</button>
<Tooltip
  placement="top"
  class="z-10 w-[204px] text-wrap rounded-container bg-white text-base text-surface-950-50 dark:bg-black"
>
  Generate a bundle with logs, metrics, and configs to help troubleshoot issues
</Tooltip>

{#snippet supportBundleDialog()}
  <GenericDialog
    onApply={submitHandler}
    onClose={() => {
      globalDialog.dialog = null
    }}
    confirmLabel="Download"
  >
    {#snippet title()}
      Download Support Bundle
    {/snippet}
    <div class="-mt-2 pb-2 font-semibold">{pipelineName}</div>
    Select the details you want to include in the bundle
    {@render supportBundleForm()}
  </GenericDialog>
{/snippet}

{#snippet supportBundleForm()}
  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
  <form class="flex flex-col gap-3">
    {#each Object.entries(fields) as [key, { label, description }]}
      <div class="flex items-center gap-4">
        <input
          type="checkbox"
          id={key}
          bind:checked={data[key as keyof SupportBundleOptions]}
          class="checkbox"
        />
        <div class="flex flex-col">
          <label for={key} class="cursor-pointer font-medium">{label}</label>
          <!-- <label for={key} class="cursor-pointer text-sm text-surface-500">{description}</label> -->
        </div>
      </div>
    {/each}
  </form>
{/snippet}
