<script lang="ts">
  const {
    pipelineName,
    status,
    baseRuntimeVersion
  }: {
    pipelineName: string
    status: 'custom' | 'latest' | 'update_available'
    baseRuntimeVersion: string
  } = $props()

  import { Popover, Tooltip } from 'common-ui'
  import RBAC from '#lib/components/auth/RBAC.svelte'
  import UpdateRuntimeButton from '#lib/components/pipelines/UpdateRuntimeButton.svelte'
  import { normalizeRuntimeVersion } from '#lib/functions/pipelines/runtimeVersion.ts'

  // Keeps the popover open long enough for the user to see the button change.
  const CLOSE_DELAY_MS = 2_000

  let isOpen = $state(false)
  // The update changes `status` to 'latest' at once. This flag keeps the popover mounted until
  // it closes.
  let isClosePending = $state(false)

  const closeAfterUpdate = () => {
    isClosePending = true
    setTimeout(() => {
      isOpen = false
      isClosePending = false
    }, CLOSE_DELAY_MS)
  }
</script>

{#if status === 'update_available' || isClosePending}
  <Popover placement="bottom-start" strategy="fixed" bind:isOpen>
    <div>A new runtime version {normalizeRuntimeVersion(baseRuntimeVersion)} is available.</div>
    <RBAC require="exec:runtime_upgrade">
      <div class="mt-2">
        <UpdateRuntimeButton {pipelineName} class="h-6" onUpdate={closeAfterUpdate} />
      </div>
    </RBAC>
  </Popover>
{:else if status === 'custom'}
  <Tooltip placement="bottom-start" strategy="fixed">
    <div>This custom runtime version is set in the compilation configuration.</div>
  </Tooltip>
{/if}
