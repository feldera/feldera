<!--
Pipeline Actions Component - Generic Multi-Action Dropdown System

This component implements a flexible button/dropdown system for pipeline actions that automatically
groups related actions into multi-action dropdowns when multiple options are available.

## Architecture Overview:

### 1. Action Processing Pipeline:
   getRawActions() → processActionsForDropdowns() → render individual buttons or multi-action dropdowns

### 2. Multi-Action Dropdown System:
   - **Static Configuration**: `multiActionConfigs` defines dropdown metadata (aria labels, button classes)
   - **Static Button Definitions**: `buttonConfigs` maps each action to complete button information
   - **Dynamic Availability**: `availableStartButtons` & `availableStopButtons` determine which actions are available
   - **Generic Rendering**: `_multiAction` snippet combines static config with derived availability

### 3. Dropdown Logic:
   - Start actions: Groups `_start`, `_start_paused`, `_resume` when multiple are available
   - Stop actions: Groups `_stop`, `_kill`, `_pause` when multiple are available
   - Prime button: First available action from each group, rendered using standalone button snippet
   - Dropdown menu: All available actions with labels, descriptions, and click handlers

### 4. Button Reuse:
   - Prime buttons reuse existing standalone button snippets (`_start()`, `_pause()`, etc.)
   - Dropdown achieves visual effect by placing chevron button adjacent to standalone button
   - Ensures perfect consistency between standalone and dropdown representations

### 5. Extensibility:
   - Add new actions to `buttonConfigs` with `standaloneButton` reference
   - Update `startActions`/`stopActions` arrays to include in dropdowns
   - System automatically handles grouping and rendering

## Key Benefits:
- **Consistent UX**: Same actions work identically in standalone and dropdown contexts
- **Responsive Design**: Automatically adapts based on available pipeline actions
- **Easy Extension**: New action types require minimal configuration changes
- **Clean Separation**: Static configuration, dynamic availability, and rendering logic are separate
-->

<script lang="ts">
  import { Popover, Tooltip } from 'common-ui'
  import { slide } from 'svelte/transition'
  import { match, P } from 'ts-pattern'
  import { goto } from '$app/navigation'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'
  import Popup from '$lib/components/common/Popup.svelte'
  import SplitButton, {
    type SplitButtonSize,
    type SplitButtonVariant
  } from '$lib/components/common/SplitButton.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import PipelineConfigurationsPopup from '$lib/components/layout/pipelines/PipelineConfigurationsPopup.svelte'
  import { duplicatePipeline, duplicatePipelineTooltip } from '$lib/compositions/duplicatePipeline'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useIsMobile } from '$lib/compositions/layout/useIsMobile.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { useConceptualHq } from '$lib/compositions/useConceptualHq.svelte'
  import { useIsEnterprise } from '$lib/compositions/useEdition.svelte'
  import { usePermission } from '$lib/compositions/usePermission.svelte'
  import { getPipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import {
    deletePipelineDisabledReason,
    getDeploymentStatusLabel,
    isPipelineShutdown
  } from '$lib/functions/pipelines/status'
  import { resolve } from '$lib/functions/svelte'
  import { captureEvent } from '$lib/services/analytics'
  import { bookADemoUrl } from '$lib/services/calendly'
  import type { PipelineAction } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  let {
    pipeline,
    onDeletePipeline,
    editConfigDisabled,
    deleted = false,
    unsavedChanges,
    onActionSuccess,
    saveFile,
    class: _class = ''
  }: {
    pipeline: WritablePipeline<true>
    onDeletePipeline?: (pipelineName: string) => void
    editConfigDisabled: boolean
    deleted?: boolean
    unsavedChanges: boolean
    onActionSuccess?: (pipelineName: string, action: PipelineAction) => void
    saveFile: () => void
    class?: string
  } = $props()

  const globalDialog = useGlobalDialog()
  const api = usePipelineManager()
  const pipelineList = usePipelineList()
  const { discardPendingListRefresh, updatePipeline, updatePipelines } = useUpdatePipelineList()
  const deletePipeline = async (pipelineName: string) => {
    await api.deletePipeline(pipelineName)
    onDeletePipeline?.(pipelineName)
    goto(resolve('/'))
  }
  const duplicateCurrentPipeline = async () => {
    const pipelines = pipelineList.pipelines
    if (!pipelines || unsavedChanges) {
      return
    }

    const newPipeline = await duplicatePipeline(api, pipeline.current, pipelines, {
      discardPendingListRefresh,
      updatePipeline,
      updatePipelines
    })
    await goto(resolve(`/pipelines/${encodeURIComponent(newPipeline.name)}/`))
  }
  const { toastError } = useToast()

  const conceptualHq = useConceptualHq()
  const calendlyPlacement = 'pipeline_actions_upgrade'
  const calendlyUrl = $derived(
    bookADemoUrl({ visitorId: conceptualHq.deviceId, placement: calendlyPlacement })
  )

  // An already-deleted pipeline offers no Delete action; otherwise the backend
  // dictates when deletion is possible (fully stopped, storage cleared).
  const deleteDisabledReason = $derived(
    deletePipelineDisabledReason(pipeline.current.status, pipeline.current.storageStatus, deleted)
  )

  const actions = {
    _start,
    _start_paused,
    _start_error,
    _start_pending,
    _standby,
    _activate,
    _resume,
    _pause,
    _kill_short,
    _kill,
    _stop,
    _multiStop,
    _multiStart,
    _more,
    _spacer_short,
    _spacer_long,
    _spinner,
    _status_spinner,
    _configurations,
    _saveFile,
    _unschedule,
    _storage_indicator
  }

  const isEnterprise = useIsEnterprise()

  const canExec = usePermission('exec:pipeline')
  const canWrite = usePermission('write:pipeline')
  const canWriteCode = usePermission('write:pipeline_code')
  // Gates the settings tooltip: a read-only caller sees a view-only hint instead
  // of "stop the pipeline to edit", which they could never act on.
  const canWriteConfig = usePermission('write:pipeline_config')

  // Action-menu entries hidden without the matching permission. Lifecycle
  // controls need `exec:pipeline`; the "More" menu holds only delete/duplicate,
  // both `write:pipeline`; saving the file writes pipeline code, so it needs
  // `write:pipeline_code`. Status, configuration and storage-state entries stay
  // visible for read-only callers; the storage clear button inside
  // `_storage_indicator` is gated on its own below.
  const EXEC_ACTIONS = new Set<keyof typeof actions>([
    '_start',
    '_start_paused',
    '_resume',
    '_standby',
    '_activate',
    '_pause',
    '_kill',
    '_kill_short',
    '_stop',
    '_start_pending',
    '_start_error',
    '_unschedule'
  ])
  const WRITE_ACTIONS = new Set<keyof typeof actions>(['_more'])
  const CODE_ACTIONS = new Set<keyof typeof actions>(['_saveFile'])
  const filterByPermission = (list: (keyof typeof actions)[]) =>
    list.filter(
      (action) =>
        (EXEC_ACTIONS.has(action) ? canExec.allowed : true) &&
        (WRITE_ACTIONS.has(action) ? canWrite.allowed : true) &&
        (CODE_ACTIONS.has(action) ? canWriteCode.allowed : true)
    )

  const stopButtons = isEnterprise.value
    ? (['_stop', '_kill'] as const)
    : (['_kill', '_stop'] as const)

  // Helper function to get raw actions for current pipeline status
  const getRawActions = (status: typeof pipeline.current.status) => {
    return filterByPermission(
      match(status)
        .returnType<(keyof typeof actions)[]>()
        .with('Stopped', () => [
          '_start',
          '_start_paused',
          '_standby',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Preparing', 'Provisioning', 'Initializing', () => [
          '_kill',
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Pausing', 'Resuming', () => [
          ...stopButtons,
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Unavailable', () => [
          ...stopButtons,
          '_spacer_long',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Running', 'ConcurrentBootstrapping', () => [
          ...stopButtons,
          '_pause',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Paused', () => [
          ...stopButtons,
          '_resume',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Suspending', () => [
          '_kill',
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Suspended', () => [
          '_spinner',
          '_kill',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Standby', () => [
          '_kill',
          '_activate',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Bootstrapping', () => [
          '_kill',
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Replaying', 'Synchronizing', () => [
          '_kill',
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('AwaitingApproval', () => [
          '_kill',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with('Stopping', () => [
          '_kill',
          '_spinner',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .with(
          { Queued: P.any },
          { CompilingSql: P.any },
          { SqlCompiled: P.any },
          { CompilingRust: P.any },
          (cause) => [
            ...(Object.values(cause)[0].cause === 'upgrade' ? ['_unschedule' as const] : []),
            '_start_pending',
            '_storage_indicator',
            '_saveFile',
            '_configurations',
            '_more'
          ]
        )
        .with('SqlError', 'RustError', 'SystemError', () => [
          '_start_error',
          '_storage_indicator',
          '_saveFile',
          '_configurations',
          '_more'
        ])
        .exhaustive()
    )
  }

  // Define groupable actions
  const startActions: (keyof typeof buttonConfigs)[] = [
    '_start',
    '_start_paused',
    '_pause',
    '_standby',
    '_activate'
  ]
  const stopActions: (keyof typeof buttonConfigs)[] = ['_stop', '_kill']

  // Derived lists of button names to display in each dropdown
  const availableStartButtons = $derived.by(() => {
    const rawActions = getRawActions(pipeline.current.status)
    const buttons = rawActions.filter((action) =>
      startActions.find((a) => a === action)
    ) as (keyof typeof buttonConfigs)[]

    return buttons
  })

  const availableStopButtons = $derived.by(() => {
    const rawActions = getRawActions(pipeline.current.status)
    const buttons = rawActions.filter((action) =>
      stopActions.find((a) => a === action)
    ) as (keyof typeof buttonConfigs)[]

    return buttons
  })

  const active = $derived.by(() => {
    const rawActions = getRawActions(pipeline.current.status)
    // Group actions into dropdowns
    return processActionsForDropdowns(rawActions)
  })

  function processActionsForDropdowns(
    rawActions: (keyof typeof actions)[]
  ): (keyof typeof actions)[] {
    let processedActions = [...rawActions]

    if (availableStartButtons.length > 1) {
      const firstStartIndex = processedActions.findIndex((action) =>
        startActions.find((a) => a === action)
      )
      // Remove all start actions and replace the first one with _multiStart
      processedActions = processedActions.filter(
        (action) => !startActions.find((a) => a === action)
      )
      processedActions.splice(firstStartIndex, 0, '_multiStart')
    }

    if (availableStopButtons.length > 1) {
      const firstStopIndex = processedActions.findIndex((action) =>
        stopActions.find((a) => a === action)
      )
      // Remove all stop actions and replace the first one with _multiStop
      processedActions = processedActions.filter((action) => !stopActions.find((a) => a === action))
      processedActions.splice(firstStopIndex, 0, '_multiStop')
    }

    return processedActions
  }

  const isMobile = useIsMobile()

  const buttonClass = 'btn'
  const iconClass = 'text-[16px]'
  const shortClass = 'btn-icon btn-icon-sm'
  // Every button in the action bar is the 24px size. Labelled ones fit their content; only
  // the spacer that stands in for a missing action keeps a fixed width.
  const longClass = 'btn-sm'
  const longSpacerClass = 'w-[104px] sm:w-[136px]'
  const shortColor = 'preset-tonal-surface'
  // The secondary actions (pause, stop, force stop, cancel start) are outlined; the start
  // actions keep the filled primary style.
  const basicBtnColor = 'preset-outlined-surface-200-800'
  const importantBtnColor = 'preset-filled-primary-500'

  const { postPipelineAction } = getPipelineAction()
  const pipelineActionCallbacks = usePipelineActionCallbacks()

  const performStartAction = async (
    action: PipelineAction,
    pipelineName: string
    // nextStatus: PipelineStatus
  ) => {
    const callbacks =
      action === 'start'
        ? {
            onPausedReady: async (pipelineName: string) => {
              const cbs = pipelineActionCallbacks.getAll(pipelineName, 'start_paused')
              await Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
            }
          }
        : undefined

    const { waitFor } = await postPipelineAction(pipeline.current.name, action, callbacks)
    waitFor().then(
      (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, action),
      toastError(`Waiting for pipeline to ${action}`)
    )
  }

  // Static multi-action dropdown configurations
  const multiActionConfigs = {
    start: {
      ariaLabel: 'See start options',
      buttonClass: 'preset-filled-primary-500',
      variant: 'filled',
      size: 'sm'
    },
    stop: {
      ariaLabel: 'See stop options',
      buttonClass: '',
      variant: 'outlined',
      size: 'sm'
    }
  } satisfies Record<
    string,
    { ariaLabel: string; buttonClass: string; variant: SplitButtonVariant; size: SplitButtonSize }
  >

  // Static button configurations for each action
  const buttonConfigs: Record<
    string,
    {
      label: string
      description: string
      onclick: () => void
      disabled: () => boolean
      disabledText?: string
      standaloneButton: Snippet
    }
  > = {
    _start: {
      label: 'Start',
      description: 'Start the pipeline normally',
      onclick: () => {
        const pipelineName = pipeline.current.name
        performStartAction('start', pipelineName)
      },
      disabled: () => unsavedChanges,
      disabledText: 'Save First',
      standaloneButton: _start
    },
    _start_paused: {
      label: 'Start as Paused',
      description: 'Start the pipeline in a paused state',
      onclick: () => {
        const pipelineName = pipeline.current.name
        performStartAction('start_paused', pipelineName)
      },
      disabled: () => unsavedChanges,
      disabledText: 'Save First',
      standaloneButton: _start_paused
    },
    _resume: {
      label: 'Resume',
      description: 'Resume the paused pipeline',
      onclick: () => {
        const pipelineName = pipeline.current.name
        performStartAction('resume', pipelineName)
      },
      disabled: () => unsavedChanges,
      disabledText: 'Save First',
      standaloneButton: _start
    },
    _stop: {
      label: 'Stop',
      description: 'Stop the pipeline after taking a checkpoint',
      onclick: () => (globalDialog.dialog = stopDialog),
      disabled: () => !isEnterprise.value,
      disabledText: 'Enterprise Only',
      standaloneButton: _stop
    },
    _kill: {
      label: 'Force Stop',
      description: 'Stop the pipeline immediately',
      onclick: () => (globalDialog.dialog = killDialog),
      disabled: () => false,
      standaloneButton: _kill
    },
    _pause: {
      label: 'Pause',
      description: 'Pause the running pipeline',
      onclick: async () => {
        const pipelineName = pipeline.current.name
        const { waitFor } = await postPipelineAction(pipelineName, 'pause')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'pause'),
          toastError('Waiting for pipeline to pause')
        )
      },
      disabled: () => false,
      standaloneButton: _pause
    },
    _standby: {
      label: 'Start in Standby',
      description: 'Put the pipeline in standby mode',
      onclick: async () => {
        const pipelineName = pipeline.current.name
        const { waitFor } = await postPipelineAction(pipelineName, 'standby')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'standby'),
          toastError('Waiting for pipeline to standby')
        )
      },
      disabled: () => unsavedChanges,
      disabledText: 'Save First',
      standaloneButton: _standby
    },
    _activate: {
      label: 'Activate',
      description: 'Activate the pipeline to start data ingress and processing',
      onclick: async () => {
        const pipelineName = pipeline.current.name
        const { waitFor } = await postPipelineAction(pipelineName, 'activate')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'activate'),
          toastError('Waiting for pipeline to activate')
        )
      },
      disabled: () => false,
      standaloneButton: _activate
    }
  }
</script>

{#snippet clearDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Clear',
      (name) => `Clear ${name} pipeline storage?`,
      async (pipelineName: string) => {
        const { waitFor } = await postPipelineAction(pipelineName, 'clear')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'clear'),
          toastError('Waiting for pipeline to clear state')
        )
      },
      'This will delete all checkpoints.'
    )(pipeline.current.name)}
  ></DeleteDialog>
{/snippet}

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      (name) => `Delete ${name} pipeline?`,
      (name: string) => {
        deletePipeline(name)
      }
    )(pipeline.current.name)}
  ></DeleteDialog>
{/snippet}

{#snippet killDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Force stop',
      (name) => `Force stop ${name} pipeline?`,
      async (pipelineName: string) => {
        const { waitFor } = await postPipelineAction(pipelineName, 'kill')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'kill'),
          toastError('Waiting for pipeline to force stop')
        )
      },
      'The pipeline will stop processing inputs without making a checkpoint, leaving only a previous one, if any.'
    )(pipeline.current.name)}
  ></DeleteDialog>
{/snippet}

{#snippet stopDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Stop',
      (name) => `Stop ${name} pipeline?`,
      async (pipelineName: string) => {
        const { waitFor } = await postPipelineAction(pipelineName, 'stop')
        waitFor().then(
          (shouldContinue) => shouldContinue && onActionSuccess?.(pipelineName, 'stop'),
          toastError('Waiting for pipeline to stop')
        )
      },
      'The pipeline will stop processing inputs and create a checkpoint of its state.'
    )(pipeline.current.name)}
  ></DeleteDialog>
{/snippet}

{#if !deleted}
  <div data-testid="box-action-buttons" class={'flex flex-nowrap items-center gap-2 ' + _class}>
    {#each active as name}
      {@render actions[name]()}
    {/each}
  </div>
{/if}

{#snippet _multiAction(configKey: keyof typeof multiActionConfigs)}
  {@const config = multiActionConfigs[configKey]}
  {@const buttonNames = configKey === 'start' ? availableStartButtons : availableStopButtons}
  {@const primeButtonName = buttonNames[0]}
  {@const primeButtonConfig = buttonConfigs[primeButtonName]}

  <Popup wrapperClass="flex">
    {#snippet trigger(toggle)}
      <SplitButton
        ontoggle={toggle}
        toggleLabel={config.ariaLabel}
        toggleClass={config.buttonClass}
        variant={config.variant}
        size={config.size}
      >
        {@render primeButtonConfig?.standaloneButton()}
      </SplitButton>
    {/snippet}
    {#snippet content(close)}
      {@const buttons = buttonNames.map((buttonName) => {
        const buttonConfig = buttonConfigs[buttonName]
        return {
          ...buttonConfig,
          disabled: buttonConfig.disabled(),
          onclick: () => {
            close()
            buttonConfig.onclick()
          }
        }
      })}
      <div
        transition:slide={{ duration: 100 }}
        class="bg-white-dark absolute top-full right-0 z-30 mt-2 scrollbar flex max-h-[400px] w-[calc(100vw-36px)] max-w-[300px] flex-col justify-stretch rounded shadow-md sm:max-w-[380px]"
      >
        {#each buttons as button}
          <button
            class="flex flex-col gap-1 px-4 py-3 text-left hover:bg-surface-50-950 {button.disabled
              ? 'pointer-events-none opacity-80'
              : ''}"
            onclick={button.onclick}
          >
            <div class="flex justify-between text-lg">
              <span class="">{button.label}</span>

              {#if button.disabled}
                <span class="text-sm">{button.disabledText}</span>
              {/if}
            </div>
            <div class="text-base text-surface-700-300">
              {button.description}
            </div>
          </button>
        {/each}
      </div>
    {/snippet}
  </Popup>
{/snippet}

{#snippet _multiStart()}
  {@render _multiAction('start')}
{/snippet}

{#snippet _multiStop()}
  {@render _multiAction('stop')}
{/snippet}

<!-- {#snippet pipelineChangesDialog()}
  <GenericDialog
    confirmLabel="Apply"
    onApply={() => {
      pipelineChangesReview.onApply()
      globalDialog.dialog = null
    }}
    onClose={() => {
      globalDialog.onclose?.()
      globalDialog.dialog = null
    }}
  >
    {#snippet title()}
      Review pipeline changes
    {/snippet}
    <ReviewPipelineChanges
      changes={pipelineChangesReview.diff}
      onskip={() => {
        pipelineChangesReview.onApply()
        globalDialog.dialog = null
      }}
    ></ReviewPipelineChanges>
  </GenericDialog>
{/snippet} -->

{#snippet _more()}
  <Popup wrapperClass="flex">
    {#snippet trigger(toggle)}
      <button
        class="{buttonClass} {shortClass} {basicBtnColor} fd fd-more_horiz {iconClass}"
        onclick={toggle}
        aria-label="Pipeline options"
      >
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:slide={{ duration: 100 }}
        class="bg-white-dark absolute top-full right-0 z-30 mt-2 flex w-44 flex-col justify-stretch rounded shadow-md"
      >
        <button
          class="flex items-center gap-2 px-4 py-3 text-left hover:bg-surface-50-950 disabled:opacity-50"
          disabled={!pipelineList.pipelines || unsavedChanges}
          onclick={() => {
            close()
            void duplicateCurrentPipeline()
          }}
        >
          <span class="fd fd-copy-plus text-[16px]"></span>
          Duplicate
        </button>
        <Tooltip placement="top">
          {#if unsavedChanges}
            Save the program before duplicating.
          {:else}
            {duplicatePipelineTooltip}
          {/if}
        </Tooltip>
        <button
          class="flex items-center gap-2 px-4 py-3 text-left hover:bg-surface-50-950 disabled:opacity-50"
          disabled={!!deleteDisabledReason}
          onclick={() => {
            close()
            globalDialog.dialog = deleteDialog
          }}
        >
          <span class="fd fd-trash-2 text-[16px]"></span>
          Delete
        </button>
        {#if deleteDisabledReason}
          <Tooltip class="whitespace-nowrap" placement="top">{deleteDisabledReason}</Tooltip>
        {/if}
      </div>
    {/snippet}
  </Popup>
{/snippet}
{#snippet start({
  text,
  action,
  disabled
}: {
  text: string
  action?: PipelineAction
  disabled?: boolean
})}
  <div>
    <button
      aria-label={text}
      class:disabled
      class={isMobile.current
        ? `${buttonClass} ${shortClass} ${importantBtnColor} ${iconClass}`
        : `${buttonClass} ${longClass} ${importantBtnColor}`}
      onclick={async () => {
        if (!action) {
          return
        }
        const pipelineName = pipeline.current.name
        performStartAction(action, pipelineName)
      }}
    >
      <span class="fd fd-play {iconClass}"></span>
      <span class="hidden sm:inline">
        {text}
      </span>
    </button>
  </div>
{/snippet}
{#snippet _start()}
  {@render start({
    text: 'Start',
    action: 'start',
    disabled: unsavedChanges
  })}
  {#if unsavedChanges}
    <Tooltip placement="top">Save the program before running</Tooltip>
  {/if}
{/snippet}
{#snippet _start_paused()}
  {@render start({
    text: 'Start',
    action: 'start_paused',
    disabled: unsavedChanges
  })}
  {#if unsavedChanges}
    <Tooltip placement="top">Save the program before running</Tooltip>
  {/if}
{/snippet}
{#snippet _resume()}
  {@render start({
    text: 'Resume',
    action: 'resume',
    disabled: unsavedChanges
  })}
  {#if unsavedChanges}
    <Tooltip placement="top">Save the program before running</Tooltip>
  {/if}
{/snippet}
{#snippet _standby()}
  {@render start({
    text: 'Standby',
    action: 'standby',
    disabled: unsavedChanges
  })}
  <Tooltip placement="top">Put the pipeline in standby mode</Tooltip>
{/snippet}
{#snippet _activate()}
  {@render start({
    text: 'Activate',
    action: 'activate',
    disabled: unsavedChanges
  })}
  <Tooltip placement="top">Activate the pipeline to start data ingress and processing</Tooltip>
{/snippet}
{#snippet _start_disabled()}
  {@render start({ text: 'Start', disabled: true })}
{/snippet}
{#snippet _start_error()}
  {@render _start_disabled()}
  <Tooltip placement="top">Resolve errors before running</Tooltip>
{/snippet}
{#snippet _start_pending()}
  {@render _start_disabled()}
  <Tooltip placement="top">Wait for compilation to complete</Tooltip>
{/snippet}
{#snippet _pause()}
  <button
    class="hidden sm:flex {buttonClass} {longClass} {basicBtnColor}"
    onclick={buttonConfigs._pause.onclick}
  >
    <span class="fd fd-pause {iconClass}"></span>
    Pause
  </button>
  <button
    class="flex sm:hidden {buttonClass} {shortClass} {basicBtnColor} {iconClass}"
    onclick={buttonConfigs._pause.onclick}
  >
    <span class="fd fd-pause {iconClass}"></span>
  </button>
{/snippet}
{#snippet _stop()}
  <div>
    <button
      disabled={!isEnterprise.value}
      class="hidden sm:flex {buttonClass} {longClass} {basicBtnColor}"
      onclick={() => {
        globalDialog.dialog = stopDialog
      }}
    >
      <span class="fd fd-square {iconClass}"></span>
      Stop
    </button>
    <button
      class="sm:hidden {buttonClass} {shortClass} {basicBtnColor} fd fd-square {iconClass}"
      onclick={() => (globalDialog.dialog = stopDialog)}
    >
    </button>
  </div>
  {#if !isEnterprise.value}
    <Popover class="w-max max-w-[90vw]" placement="bottom">
      Stopping pipelines gracefully is only available in the Enterprise edition.<br />
      <a
        class="block pt-2 underline"
        href={calendlyUrl}
        target="_blank"
        rel="noreferrer"
        onclick={() =>
          captureEvent('calendly_opened', { url: calendlyUrl, placement: calendlyPlacement })}
        >Upgrade</a
      >
    </Popover>
  {/if}
{/snippet}
{#snippet _kill_short()}
  <div>
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-square-power {iconClass}"
      onclick={() => (globalDialog.dialog = killDialog)}
    >
    </button>
  </div>
  <Tooltip placement="top">Force Stop</Tooltip>
{/snippet}
{#snippet _kill()}
  <button
    class="hidden sm:flex {buttonClass} {longClass} {basicBtnColor}"
    onclick={() => (globalDialog.dialog = killDialog)}
  >
    <span class="fd fd-square-power {iconClass}"></span>
    Force Stop
  </button>
  <button
    class="sm:hidden {buttonClass} {shortClass} {basicBtnColor} fd fd-square-power {iconClass}"
    onclick={() => (globalDialog.dialog = killDialog)}
  >
  </button>
{/snippet}
{#snippet _saveFile()}
  <div class="-mr-2 block sm:hidden"></div>
  <div class="hidden sm:flex">
    <button
      class="{buttonClass} {shortClass} {basicBtnColor} fd fd-save {iconClass}"
      class:disabled={!unsavedChanges}
      onclick={saveFile}
    >
    </button>
  </div>
  <Tooltip placement="top">
    {#if unsavedChanges}
      Save file: Ctrl + S
    {:else}
      File saved
    {/if}
  </Tooltip>
{/snippet}
{#snippet _unschedule()}
  <!-- TODO: add support for a short size when in mobile -->
  <button
    class="{buttonClass} {longClass} {basicBtnColor}"
    onclick={() => {
      globalDialog.dialog = killDialog
    }}
  >
    Cancel start
  </button>
  <Tooltip class="whitespace-nowrap" placement="top">
    The pipeline is scheduled to start automatically after compilation
  </Tooltip>
{/snippet}

{#snippet _configurations()}
  <PipelineConfigurationsPopup {pipeline} pipelineBusy={editConfigDisabled}
  ></PipelineConfigurationsPopup>
  {#if !canWriteConfig.allowed}
    <Tooltip placement="top">You have read-only access to the configuration</Tooltip>
  {:else if editConfigDisabled}
    <Tooltip placement="top">Stop the pipeline to edit settings</Tooltip>
  {:else}
    <Tooltip placement="top">Compilation and runtime configuration</Tooltip>
  {/if}
{/snippet}
{#snippet _spacer_short()}
  <div class={shortClass}></div>
{/snippet}
{#snippet _spacer_long()}
  <div class={longSpacerClass}></div>
{/snippet}
{#snippet _spinner()}
  <div class="flex sm:hidden">
    {@render _spinner_short()}
  </div>
  <div class="hidden sm:flex">
    {@render _status_spinner()}
  </div>
{/snippet}
{#snippet _spinner_short()}
  <div class="pointer-events-none {buttonClass} {shortClass} {basicBtnColor}">
    <IconLoader class="h-4 flex-none animate-spin fill-surface-950-50"></IconLoader>
  </div>
{/snippet}
{#snippet _status_spinner()}
  <button class="{buttonClass} {longClass} pointer-events-none {basicBtnColor}">
    <IconLoader class="h-4 flex-none animate-spin fill-surface-950-50"></IconLoader>
    <span>{getDeploymentStatusLabel(pipeline.current.status)}</span>
  </button>
{/snippet}
{#snippet _storage_indicator()}
  <!-- The storage status, combined with a clear icon button when the user may clear the storage.
       Clearing is only possible while the pipeline is stopped; while it runs the button is
       disabled and its tooltip says why. -->
  {@const storageStatus = pipeline.current.storageStatus}
  {@const isShutdown = isPipelineShutdown(pipeline.current.status)}
  {#if storageStatus === 'Clearing'}
    <div
      class="flex h-6 items-center gap-1 rounded-(--radius-control-sm) px-2 text-[12px] leading-4 text-nowrap {basicBtnColor}"
    >
      <IconLoader class="h-4 w-4 flex-none animate-spin fill-current"></IconLoader>
      Clearing storage…
    </div>
    <Tooltip placement="top">Clearing pipeline storage, including any checkpoints.</Tooltip>
  {:else if storageStatus === 'InUse' && canExec.allowed}
    <div
      class="flex h-6 items-center gap-1 rounded-(--radius-control-sm) pl-2 text-[12px] leading-4 text-nowrap text-surface-700-300 {basicBtnColor}"
    >
      <span class="fd fd-database text-[16px]"></span>
      Storage in use
      <div class="ml-1 flex h-full border-l border-surface-200-800">
        <button
          aria-label="Clear storage"
          class="{buttonClass} {shortClass} fd fd-eraser h-full! rounded-l-none! rounded-r-[2px]! {iconClass} hover:filter-none! hover:not-disabled:bg-surface-100-900"
          disabled={!isShutdown}
          onclick={() => (globalDialog.dialog = clearDialog)}
        ></button>
      </div>
    </div>
    <Tooltip placement="top">
      {#if isShutdown}
        Pipeline storage is in use. Click to clear it.
      {:else}
        The storage is used by the running pipeline. Stop the pipeline to clear it.
      {/if}
    </Tooltip>
  {:else}
    <div
      class="flex h-6 items-center gap-1 px-2 text-[12px] leading-4 text-nowrap text-surface-700-300"
    >
      <span class="fd {storageStatus === 'InUse' ? 'fd-database' : 'fd-database-off'} text-[16px]"
      ></span>
      {storageStatus === 'InUse' ? 'Storage in use' : 'Storage cleared'}
    </div>
    <Tooltip placement="top">
      {#if storageStatus === 'Cleared'}
        There are no checkpoints available.
      {:else}
        Pipeline storage is in use.
      {/if}
    </Tooltip>
  {/if}
{/snippet}
{#snippet _clear_storage()}
  {@const storageStatus = pipeline.current.storageStatus}
  {@const isShutdown = isPipelineShutdown(pipeline.current.status)}
  {#if storageStatus === 'Clearing'}
    <div class="pointer-events-none {buttonClass} {shortClass} {shortColor}">
      <IconLoader class="h-4 flex-none animate-spin fill-surface-950-50"></IconLoader>
    </div>
    <Tooltip placement="top">
      The pipeline storage is being deleted, and provisioned resources deallocated.
    </Tooltip>
  {:else}
    <div>
      <button
        onclick={() => (globalDialog.dialog = clearDialog)}
        class="{buttonClass} {shortClass} {shortColor} fd {storageStatus === 'InUse'
          ? 'fd-server'
          : 'fd-server-off'} preset-tonal-surface {iconClass} {isShutdown &&
        storageStatus === 'InUse'
          ? ''
          : 'disabled'}"
      >
      </button>
    </div>
    <Tooltip placement="top">
      {#if storageStatus === 'Cleared'}
        Pipeline is not using any storage.
      {:else if isShutdown}
        Pipeline storage is in use. Click to clear it.
      {:else}
        Pipeline is using storage. Stop the pipeline to clear it.
      {/if}
    </Tooltip>
  {/if}
{/snippet}
