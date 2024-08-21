<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import type { ProgramStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  let {
    downstreamChanged,
    saveCode,
    programStatus
  }: {
    downstreamChanged: boolean
    saveCode: () => void
    programStatus: ProgramStatus | undefined
  } = $props()

  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
</script>

<div class="flex h-full flex-nowrap gap-2">
  <!-- <div class="w-20 p-0.5 text-center">
    {downstreamChanged ? 'changed' : 'saved'}
  </div> -->
  <div class="flex h-full flex-nowrap">
    <button
      class={'px-4 hover:preset-filled-primary-200-800 ' +
        (downstreamChanged ? 'preset-tonal-primary' : 'bg-surface-50-950')}
      class:disabled={!downstreamChanged}
      onclick={saveCode}
    >
      Save
    </button>
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black">Ctrl + S</Tooltip>
    <button
      class="w-32 px-2 hover:preset-filled-primary-200-800"
      tabindex={10}
      onclick={() => (autoSavePipeline.value = !autoSavePipeline.value)}
    >
      Autosave: {autoSavePipeline.value ? 'on' : 'off'}
    </button>
  </div>
  <div class="flex w-16 flex-nowrap justify-end gap-2 self-center">
    <span
      class={match(programStatus)
        .with(
          'Success',
          'CompilingRust',
          { RustError: P.any },
          () => 'bx bx-check -mr-1 -mt-1 h-6 text-[32px] text-success-500'
        )
        .with(
          'Pending',
          'CompilingSql',
          undefined,
          () => 'bx bx-loader-alt animate-spin text-[24px]'
        )
        .with(P.shape({}), () => 'bx bx-x-circle text-[24px] text-error-500')
        .exhaustive()}
    >
    </span>
    SQL
  </div>

  <div
    class="{match(programStatus)
      .with('CompilingRust', { RustError: P.any }, () => 'flex')
      .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => 'hidden')
      .exhaustive()} w-36 flex-nowrap justify-end gap-2 self-center whitespace-nowrap"
  >
    <span
      class={match(programStatus)
        .with('CompilingRust', () => 'bx bx-loader-alt animate-spin text-[24px]')
        .with({ RustError: P.any }, () => 'bx bx-x-circle text-[24px] text-error-500')
        .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => '')
        .exhaustive()}
    >
    </span>
    Rust compiler
  </div>
</div>
