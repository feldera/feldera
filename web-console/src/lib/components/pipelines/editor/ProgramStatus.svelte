<script lang="ts">
  import type { ProgramStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'

  let {
    programStatus
  }: {
    programStatus: ProgramStatus | undefined
  } = $props()

  const spinnerClass = 'animate-spin h-5 fill-surface-950-50'
  let sqlClass = $derived(
    match(programStatus)
      .with(
        'Success',
        'SqlCompiled',
        'CompilingRust',
        'RustError',
        'SystemError',
        () => 'fd fd-circle-check-big text-[20px] text-success-500'
      )
      .with('Pending', 'CompilingSql', undefined, () => spinnerClass)
      .with('SqlError', () => 'fd fd-circle-x inline-block text-[20px] text-error-500')
      .exhaustive()
  )
  let rustClass = $derived(
    match(programStatus)
      .with('CompilingRust', () => spinnerClass)
      .with('RustError', () => 'fd fd-circle-x text-[20px] text-error-500')
      .with(
        'Success',
        'Pending',
        'CompilingSql',
        'SqlCompiled',
        'SqlError',
        'SystemError',
        undefined,
        () => 'hidden'
      )
      .exhaustive()
  )
</script>

<div class="flex flex-nowrap justify-end gap-2 self-center">
  {#if sqlClass !== spinnerClass}
    <span class={sqlClass}> </span>
  {:else}
    <IconLoader class={sqlClass}></IconLoader>
  {/if}
  SQL
</div>

<div
  class="{rustClass === 'hidden'
    ? 'hidden'
    : 'flex'} flex-nowrap gap-2 self-center whitespace-nowrap"
>
  {#if rustClass !== spinnerClass}
    <span class={rustClass}> </span>
  {:else}
    <IconLoader class={rustClass}></IconLoader>
  {/if}
  <span>
    Rust <span class="hidden sm:inline">compiler</span>
  </span>
</div>
