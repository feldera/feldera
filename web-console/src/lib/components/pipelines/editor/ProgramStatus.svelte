<script lang="ts">
  import type { ProgramStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'

  let {
    programStatus
  }: {
    programStatus: ProgramStatus | undefined
  } = $props()

  const spinnerClass = 'animate-spin text-[24px] fill-surface-950-50'
  let sqlClass = $derived(
    match(programStatus)
      .with(
        'Success',
        'SqlCompiled',
        'CompilingRust',
        { RustError: P.any },
        () => 'fd fd-circle-check-big text-[24px] text-success-500'
      )
      .with('Pending', 'CompilingSql', undefined, () => 'spinner')
      .with(P.shape({}), () => 'fd fd-circle-x inline-block text-[20px] text-error-500')
      .exhaustive()
  )
  let rustClass = $derived(
    match(programStatus)
      .with('SqlCompiled', 'CompilingRust', () => 'spinner')
      .with({ RustError: P.any }, () => 'fd fd-circle-x text-[20px] text-error-500')
      .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => '')
      .exhaustive()
  )
</script>

<div class="flex flex-nowrap justify-end gap-2 self-center">
  <span class={sqlClass}>
    <IconLoader class={sqlClass === 'spinner' ? spinnerClass : 'hidden'}></IconLoader>
  </span>
  SQL
</div>

<div
  class="{match(programStatus)
    .with('SqlCompiled', 'CompilingRust', { RustError: P.any }, () => 'flex')
    .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => 'hidden')
    .exhaustive()} flex-nowrap gap-2 self-center whitespace-nowrap"
>
  <span class={rustClass}>
    <IconLoader class={rustClass === 'spinner' ? spinnerClass : 'hidden'}></IconLoader>
  </span>
  Rust <span class="hidden sm:inline">compiler</span>
</div>
