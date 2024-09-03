<script lang="ts">
  import type { ProgramStatus } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'

  let {
    programStatus
  }: {
    programStatus: ProgramStatus | undefined
  } = $props()
</script>

<div class="flex w-16 flex-nowrap justify-end gap-2 self-center">
  <span
    class={match(programStatus)
      .with(
        'Success',
        'CompilingRust',
        { RustError: P.any },
        () => 'bx bx-check -mr-1 -mt-1 h-6 text-[32px] text-success-500'
      )
      .with('Pending', 'CompilingSql', undefined, () => 'bx bx-loader-alt animate-spin text-[24px]')
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
