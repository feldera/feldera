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
        () => 'fd fd-check -mr-1 -mt-1 h-6 text-[32px] text-success-500'
      )
      .with('Pending', 'CompilingSql', undefined, () => 'gc gc-loader-alt animate-spin text-[24px]')
      .with(P.shape({}), () => 'fd fd-close_circle_outline inline-block text-[24px] text-error-500')
      .exhaustive()}
  >
  </span>
  SQL
</div>

<div
  class="{match(programStatus)
    .with('CompilingRust', { RustError: P.any }, () => 'flex')
    .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => 'hidden')
    .exhaustive()} flex-nowrap gap-2 self-center whitespace-nowrap"
>
  <span
    class={match(programStatus)
      .with('CompilingRust', () => 'gc gc-loader-alt animate-spin pt-[0.5px] text-[24px]')
      .with({ RustError: P.any }, () => 'fd fd-close_circle_outline text-[24px] text-error-500')
      .with('Success', 'Pending', 'CompilingSql', P.shape({}), undefined, () => '')
      .exhaustive()}
  >
  </span>
  Rust <span class="hidden sm:inline">compiler</span>
</div>
