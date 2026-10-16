<script lang="ts" module>
  import type { SourcePositionRange } from 'profiler-lib'
  import type { SourceFile } from '../../functions/processZipBundle'

  export type SourceTabProps = {
    file: SourceFile
    highlightRanges: SourcePositionRange[]
  }
</script>

<script lang="ts">
  import SourceCodeView from '../SourceCodeView.svelte'
  let { file, highlightRanges }: SourceTabProps = $props()
</script>

<div class="relative min-h-0 flex-1 bg-white-dark">
  <div class="absolute inset-0 overflow-auto">
    <SourceCodeView {file} {highlightRanges} />
  </div>
</div>
