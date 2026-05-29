<script lang="ts">
  import type { ZipItem } from 'but-unzip'
  import { Select } from 'common-ui'

  let {
    profileFiles,
    selectedTimestamp,
    onSelectTimestamp,
    class: className = ''
  }: {
    profileFiles: [Date, ZipItem[]][]
    selectedTimestamp: Date | null
    onSelectTimestamp: (timestamp: Date) => void
    class?: string
  } = $props()

  const timestamps = $derived(profileFiles.map((p) => p[0]))
</script>

<label class="flex items-center gap-2 text-sm {className}">
  <span class="text-surface-600-400">Snapshot:</span>
  <Select
    class="w-36"
    value={selectedTimestamp?.getTime()}
    onchange={(e) => {
      onSelectTimestamp(new Date(parseInt(e.currentTarget.value)))
    }}
  >
    {#each timestamps as timestamp (timestamp)}
      <option value={timestamp.getTime()}>{timestamp.toLocaleTimeString()}</option>
    {/each}
  </Select>
</label>
