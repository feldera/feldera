<script lang="ts">
  import type { ZipItem } from 'but-unzip'

  let {
    profileFiles,
    selectedTimestamp = $bindable(),
    class: className = ''
  }: {
    profileFiles: [Date, ZipItem[]][]
    selectedTimestamp: Date | null
    class?: string
  } = $props()

  const timestamps = $derived(profileFiles.map((p) => p[0]))
</script>

<label class="flex items-center gap-2 text-sm {className}">
  <span class="text-surface-600-400">Snapshot:</span>
  <select
    class="select w-36"
    value={selectedTimestamp?.getTime()}
    onchange={(e) => {
      selectedTimestamp = new Date(parseInt(e.currentTarget.value))
    }}
  >
    {#each timestamps as timestamp (timestamp)}
      <option value={timestamp.getTime()}>{timestamp.toLocaleTimeString()}</option>
    {/each}
  </select>
</label>
