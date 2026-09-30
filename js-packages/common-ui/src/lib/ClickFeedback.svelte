<script lang="ts">
  import type { Snippet } from 'svelte'

  let {
    active: externalActive = false,
    timeoutMs = 2_000,
    clickFeedback = $bindable<() => void>(),
    children
  }: {
    active?: boolean
    timeoutMs?: number
    clickFeedback?: () => void
    children: Snippet<[{ active: boolean; clickFeedback: () => void }]>
  } = $props()

  let pending = $state(false)
  const active = $derived(pending || externalActive)

  clickFeedback = () => {
    pending = true
    setTimeout(() => (pending = false), timeoutMs)
  }
</script>

{@render children({ active, clickFeedback })}
