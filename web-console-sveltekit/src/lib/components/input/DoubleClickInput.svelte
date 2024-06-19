<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { Action } from 'svelte/action'

  let { value = $bindable(), children } = $props<{ value: string; children?: Snippet }>()
  let showInput = $state(false)

  const autofocus: Action = (e) => {
    e.focus()
  }

  const handleSubmit = (e: { currentTarget: EventTarget & HTMLInputElement }) => {
    if (value !== e.currentTarget.value) {
      value = e.currentTarget.value
    }
    showInput = false
  }
</script>

{#if showInput}
  <input
    use:autofocus
    {value}
    onblur={(e) => {
      handleSubmit(e)
    }}
    onkeydown={(e) => {
      if (e.key !== 'Enter') {
        return
      }
      handleSubmit(e)
    }} />
{:else}
  <span role="button" tabindex={0} ondblclick={() => (showInput = true)}>
    {@render children()}
  </span>
{/if}
