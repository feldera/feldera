<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { Action } from 'svelte/action'

  let {
    value = $bindable(),
    children,
    onvalue,
    class: _class = ''
  }: {
    value: string
    children?: Snippet
    onvalue?: (value: string) => void
    class?: string
  } = $props()
  let showInput = $state(false)

  const autofocus: Action = (e) => {
    e.focus()
  }

  const handleSubmit = (e: { currentTarget: EventTarget & HTMLInputElement }) => {
    if (value !== e.currentTarget.value) {
      value = e.currentTarget.value
      onvalue?.(e.currentTarget.value)
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
    }}
    class={_class}
  />
{:else}
  <span role="button" tabindex={0} ondblclick={() => (showInput = true)}>
    {@render children?.()}
  </span>
{/if}
