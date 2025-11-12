<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'
  import type { Action } from 'svelte/action'

  let {
    value = $bindable(),
    children,
    onvalue,
    class: _class = '',
    inputClass,
    disabled
  }: {
    value: string
    children?: Snippet
    onvalue?: (value: string) => void | Promise<any>
    class?: string
    inputClass?: string
    disabled?: boolean
  } = $props()
  let showInput = $state(false)

  const autofocus: Action = (e) => {
    e.focus()
  }

  const handleSubmit = (e: { currentTarget: EventTarget & HTMLInputElement }) => {
    if (_value !== e.currentTarget.value) {
      onvalue?.(e.currentTarget.value)?.then(() => {
        if (e.currentTarget) {
          value = _value = e.currentTarget.value
        }
      })
    }
    showInput = false
  }
  // Local _value can be updated instantly and checked within sequential onblur and onkeydown handlers,
  // unlike $bindable value
  let _value = value
  $effect(() => {
    _value = value
  })
</script>

{#if showInput}
  <input
    use:autofocus
    {value}
    onblur={(e) => {
      e.currentTarget.value = ''
      showInput = false
    }}
    onkeydown={(e) => {
      if (e.key === 'Enter') {
        handleSubmit(e)
      }
    }}
    enterkeyhint="done"
    class={inputClass}
  />
{:else}
  <span
    class="group {_class} {disabled ? 'cursor-default' : ''}"
    role="button"
    tabindex={0}
    ondblclick={() => {
      if (disabled) {
        return
      }
      showInput = true
    }}
  >
    {@render children?.()}
    <button
      {disabled}
      onclick={() => {
        showInput = true
      }}
      class="fd fd-pencil-line text-[20px] text-surface-400-600 {disabled
        ? ''
        : 'group-hover:text-surface-950-50'}"
      aria-label="Edit pipeline name"
    >
    </button>
  </span>
{/if}
