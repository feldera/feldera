<script lang="ts">
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { goto } from '$app/navigation'
  import { resolve } from '$lib/functions/svelte'
  import type { Snippet } from '$lib/types/svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  let {
    onShowInput,
    createButton,
    afterInput,
    inputClass,
    onHideInput,
    onSuccess
  }: {
    onShowInput?: () => void
    createButton: Snippet<[onclick: () => void]>
    afterInput?: Snippet<[error?: string]>
    inputClass?: string
    onHideInput?: () => void
    onSuccess?: () => void
  } = $props()

  let showInput = $state(false)

  $effect(() => {
    if (showInput) {
      onShowInput?.()
      createPipelineInputRef.focus()
    }
  })

  let createPipelineInputRef: HTMLElement

  function clickOutsideFocus(element: HTMLElement, callbackFunction: () => void) {
    function onClick(event: MouseEvent) {
      if (!element.contains(event.target as Node)) {
        callbackFunction()
      }
    }

    function listenClick() {
      document.body.addEventListener('click', onClick as any)
    }
    function ignoreClick() {
      setTimeout(() => {
        document.body.removeEventListener('click', onClick as any)
      }, 100)
    }

    element.addEventListener('focus', listenClick)
    element.addEventListener('blur', ignoreClick)

    return {
      update(newCallbackFunction: () => void) {
        callbackFunction = newCallbackFunction
      },
      destroy() {
        document.body.removeEventListener('click', onClick as any)
        element.removeEventListener('focus', listenClick)
        element.removeEventListener('blur', ignoreClick)
      }
    }
  }
  const { updatePipelines } = useUpdatePipelineList()
  const api = usePipelineManager()

  const createPipeline = async (pipelineName: string) => {
    const newPipeline = await api.postPipeline({
      name: pipelineName,
      runtime_config: {},
      program_config: {},
      description: '',
      program_code: ''
    })
    updatePipelines((pipelines) => [...pipelines, newPipeline])

    goto(resolve(`/pipelines/${encodeURIComponent(pipelineName)}/`))
  }

  let newPipelineError = $state<string>()
</script>

{#if showInput}
  <input
    bind:this={createPipelineInputRef}
    onblur={(e) => {
      e.currentTarget.value = ''
      newPipelineError = undefined
      showInput = false
      onHideInput?.()
    }}
    onkeydown={async (e) => {
      if (e.key === 'Enter') {
        await createPipeline(e.currentTarget.value).then(
          () => {
            e.currentTarget?.blur()
            onSuccess?.()
          },
          (e) => {
            if ('message' in e) {
              newPipelineError = e.message
            }
          }
        )
      }
    }}
    enterkeyhint="done"
    placeholder="New Pipeline Name"
    class="placeholder-surface-800 dark:placeholder-surface-200 {inputClass}"
  />
  {@render afterInput?.(newPipelineError)}
{:else}
  {@render createButton(() => (showInput = true))}
{/if}
