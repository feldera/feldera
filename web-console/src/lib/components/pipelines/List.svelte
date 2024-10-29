<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import { base } from '$app/paths'
  import { postPipeline, type PipelineThumb } from '$lib/services/pipelineManager'
  import { goto } from '$app/navigation'
  import { page } from '$app/stores'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'

  let { pipelines = $bindable() }: { pipelines: PipelineThumb[] } = $props()

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
  let showDrawer = useDrawer()
  let assistCreatingPipeline = $derived($page.url.hash === '#new')
  const stopAssisting = () => {
    goto('')
  }
  $effect(() => {
    if (assistCreatingPipeline) {
      showDrawer.value = true
      createPipelineInputRef.focus()
    }
  })

  let createPipelineInputRef: HTMLElement

  const createPipeline = async (pipelineName: string) => {
    const newPipeline = await postPipeline({
      name: pipelineName,
      runtime_config: {},
      program_config: {},
      description: '',
      program_code: ''
    })
    pipelines = [...pipelines, newPipeline]
    goto(`${base}/pipelines/${encodeURIComponent(pipelineName)}/`)
  }

  const bindScrollY = (node: HTMLElement, val: { scrollY: number }) => {
    $effect(() => {
      node.scrollTop = scrollY
    })
    const handle = (e: any) => {
      scrollY = e.target.scrollTop
    }
    node.addEventListener('scroll', handle)
    return {
      destroy: () => removeEventListener('scroll', handle)
    }
  }

  let newPipelineError = $state<string>()
</script>

<div
  class="relative flex h-full flex-col gap-2 overflow-y-auto scrollbar"
  use:bindScrollY={{ scrollY }}
>
  <div class="sticky top-0 px-4 py-0.5 bg-surface-50-950">
    {#if assistCreatingPipeline}
      <input
        bind:this={createPipelineInputRef}
        onblur={(e) => {
          e.currentTarget.value = ''
          newPipelineError = undefined
          stopAssisting()
        }}
        onkeydown={async (e) => {
          if (e.code === 'Enter') {
            await createPipeline(e.currentTarget.value).then(
              () => {
                e.currentTarget?.blur()
              },
              (e) => {
                if ('message' in e) {
                  newPipelineError = e.message
                }
              }
            )
          }
        }}
        placeholder="New Pipeline Name"
        class="input placeholder-surface-800 outline-none transition-none duration-0 bg-surface-50-950 dark:placeholder-surface-200"
      />
      {#if newPipelineError}
        <div class="-mb-2 pt-2 text-error-500">{newPipelineError}</div>
      {:else}
        <div class="-mb-2 pt-2 text-surface-600-400">Press Enter to create</div>
      {/if}
    {:else}
      <div class="flex justify-center">
        <button
          class="btn mb-7 mt-auto self-end text-sm preset-filled-primary-500"
          onclick={() => goto('#new')}
        >
          CREATE NEW PIPELINE
        </button>
      </div>
    {/if}
  </div>
  {#each pipelines as pipeline}
    <a
      class="-my-0.5 flex flex-nowrap items-center gap-2 border-2 border-transparent px-3.5 {$page
        .params.pipelineName === pipeline.name
        ? 'bg-white-black'
        : 'border-transparent hover:!bg-opacity-30 hover:bg-surface-100-900'}"
      onclick={() => {
        if (showDrawer.isMobileDrawer) {
          showDrawer.value = false
        }
      }}
      href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}
    >
      <div class="w-full overflow-ellipsis whitespace-break-spaces py-1">
        <!-- Insert a thin whitespace to help break names containing underscore -->
        {pipeline.name.replaceAll('_', `_ `)}
      </div>
      <PipelineStatus class="ml-auto" {...pipeline}></PipelineStatus>
    </a>
  {/each}
  <span class="sticky bottom-0 mt-auto py-1 pl-4 bg-surface-50-950 text-surface-700-300"
    >{$page.data.felderaVersion}</span
  >
</div>
