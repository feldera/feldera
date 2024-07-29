<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/Status.svelte'
  import { base } from '$app/paths'
  import { postPipeline, type PipelineThumb } from '$lib/services/pipelineManager'
  import { goto, replaceState } from '$app/navigation'
  import { page } from '$app/stores'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'

  let { pipelines }: { pipelines: PipelineThumb[] } = $props()

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
  let showDrawer = useLocalStorage('layout/drawer', false)
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
</script>

<div class="flex flex-col gap-4 p-4">
  {#each pipelines as pipeline}
    <div class="flex flex-nowrap items-center gap-2 break-all">
      <a
        class=" transition-none duration-0"
        href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}
      >
        {pipeline.name}
      </a>
      <PipelineStatus class="ml-auto" {...pipeline}></PipelineStatus>
    </div>
  {/each}
  <div class="relative">
    <input
      bind:this={createPipelineInputRef}
      onblur={(e) => {
        e.currentTarget.value = ''
        stopAssisting()
      }}
      onkeydown={async (e) => {
        if (e.key === 'Enter') {
          const name = e.currentTarget.value
          await postPipeline({
            name,
            runtime_config: {},
            program_config: {},
            description: '',
            program_code: ''
          })
          goto(`${base}/pipelines/${name}`)
          e.currentTarget.blur()
        }
      }}
      placeholder="+ create pipeline"
      class="input placeholder-surface-700 outline-none bg-surface-50-950 dark:placeholder-surface-300"
    />
    {#if assistCreatingPipeline}
      <div
        class="absolute top-8 text-nowrap rounded bg-white px-3 py-2 text-sm font-medium shadow-md text-surface-950-50 dark:bg-black"
      >
        Enter pipeline name and press Enter
      </div>
    {/if}
  </div>
</div>
