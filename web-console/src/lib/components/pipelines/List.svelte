<script lang="ts" context="module">
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import { base } from '$app/paths'
  import { postPipeline, type PipelineThumb } from '$lib/services/pipelineManager'
  import { goto } from '$app/navigation'
  import { page } from '$app/stores'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'

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
</script>

<div class="relative flex flex-col gap-2 overflow-y-auto pb-2" use:bindScrollY={{ scrollY }}>
  <div class="sticky top-0 px-4 py-0.5 bg-surface-50-950">
    {#if assistCreatingPipeline}
      <input
        bind:this={createPipelineInputRef}
        onblur={(e) => {
          e.currentTarget.value = ''
          stopAssisting()
        }}
        onkeydown={async (e) => {
          if (e.key === 'Enter') {
            await createPipeline(e.currentTarget.value)
            e.currentTarget.blur()
          }
        }}
        placeholder="New Pipeline Name"
        class="input placeholder-surface-800 outline-none transition-none duration-0 bg-surface-50-950 dark:placeholder-surface-200"
      />
      <div class="-mb-2 pt-2 text-surface-600-400">Press Enter to create</div>
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
      href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}
    >
      <div class="w-full overflow-ellipsis whitespace-break-spaces py-1">
        <!-- Insert a thin whitespace to help break names containing underscore -->
        {pipeline.name.replaceAll('_', `_ `)}
      </div>
      <PipelineStatus class="ml-auto" {...pipeline}></PipelineStatus>
    </a>
  {/each}
</div>
