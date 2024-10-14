<script lang="ts" module>
  let modules = $state<
    Record<
      string,
      {
        define: Define
      }[]
    >
  >({})
  const runtime = $state(new Runtime())
</script>

<script lang="ts">
  import { Runtime, Inspector, type Module, type Define } from '@observablehq/runtime'
  import ModuleComponent from './Module.svelte'

  let { define, pipelineName }: { define: Define; pipelineName: string } = $props()
  let nextId = $state(0)

  $effect.pre(() => {
    modules[pipelineName] ??= []
  })

  const addModule = (pipelineName: string, define: Define) => {
    modules[pipelineName].push({
      define: (runtime, observer) => define(runtime, observer) as Define
    })
  }

  $effect(() => {
    if (!modules[pipelineName].length) {
      addModule(pipelineName, define)
    }
  })

  const handleUpdate = (moduleIdx: number) => {
    if (modules[pipelineName].length === moduleIdx + 1) {
      addModule(pipelineName, define)
    }
  }
</script>

<div class="flex min-h-full flex-1 flex-col gap-0.5 bg-white pb-4 dark:bg-black">
  {#each modules[pipelineName] as { define }, i (pipelineName + i)}
    {@const isLast = i === modules[pipelineName].length - 1}
    <ModuleComponent
      {runtime}
      {define}
      onModuleUpdated={() => handleUpdate(i)}
      onDelete={() => modules[pipelineName].splice(i, 1)}
      {isLast}
    ></ModuleComponent>
  {/each}
</div>
