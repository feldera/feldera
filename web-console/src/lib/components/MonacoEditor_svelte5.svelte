<script lang="ts">
  import type { editor } from 'monaco-editor/esm/vs/editor/editor.api'
  import { onMount } from 'svelte'
  const { updateContent, content } = $props<{
    updateContent: (newContent: string) => void
    content: string
  }>()
  let element = $state<HTMLDivElement>()
  let instance: editor.ICodeEditor | null = $state(null)
  onMount(() => {
    self.MonacoEnvironment = {
      getWorker: async () =>
        new Worker(
          new URL('monaco-editor/esm/vs/editor/browser/services/webWorker.js', import.meta.url)
        )
    }
    ;(async () => {
      const monaco = await import('monaco-editor')
      monaco.editor.onDidCreateEditor((i) => {
        instance = i
        setTimeout(() => {
          i.setValue(content)
        }, 0)
      })
      if (!element) return
      const editor = monaco.editor.create(element, {
        language: 'markdown'
      })
      editor.onDidChangeModelContent(() => {
        if (content !== editor.getValue()) {
          updateContent(editor.getValue())
        }
      })
    })()
  })
</script>

{#if instance === null}
  <p>Loading...</p>
{/if}

<div bind:this={element}></div>

<style>
  div {
    display: flex;
    width: 100%;
    height: 100%;
  }
</style>
