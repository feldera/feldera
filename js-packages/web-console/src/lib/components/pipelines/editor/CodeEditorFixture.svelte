<!--
  Renders the real CodeEditor in a test. This wrapper fills the required snippet
  props and gives the editor one writable file.
-->
<script lang="ts">
  import CodeEditor from '#lib/components/pipelines/editor/CodeEditor.svelte'

  let { code }: { code: string } = $props()

  const files = [
    {
      name: 'program.sql',
      language: 'sql' as const,
      access: {
        get current() {
          return code
        },
        set current(value: string) {
          code = value
        }
      }
    }
  ]
  let currentFileName = $state('program.sql')
</script>

<div class="h-96 w-200">
  <CodeEditor path="fixture-pipeline" {files} bind:currentFileName autoSave={false}>
    {#snippet codeEditor(textEditor)}
      {@render textEditor()}
    {/snippet}
    {#snippet fileTab(text)}
      <span>{text}</span>
    {/snippet}
  </CodeEditor>
</div>
