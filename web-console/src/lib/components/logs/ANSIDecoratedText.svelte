<!-- Render text decorated by ANSI escape sequences -->

<script lang="ts">
  import prettyAnsi from 'pretty-ansi'
  import '$lib/components/logs/styles.css'

  let { value }: { value: string } = $props()
</script>

{@html prettyAnsi(value)}
