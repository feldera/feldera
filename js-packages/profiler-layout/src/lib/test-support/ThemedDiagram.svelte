<!--
  A diagram whose palette is state, so a suite can switch the theme the way the application does: a prop
  change on a mounted diagram, which restyles it in place.

  `rerender` replaces every prop, the profile included, and the diagram rebuilds itself from scratch -
  palette and all, which passes a repaint test without repainting anything.
-->
<script lang="ts">
  import type { Dataflow, DiagramTheme, JsonProfiles, ProfilerCallbacks } from 'profiler-lib'
  import ProfilerDiagram from '../components/ProfilerDiagram.svelte'

  interface Props {
    profileData: JsonProfiles
    dataflowData: Dataflow | undefined
    programCode: string[] | undefined
    callbacks: ProfilerCallbacks
    theme: DiagramTheme
  }
  const props: Props = $props()

  /** The palette a suite switched to, standing in for the one it was mounted with. */
  let switched = $state<DiagramTheme | undefined>()
  const theme = $derived(switched ?? props.theme)
  let diagram = $state<{ search(query: string): void } | undefined>()

  export const setTheme = (next: DiagramTheme) => {
    switched = next
  }
  export const search = (query: string) => diagram?.search(query)
</script>

<ProfilerDiagram
  bind:this={diagram}
  profileData={props.profileData}
  dataflowData={props.dataflowData}
  programCode={props.programCode}
  callbacks={props.callbacks}
  {theme}
/>
