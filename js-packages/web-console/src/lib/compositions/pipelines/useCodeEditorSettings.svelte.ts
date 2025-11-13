import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export const useCodeEditorSettings = () => {
  const editorFontSize = useLocalStorage('layout/pipelines/editor/fontSize', 14)
  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
  const showMinimap = useLocalStorage('layout/pipelines/editor/minimap', true)
  const showStickyScroll = useLocalStorage('layout/pipelines/editor/stickyScroll', true)
  return {
    editorFontSize,
    autoSavePipeline,
    showMinimap,
    showStickyScroll
  }
}
