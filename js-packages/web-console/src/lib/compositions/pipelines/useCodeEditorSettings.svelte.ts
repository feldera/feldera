import { useLocalStorage } from '$lib/compositions/useLocalStorage.svelte'

export const useCodeEditorSettings = () => {
  const editorFontSize = useLocalStorage('layout/pipelines/editor/fontSize', 14)
  const autoSaveFiles = useLocalStorage('layout/pipelines/autosave', true)
  const showMinimap = useLocalStorage('layout/pipelines/editor/minimap', true)
  const showStickyScroll = useLocalStorage('layout/pipelines/editor/stickyScroll', true)
  return {
    editorFontSize,
    autoSaveFiles,
    showMinimap,
    showStickyScroll
  }
}
