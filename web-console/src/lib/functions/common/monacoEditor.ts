import Monaco, {
  exportedThemes,
  nativeThemes,
  themeNames
} from '$lib/components/MonacoEditorRunes.svelte'

export const isMonacoEditorDisabled = (disabled?: boolean) => ({
  domReadOnly: disabled,
  readOnly: disabled
})

export default Monaco
export { exportedThemes, nativeThemes, themeNames }
