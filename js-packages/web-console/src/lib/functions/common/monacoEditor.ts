import { exportedThemes, nativeThemes, themeNames } from '$lib/components/MonacoEditorRunes.svelte'

export const isMonacoEditorDisabled = (disabled?: boolean) => ({
  // domReadOnly: disabled,
  readOnly: disabled
})

export { exportedThemes, nativeThemes, themeNames }
