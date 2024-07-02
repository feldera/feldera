
import Monaco, { exportedThemes, nativeThemes, themeNames } from '$lib/components/MonacoEditor.svelte';

export const isMonacoEditorDisabled = (disabled?: boolean) => ({
  domReadOnly: disabled,
  readOnly: disabled
})

export default Monaco;
export { exportedThemes, nativeThemes, themeNames };