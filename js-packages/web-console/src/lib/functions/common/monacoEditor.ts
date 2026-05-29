import { exportedThemes, nativeThemes, themeNames } from 'common-ui'

export const isMonacoEditorDisabled = (disabled?: boolean) => ({
  // domReadOnly: disabled,
  readOnly: disabled
})

export { exportedThemes, nativeThemes, themeNames }
