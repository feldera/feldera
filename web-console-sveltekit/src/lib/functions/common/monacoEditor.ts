export const isMonacoEditorDisabled = (disabled?: boolean) => ({
  domReadOnly: disabled,
  readOnly: disabled
})
