import type { editor } from 'monaco-editor/esm/vs/editor/editor.api.js'
import { MonacoPlaceholderContentWidget } from '$lib/components/monacoEditor/ContentPlaceholderWidget'

export const effectMonacoContentPlaceholder = (
  editorRef: editor.IStandaloneCodeEditor,
  placeholder?: string,
  placeholderStyle?: Partial<CSSStyleDeclaration>
) => {
  placeholder
  if (!editorRef || !placeholder) {
    return
  }
  const placeholderWidget = new MonacoPlaceholderContentWidget(
    placeholder,
    editorRef,
    placeholderStyle
  )
  return () => placeholderWidget.dispose()
}
