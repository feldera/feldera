import { editor } from 'monaco-editor/esm/vs/editor/editor.api'

/**
 * Represents an placeholder renderer for monaco editor
 * Roughly based on https://github.com/microsoft/vscode/blob/main/src/vs/workbench/contrib/codeEditor/browser/untitledTextEditorHint/untitledTextEditorHint.ts
 */
export class MonacoPlaceholderContentWidget {
  private static ID = 'editor.widget.placeholderHint'
  private placeholder: string
  private editorRef: editor.IStandaloneCodeEditor
  private domNode: HTMLElement | undefined
  private placeholderStyle: Partial<CSSStyleDeclaration> | undefined
  dispose: () => void

  constructor(
    placeholder: typeof this.placeholder,
    editorRef: typeof this.editorRef,
    placeholderStyle?: typeof this.placeholderStyle
  ) {
    this.placeholder = placeholder
    this.editorRef = editorRef
    this.placeholderStyle = placeholderStyle
    // register a listener for editor code changes
    let disposeHandler = this.editorRef.onDidChangeModelContent(() =>
      this.onDidChangeModelContent()
    )
    // ensure that on initial load the placeholder is shown
    this.onDidChangeModelContent()
    // ensure widget and event handler are properly disposed of
    this.dispose = () => {
      disposeHandler.dispose()
      this.editorRef.removeContentWidget(this)
    }
  }

  onDidChangeModelContent() {
    if (this.editorRef.getValue() === '') {
      this.editorRef.addContentWidget(this)
    } else {
      this.editorRef.removeContentWidget(this)
    }
  }

  getId() {
    return MonacoPlaceholderContentWidget.ID
  }

  getDomNode() {
    if (!this.domNode) {
      this.domNode = document.createElement('span')
      this.domNode.textContent = this.placeholder
      this.domNode.style.width = 'max-content'
      this.domNode.style.whiteSpace = 'pre-wrap'
      this.domNode.style.pointerEvents = 'none'
      for (const [prop, val] of Object.entries(this.placeholderStyle ?? {})) {
        const [value, pri = ''] = val.split('!')
        this.domNode.style.setProperty(prop, value, pri)
      }
      this.editorRef.applyFontInfo(this.domNode)
    }

    return this.domNode
  }

  getPosition() {
    return {
      position: { lineNumber: 1, column: 1 },
      preference: [editor.ContentWidgetPositionPreference.EXACT]
    }
  }
}
