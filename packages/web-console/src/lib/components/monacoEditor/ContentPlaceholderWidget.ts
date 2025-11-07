import { editor, type IDisposable } from 'monaco-editor'

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
    let disposables: IDisposable[] = []
    disposables.push(
      this.editorRef.onDidChangeModelContent(() => this.refreshPlaceholderVisibility())
    )
    disposables.push(this.editorRef.onDidChangeModel(() => this.refreshPlaceholderVisibility()))
    disposables.push(this.editorRef.onDidFocusEditorText(() => this.refreshPlaceholderVisibility()))
    disposables.push(this.editorRef.onDidBlurEditorText(() => this.refreshPlaceholderVisibility()))
    // ensure that on initial load the placeholder is shown
    this.refreshPlaceholderVisibility()
    // ensure widget and event handler are properly disposed of
    this.dispose = () => {
      for (const { dispose } of disposables) {
        dispose()
      }
      this.editorRef.removeContentWidget(this)
    }
  }

  refreshPlaceholderVisibility() {
    if (this.editorRef.hasTextFocus() && !this.editorRef.getOption(editor.EditorOption.readOnly)) {
      this.editorRef.removeContentWidget(this)
      return
    }
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
      for (const [prop, val] of Object.entries(
        (this.placeholderStyle as CSSStyleDeclaration) ?? {}
      )) {
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
