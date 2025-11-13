import { editor } from 'monaco-editor/esm/vs/editor/editor.api'

export class GenericOverlayWidget implements editor.IOverlayWidget {
  protected editorRef: editor.IStandaloneCodeEditor
  protected domNode: HTMLElement | undefined
  protected options: {
    id: string
    position?: editor.IOverlayWidgetPosition['preference']
  }
  dispose: () => void

  constructor(
    editorRef: editor.IStandaloneCodeEditor,
    node: HTMLElement,
    options: typeof this.options
  ) {
    this.editorRef = editorRef
    this.domNode = node
    this.options = options
    this.editorRef.addOverlayWidget(this)
    this.domNode?.classList.remove('hidden')
    this.dispose = () => {
      try {
      } catch {}
      this.editorRef.removeOverlayWidget(this)
    }
  }

  getId() {
    return this.options.id
  }
  getDomNode() {
    if (this.domNode) {
      return this.domNode
    }
    this.domNode = document.createElement('div')
    return this.domNode
  }
  getPosition() {
    if (!this.options.position) {
      return null
    }
    return {
      preference: this.options.position
    }
  }
}
