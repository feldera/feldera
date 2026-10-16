import * as monaco from 'monaco-editor'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import SourceCodeView from './SourceCodeView.svelte'

const modelOf = (name: string) =>
  monaco.editor.getModel(monaco.Uri.parse(`inmemory://profiler/${name}`))

describe('SourceCodeView', () => {
  it('shows a file in the language of the file', () => {
    const view = render(SourceCodeView, {
      file: { name: 'udf.rs', language: 'rust', code: 'pub fn f() {}' }
    })
    expect(modelOf('udf.rs')?.getLanguageId()).toBe('rust')
    expect(modelOf('udf.rs')?.getValue()).toBe('pub fn f() {}')
    view.unmount()
    expect(modelOf('udf.rs')).toBeNull()
  })

  it('shows the new code when the file changes', async () => {
    const file = { name: 'udf.rs', language: 'rust', code: 'pub fn f() {}' } as const
    const view = render(SourceCodeView, { file })
    await view.rerender({ file: { ...file, code: 'pub fn g() {}' } })
    expect(modelOf('udf.rs')?.getValue()).toBe('pub fn g() {}')
    view.unmount()
  })

  it('gives each file its own model, so two files can be mounted at the same time', () => {
    // A tab switch can mount the next file before it unmounts the previous file.
    const sql = render(SourceCodeView, {
      file: { name: 'program.sql', language: 'sql', code: 'SELECT 1;' }
    })
    const udf = render(SourceCodeView, {
      file: { name: 'udf.rs', language: 'rust', code: 'pub fn f() {}' }
    })
    expect(modelOf('program.sql')?.getValue()).toBe('SELECT 1;')
    expect(modelOf('udf.rs')?.getValue()).toBe('pub fn f() {}')
    sql.unmount()
    udf.unmount()
  })
})
