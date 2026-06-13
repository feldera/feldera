import * as monaco from 'monaco-editor'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  FELDERA_DARK_THEME,
  FELDERA_LIGHT_THEME,
  HIGHLIGHT_DARK,
  HIGHLIGHT_LIGHT,
  felderaMonacoThemes,
  registerFelderaMonacoThemes
} from './sqlCodeTheme.js'

// Selection colours must keep the source range clearly visible on top of the editor
// background (issue #6404). The first block pins the theme data; the second mounts a real
// Monaco editor in the browser to confirm the registered theme actually paints the selection
// in the expected colour, not just the data we hand to `defineTheme`.

describe('feldera monaco theme data', () => {
  const themes = felderaMonacoThemes()

  it("uses the pipeline editor's exact colours and theme names — single source of truth", () => {
    // These values are identical to those in
    // js-packages/web-console/src/lib/components/pipelines/editor/CodeEditor.svelte. If you
    // change one location, change the other.
    expect(HIGHLIGHT_LIGHT).toBe('#add6ff90')
    expect(HIGHLIGHT_DARK).toBe('#264f7890')
    expect(FELDERA_LIGHT_THEME).toBe('feldera-light')
    expect(FELDERA_DARK_THEME).toBe('feldera-dark')
  })

  it('overrides `editor.inactiveSelectionBackground` for both bases', () => {
    expect(themes[FELDERA_LIGHT_THEME]?.base).toBe('vs')
    expect(themes[FELDERA_DARK_THEME]?.base).toBe('vs-dark')
    expect(themes[FELDERA_LIGHT_THEME]?.colors?.['editor.inactiveSelectionBackground']).toBe(
      HIGHLIGHT_LIGHT
    )
    expect(themes[FELDERA_DARK_THEME]?.colors?.['editor.inactiveSelectionBackground']).toBe(
      HIGHLIGHT_DARK
    )
  })

  it('keeps the SQL string token tint shared with the pipeline editor', () => {
    expect(themes[FELDERA_LIGHT_THEME]?.rules).toContainEqual({
      token: 'string.sql',
      foreground: '#7a3d00'
    })
    expect(themes[FELDERA_DARK_THEME]?.rules).toContainEqual({
      token: 'string.sql',
      foreground: '#d9731a'
    })
  })
})

describe('feldera-light selection rendering (live monaco)', () => {
  let container: HTMLDivElement
  let editor: monaco.editor.IStandaloneCodeEditor

  beforeEach(() => {
    registerFelderaMonacoThemes()
    container = document.createElement('div')
    container.style.cssText = 'width: 800px; height: 200px; position: absolute; top: 0; left: 0;'
    document.body.appendChild(container)
    editor = monaco.editor.create(container, {
      value: 'select 1 from t',
      language: 'sql',
      theme: FELDERA_LIGHT_THEME,
      automaticLayout: false,
      minimap: { enabled: false },
      readOnly: true
    })
    editor.layout({ width: 800, height: 200 })
  })

  afterEach(() => {
    editor.dispose()
    container.remove()
  })

  /** Convert `#rrggbbaa` to the `rgb(r, g, b)` / `rgba(r, g, b, a)` form `getComputedStyle`
   *  returns. Browsers normalise inline colours to that representation, so a string compare
   *  against the registered hex needs this bridge. */
  function expectedRgba(hex: string): string {
    const r = parseInt(hex.slice(1, 3), 16)
    const g = parseInt(hex.slice(3, 5), 16)
    const b = parseInt(hex.slice(5, 7), 16)
    const a = parseInt(hex.slice(7, 9), 16) / 255
    return `rgba(${r}, ${g}, ${b}, ${Number(a.toFixed(2))})`
  }

  it('paints the inactive selection in HIGHLIGHT_LIGHT', async () => {
    editor.setSelection(new monaco.Selection(1, 1, 1, 7))
    ;(document.activeElement as HTMLElement | null)?.blur()
    // Monaco draws selection overlays on the next animation frame; the theme colour is
    // injected via a stylesheet rule, not an inline style, so we read `getComputedStyle`.
    await new Promise((r) => requestAnimationFrame(() => r(null)))

    const overlay = container.querySelector<HTMLElement>('.view-overlays .selected-text')
    expect(overlay, 'selected-text overlay should be rendered').not.toBeNull()
    const bg = getComputedStyle(overlay!).backgroundColor
    expect(bg).toBe(expectedRgba(HIGHLIGHT_LIGHT))
  })
})
