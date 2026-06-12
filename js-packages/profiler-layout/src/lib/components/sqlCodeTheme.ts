// Monaco theme registration shared with the pipeline editor (`CodeEditor.svelte` registers the
// same `feldera-light` / `feldera-dark` names with the same values). Defining them again here is
// safe: Monaco's `defineTheme` is idempotent — the last registration wins, and as long as the
// values match the two locations stay in sync. Kept in a leaf module so the theme data can be
// unit-tested without rendering the Svelte component.

import * as monaco from 'monaco-editor'

/** Light-mode selection colour. The editor in the profiler is read-only and the user is steering
 *  the diagram, so the selection is always rendered with the "inactive" colour — Monaco's stock
 *  inactive shade (vs: `#E5EBF1`) is nearly invisible. `#add6ff90` is the saturated blue used by
 *  the pipeline editor for the same reason. */
export const HIGHLIGHT_LIGHT = '#add6ff90'
/** Dark-mode counterpart of HIGHLIGHT_LIGHT, matching the pipeline editor's `feldera-dark`. */
export const HIGHLIGHT_DARK = '#264f7890'

/** Theme names — identical to the ones the pipeline editor registers. */
export const FELDERA_LIGHT_THEME = 'feldera-light'
export const FELDERA_DARK_THEME = 'feldera-dark'

/** Returns the theme data passed to `monaco.editor.defineTheme`. Pure (no monaco call), so tests
 *  can inspect it without going through Monaco's theme registry. The token rule for `string.sql`
 *  matches the pipeline editor's tint so the two views render SQL strings identically. */
export function felderaMonacoThemes(): Record<string, monaco.editor.IStandaloneThemeData> {
  return {
    [FELDERA_LIGHT_THEME]: {
      base: 'vs',
      inherit: true,
      rules: [{ token: 'string.sql', foreground: '#7a3d00' }],
      colors: {
        'editor.inactiveSelectionBackground': HIGHLIGHT_LIGHT
      }
    },
    [FELDERA_DARK_THEME]: {
      base: 'vs-dark',
      inherit: true,
      rules: [{ token: 'string.sql', foreground: '#d9731a' }],
      colors: {
        'editor.inactiveSelectionBackground': HIGHLIGHT_DARK
      }
    }
  }
}

/** Register the themes with Monaco. Idempotent — Monaco overwrites existing themes by name, so
 *  HMR re-runs, repeated mounts, and co-registration with the pipeline editor are all safe. */
export function registerFelderaMonacoThemes(): void {
  const themes = felderaMonacoThemes()
  for (const [name, data] of Object.entries(themes)) {
    monaco.editor.defineTheme(name, data)
  }
}
